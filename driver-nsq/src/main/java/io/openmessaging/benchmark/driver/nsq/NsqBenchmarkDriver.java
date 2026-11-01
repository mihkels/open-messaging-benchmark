/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.nsq;

import com.github.brainlag.nsq.NSQProducer;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.yaml.YAMLFactory;

public class NsqBenchmarkDriver implements BenchmarkDriver {
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final Set<CompletableFuture<BenchmarkConsumer>> consumerCreations = new HashSet<>();
    private NsqConfig config;
    private EventLoopGroup consumerEventLoops;
    private boolean closed;

    @Override
    public void initialize(Path configurationFile, MeterRegistry statsLogger) throws IOException {
        try (var input = Files.newInputStream(configurationFile)) {
            config = reader.readValue(input);
        }
        if (config.nsqdHttpPort < 1 || config.nsqdHttpPort > 65535) {
            throw new IllegalArgumentException("nsqdHttpPort must be between 1 and 65535");
        }
        log.info("read config file," + config.toString());
    }

    @Override
    public String getTopicNamePrefix() {
        return "Nsq-Benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        // Create the topic up front so nsqd registers it with nsqlookupd before consumers look it up;
        // otherwise consumers only find it at their next periodic lookup
        log.info(
                "Creating topic {} on nsqd {}:{} (partitions are ignored)",
                topic,
                config.nsqdHost,
                config.nsqdHttpPort);
        HttpRequest request =
                HttpRequest.newBuilder(
                                URI.create(
                                        "http://"
                                                + config.nsqdHost
                                                + ":"
                                                + config.nsqdHttpPort
                                                + "/topic/create?topic="
                                                + URLEncoder.encode(topic, StandardCharsets.UTF_8)))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build();
        return httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(
                        response -> {
                            if (response.statusCode() != 200) {
                                throw new IllegalStateException(
                                        "Failed to create NSQ topic "
                                                + topic
                                                + ": HTTP "
                                                + response.statusCode()
                                                + " "
                                                + response.body());
                            }
                        });
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(final String topic) {
        NSQProducer nsqProducer = null;
        try {
            nsqProducer = newProducer();
            nsqProducer.addAddress(config.nsqdHost, 4150);
            nsqProducer.start();
            log.info("start a nsq producer");
            return CompletableFuture.completedFuture(new NsqBenchmarkProducer(nsqProducer, topic));
        } catch (Exception failure) {
            if (nsqProducer != null) {
                shutdownAfterFailure(nsqProducer::shutdown, failure);
            }
            return CompletableFuture.failedFuture(failure);
        }
    }

    NSQProducer newProducer() {
        return new NSQProducer();
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        NsqBenchmarkConsumer consumer = null;
        NsqEventLoopGroup scope = null;
        try {
            NsqLookup lookup =
                    new NsqLookup(httpClient, Duration.ofSeconds(3), config.nsqdHost, config.nsqdHttpPort);
            lookup.addLookupAddress(config.lookupHost, 4161);
            scope = new NsqEventLoopGroup(consumerEventLoops());
            consumer = newConsumer(lookup, topic, subscriptionName, consumerCallback, scope);
            CompletableFuture<BenchmarkConsumer> creation;
            boolean cancel;
            synchronized (this) {
                if (closed) {
                    return consumer.failBeforeStart(new IllegalStateException("NSQ driver is closed"));
                }
                creation = Objects.requireNonNull(consumer.startAsync(), "NSQ consumer creation future");
                cancel = closed;
                if (!cancel) {
                    consumerCreations.add(creation);
                }
            }
            if (cancel) {
                if (!creation.cancel(true) && !creation.isCompletedExceptionally()) {
                    IllegalStateException failure = new IllegalStateException("NSQ driver is closed");
                    BenchmarkConsumer handle = creation.getNow(null);
                    if (handle != null) {
                        closeAfterFailure(handle, failure);
                    }
                    if (handle != consumer) {
                        closeAfterFailure(consumer, failure);
                    }
                    shutdownAfterFailure(scope::closeChannels, failure);
                    return CompletableFuture.failedFuture(failure);
                }
            } else {
                creation.whenComplete(
                        (handle, failure) -> {
                            synchronized (this) {
                                consumerCreations.remove(creation);
                            }
                        });
            }
            return creation;
        } catch (Exception failure) {
            if (consumer != null) {
                closeAfterFailure(consumer, failure);
            }
            if (scope != null) {
                shutdownAfterFailure(scope::closeChannels, failure);
            }
            return CompletableFuture.failedFuture(failure);
        }
    }

    NsqBenchmarkConsumer newConsumer(
            NsqLookup lookup,
            String topic,
            String subscriptionName,
            ConsumerCallback consumerCallback,
            NsqEventLoopGroup scope) {
        return new NsqBenchmarkConsumer(
                lookup, topic, subscriptionName, consumerCallback, lookup::includesPublishingNode, scope);
    }

    private synchronized EventLoopGroup consumerEventLoops() {
        if (closed) {
            throw new IllegalStateException("NSQ driver is closed");
        }
        if (consumerEventLoops == null) {
            consumerEventLoops =
                    new MultiThreadIoEventLoopGroup(
                            0,
                            task -> {
                                Thread thread = new Thread(task, "nsq-consumer-io");
                                thread.setDaemon(true);
                                return thread;
                            },
                            NioIoHandler.newFactory());
        }
        return consumerEventLoops;
    }

    private static void shutdownAfterFailure(Runnable shutdown, Throwable failure) {
        try {
            shutdown.run();
        } catch (RuntimeException closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private static void closeAfterFailure(AutoCloseable resource, Throwable failure) {
        try {
            resource.close();
        } catch (Exception closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    @Override
    public void close() throws Exception {
        EventLoopGroup eventLoops;
        List<CompletableFuture<BenchmarkConsumer>> pending;
        synchronized (this) {
            closed = true;
            eventLoops = consumerEventLoops;
            consumerEventLoops = null;
            pending = new ArrayList<>(consumerCreations);
            consumerCreations.clear();
        }
        pending.forEach(creation -> creation.cancel(true));
        try {
            if (eventLoops != null) {
                eventLoops.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
            }
        } finally {
            httpClient.close();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(NsqBenchmarkDriver.class);
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private static final ObjectReader reader =
            mapper.readerFor(NsqConfig.class).without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
}
