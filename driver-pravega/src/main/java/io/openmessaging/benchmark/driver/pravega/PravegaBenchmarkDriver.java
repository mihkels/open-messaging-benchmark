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
package io.openmessaging.benchmark.driver.pravega;

import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.yaml.YAMLFactory;

public class PravegaBenchmarkDriver implements BenchmarkDriver {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkDriver.class);

    private static final ObjectWriter objectWriter =
            new ObjectMapper().writerWithDefaultPrettyPrinter();

    private PravegaConfig config;
    private String scopeName;
    private StreamManager streamManager;
    private ReaderGroupManager readerGroupManager;
    private EventStreamClientFactory clientFactory;
    private final List<String> createdTopics = new ArrayList<>();
    private int activeTopicCreations;
    private boolean closing;

    @Override
    public void initialize(final Path configurationFile, final MeterRegistry statsLogger)
            throws IOException, InterruptedException {
        config = readConfig(configurationFile);
        log.info("Pravega driver configuration: {}", objectWriter.writeValueAsString(config));

        ClientConfig clientConfig =
                ClientConfig.builder().controllerURI(URI.create(config.client().controllerURI())).build();
        scopeName = config.client().scopeName();
        try {
            streamManager = StreamManager.create(clientConfig);
            readerGroupManager = ReaderGroupManager.withScope(scopeName, clientConfig);
            clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);
        } catch (Exception failure) {
            closeResource(clientFactory, failure);
            closeResource(readerGroupManager, failure);
            closeResource(streamManager, failure);
            clientFactory = null;
            readerGroupManager = null;
            streamManager = null;
            if (failure instanceof InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            if (failure instanceof IOException ioFailure) {
                throw ioFailure;
            }
            if (failure instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            throw new IOException("Failed to initialize the Pravega driver", failure);
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private static final ObjectReader reader =
            mapper
                    .readerFor(PravegaConfig.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static PravegaConfig readConfig(Path configurationFile) throws IOException {
        try (InputStream input = Files.newInputStream(configurationFile)) {
            return reader.readValue(input);
        }
    }

    private String cleanName(String name) {
        return name.replaceAll("[^A-Za-z0-9-]", "");
    }

    @Override
    public String getTopicNamePrefix() {
        return "openmessaging-benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        synchronized (this) {
            if (closing || streamManager == null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Driver is closed"));
            }
            activeTopicCreations++;
        }
        try {
            topic = cleanName(topic);
            log.info("createTopic: topic={}, partitions={}", topic, partitions);
            if (config.createScope()) {
                streamManager.createScope(scopeName);
            }
            ScalingPolicy scalingPolicy;
            if (config.enableStreamAutoScaling()
                    && (config.eventsPerSecond() != PravegaConfig.DEFAULT_STREAM_AUTOSCALING_VALUE
                            || config.kbytesPerSecond() != PravegaConfig.DEFAULT_STREAM_AUTOSCALING_VALUE)) {
                scalingPolicy =
                        config.eventsPerSecond() != PravegaConfig.DEFAULT_STREAM_AUTOSCALING_VALUE
                                ? ScalingPolicy.byEventRate(config.eventsPerSecond(), 2, partitions)
                                : ScalingPolicy.byDataRate(config.kbytesPerSecond(), 2, partitions);
            } else {
                scalingPolicy = ScalingPolicy.fixed(partitions);
            }
            streamManager.createStream(
                    scopeName, topic, StreamConfiguration.builder().scalingPolicy(scalingPolicy).build());
            synchronized (this) {
                createdTopics.add(topic);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Exception failure) {
            return failedCreation(failure);
        } finally {
            synchronized (this) {
                activeTopicCreations--;
                notifyAll();
            }
        }
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        try {
            topic = cleanName(topic);
            BenchmarkProducer producer;
            if (config.enableTransaction()) {
                producer =
                        new PravegaBenchmarkTransactionProducer(
                                topic,
                                clientFactory,
                                config.includeTimestampInEvent(),
                                config.writer().enableConnectionPooling(),
                                config.eventsPerTransaction());
            } else {
                producer =
                        new PravegaBenchmarkProducer(
                                topic,
                                clientFactory,
                                config.includeTimestampInEvent(),
                                config.writer().enableConnectionPooling());
            }
            return CompletableFuture.completedFuture(producer);
        } catch (Exception failure) {
            return failedCreation(failure);
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        try {
            topic = cleanName(topic);
            subscriptionName = cleanName(subscriptionName);
            BenchmarkConsumer consumer =
                    PravegaBenchmarkConsumer.create(
                            topic,
                            scopeName,
                            subscriptionName,
                            consumerCallback,
                            clientFactory,
                            readerGroupManager,
                            config.includeTimestampInEvent());
            return CompletableFuture.completedFuture(consumer);
        } catch (Exception failure) {
            return failedCreation(failure);
        }
    }

    @Override
    public void close() throws Exception {
        log.info("close: Pravega driver is closing");
        EventStreamClientFactory factoryToClose;
        ReaderGroupManager readerManagerToClose;
        StreamManager streamManagerToClose;
        List<String> topics;
        InterruptedException interrupted = null;
        synchronized (this) {
            closing = true;
            while (activeTopicCreations > 0) {
                try {
                    wait();
                } catch (InterruptedException failure) {
                    if (interrupted == null) {
                        interrupted = failure;
                    }
                }
            }
            factoryToClose = clientFactory;
            readerManagerToClose = readerGroupManager;
            streamManagerToClose = streamManager;
            clientFactory = null;
            readerGroupManager = null;
            streamManager = null;
            topics = List.copyOf(createdTopics);
            createdTopics.clear();
        }
        Exception failure = closeResource(factoryToClose, interrupted);
        failure = closeResource(readerManagerToClose, failure);
        if (streamManagerToClose != null) {
            for (String topic : topics) {
                try {
                    log.info("deleteTopics: topic={}", topic);
                    streamManagerToClose.sealStream(scopeName, topic);
                    if (config.deleteStreams()) {
                        streamManagerToClose.deleteStream(scopeName, topic);
                    }
                } catch (Exception topicFailure) {
                    if (topicFailure instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    failure = addFailure(failure, topicFailure);
                }
            }
        }
        failure = closeResource(streamManagerToClose, failure);
        if (interrupted != null) {
            Thread.currentThread().interrupt();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static <T> CompletableFuture<T> failedCreation(Exception failure) {
        if (failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return CompletableFuture.failedFuture(failure);
    }

    private static Exception closeResource(AutoCloseable resource, Exception failure) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception closeFailure) {
                if (closeFailure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                return addFailure(failure, closeFailure);
            }
        }
        return failure;
    }

    private static Exception addFailure(Exception failure, Exception additional) {
        if (failure == null) {
            return additional;
        }
        if (failure != additional) {
            failure.addSuppressed(additional);
        }
        return failure;
    }
}
