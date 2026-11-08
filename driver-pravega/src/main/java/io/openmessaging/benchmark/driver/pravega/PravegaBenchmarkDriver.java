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
import io.openmessaging.benchmark.driver.DriverConfigs;
import io.openmessaging.benchmark.driver.Failures;
import io.openmessaging.benchmark.driver.SecretRedaction;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaBenchmarkDriver implements BenchmarkDriver {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkDriver.class);

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
        log.atInfo()
                .setMessage("Pravega driver configuration: {}")
                .addArgument(() -> SecretRedaction.redactedJson(config))
                .log();

        ClientConfig clientConfig =
                ClientConfig.builder().controllerURI(URI.create(config.client().controllerURI())).build();
        scopeName = config.client().scopeName();
        try {
            streamManager = StreamManager.create(clientConfig);
            readerGroupManager = ReaderGroupManager.withScope(scopeName, clientConfig);
            clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);
        } catch (Exception failure) {
            Failures cleanup = new Failures();
            cleanup.add(failure);
            closeResource(clientFactory, cleanup);
            closeResource(readerGroupManager, cleanup);
            closeResource(streamManager, cleanup);
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

    private static PravegaConfig readConfig(Path configurationFile) throws IOException {
        return DriverConfigs.read(configurationFile, PravegaConfig.class);
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
                        config.eventsPerSecond() == PravegaConfig.DEFAULT_STREAM_AUTOSCALING_VALUE
                                ? ScalingPolicy.byDataRate(config.kbytesPerSecond(), 2, partitions)
                                : ScalingPolicy.byEventRate(config.eventsPerSecond(), 2, partitions);
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
        InterruptedException interrupted;
        synchronized (this) {
            closing = true;
            interrupted = awaitActiveTopicCreations();
            factoryToClose = clientFactory;
            readerManagerToClose = readerGroupManager;
            streamManagerToClose = streamManager;
            clientFactory = null;
            readerGroupManager = null;
            streamManager = null;
            topics = List.copyOf(createdTopics);
            createdTopics.clear();
        }
        Failures failures = new Failures();
        if (interrupted != null) {
            failures.addInterruption(interrupted);
        }
        closeResource(factoryToClose, failures);
        closeResource(readerManagerToClose, failures);
        if (streamManagerToClose != null) {
            sealAndDeleteTopics(streamManagerToClose, topics, failures);
        }
        closeResource(streamManagerToClose, failures);
        failures.throwIfAny();
    }

    // Waits until no topic creation is in flight. Interruptions do not stop the wait, since cleanup
    // must see every created topic; the first one is returned so the caller can restore it.
    private synchronized InterruptedException awaitActiveTopicCreations() {
        InterruptedException interrupted = null;
        while (activeTopicCreations > 0) {
            try {
                wait();
            } catch (InterruptedException failure) {
                if (interrupted == null) {
                    interrupted = failure;
                }
            }
        }
        return interrupted;
    }

    private void sealAndDeleteTopics(
            StreamManager streamManagerToClose, List<String> topics, Failures failures) {
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
                failures.add(topicFailure);
            }
        }
    }

    private static <T> CompletableFuture<T> failedCreation(Exception failure) {
        if (failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return CompletableFuture.failedFuture(failure);
    }

    // Restores an interrupt at once instead of deferring it like Failures.close.
    private static void closeResource(AutoCloseable resource, Failures failures) {
        if (resource != null) {
            try {
                resource.close();
            } catch (InterruptedException closeFailure) {
                Thread.currentThread().interrupt();
                failures.add(closeFailure);
            } catch (Exception closeFailure) {
                failures.add(closeFailure);
            }
        }
    }
}
