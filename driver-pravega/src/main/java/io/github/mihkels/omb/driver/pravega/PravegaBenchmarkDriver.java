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
package io.github.mihkels.omb.driver.pravega;

import io.github.mihkels.omb.driver.BenchmarkConsumer;
import io.github.mihkels.omb.driver.BenchmarkDriver;
import io.github.mihkels.omb.driver.BenchmarkProducer;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.DriverConfigs;
import io.github.mihkels.omb.driver.Failures;
import io.github.mihkels.omb.driver.pravega.config.PravegaConfig;
import io.micrometer.core.instrument.MeterRegistry;
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
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaBenchmarkDriver implements BenchmarkDriver {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkDriver.class);

    private PravegaConfig config;
    private String scopeName;

    /**
     * The clients {@link #initialize} creates; published together once all of them exist.
     *
     * @param streamManager creates, seals and deletes streams
     * @param readerGroupManager creates the reader groups of consumers
     * @param clientFactory creates writers and readers
     */
    record Clients(
            StreamManager streamManager,
            ReaderGroupManager readerGroupManager,
            EventStreamClientFactory clientFactory) {}

    // Cleared on close
    private @Nullable Clients clients;
    private final List<String> createdTopics = new ArrayList<>();
    private int activeTopicCreations;
    private boolean closing;

    @Override
    public void initialize(final Path configurationFile, final MeterRegistry statsLogger)
            throws IOException, InterruptedException {
        config = readConfig(configurationFile);
        if (!config.includeTimestampInEvent()) {
            log.warn(
                    "includeTimestampInEvent is false: end-to-end latency is not measured and will be"
                            + " reported as empty");
        }
        ClientConfig clientConfig =
                ClientConfig.builder().controllerURI(URI.create(config.client().controllerURI())).build();
        scopeName = config.client().scopeName();
        StreamManager streamManager = null;
        ReaderGroupManager readerGroupManager = null;
        EventStreamClientFactory clientFactory = null;
        try {
            streamManager = StreamManager.create(clientConfig);
            readerGroupManager = ReaderGroupManager.withScope(scopeName, clientConfig);
            clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);
            clients = new Clients(streamManager, readerGroupManager, clientFactory);
        } catch (Exception failure) {
            Failures.closeAfterFailure(failure, clientFactory, readerGroupManager, streamManager);
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
        StreamManager streamManager;
        synchronized (this) {
            Clients current = clients;
            if (closing || current == null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Driver is closed"));
            }
            streamManager = current.streamManager();
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
        Clients current = clients;
        if (current == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Driver is closed"));
        }
        EventStreamClientFactory clientFactory = current.clientFactory();
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
        Clients current = clients;
        if (current == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Driver is closed"));
        }
        EventStreamClientFactory clientFactory = current.clientFactory();
        ReaderGroupManager readerGroupManager = current.readerGroupManager();
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
        Clients toClose;
        List<String> topics;
        InterruptedException interrupted;
        synchronized (this) {
            closing = true;
            interrupted = awaitActiveTopicCreations();
            toClose = clients;
            clients = null;
            topics = List.copyOf(createdTopics);
            createdTopics.clear();
        }
        Failures failures = new Failures();
        if (interrupted != null) {
            failures.addInterruption(interrupted);
        }
        if (toClose != null) {
            failures.closeRestoringInterruptNow(toClose.clientFactory());
            failures.closeRestoringInterruptNow(toClose.readerGroupManager());
            sealAndDeleteTopics(toClose.streamManager(), topics, failures);
            failures.closeRestoringInterruptNow(toClose.streamManager());
        }
        failures.throwIfAny();
    }

    // Waits until no topic creation is in flight. Interruptions do not stop the wait, since cleanup
    // must see every created topic; the first one is returned so the caller can restore it.
    private synchronized @Nullable InterruptedException awaitActiveTopicCreations() {
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
}
