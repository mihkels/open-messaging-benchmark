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
package io.openmessaging.benchmark.driver.natsstreaming;

import io.micrometer.core.instrument.MeterRegistry;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.SubscriptionOptions;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.DriverConfigs;
import io.openmessaging.benchmark.driver.ResourceNames;
import io.openmessaging.benchmark.driver.SecretRedaction;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.LoggerFactory;

public class NatsStreamingBenchmarkDriver implements BenchmarkDriver {
    private static final String DEFAULT_CLUSTER_ID = "test-cluster";
    private final ConnectionFactory connectionFactory;
    private String clusterId;
    private StreamingConnection natsStreamingPublisher;
    private final SubscriptionOptions.Builder subBuilder = new SubscriptionOptions.Builder();
    private final Options.Builder optsBuilder = new Options.Builder();
    private volatile boolean closed;

    public NatsStreamingBenchmarkDriver() {
        this(NatsStreaming::connect);
    }

    NatsStreamingBenchmarkDriver(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @FunctionalInterface
    interface ConnectionFactory {
        StreamingConnection connect(String clusterId, String clientId, Options options)
                throws IOException, InterruptedException;
    }

    @Override
    public void initialize(Path configurationFile, MeterRegistry statsLogger) throws IOException {
        NatsStreamingClientConfig config =
                DriverConfigs.read(configurationFile, NatsStreamingClientConfig.class);
        log.atInfo()
                .setMessage("NATS Streaming driver configuration: {}")
                .addArgument(() -> SecretRedaction.redactedJson(config))
                .log();
        if (config.clusterId != null) {
            clusterId = config.clusterId;
        } else {
            clusterId = DEFAULT_CLUSTER_ID;
        }
        if (config.natsHostUrl != null) {
            optsBuilder.natsUrl(config.natsHostUrl);
            log.info("natUrl {}", SecretRedaction.redactString(config.natsHostUrl));
        }
        optsBuilder.maxPubAcksInFlight(config.maxPubAcksInFlight);

        subBuilder.maxInFlight(config.maxInFlight);
    }

    @Override
    public String getTopicNamePrefix() {
        return "Nats-streaming-benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        log.info("nats streaming create a topic {}", topic);
        log.info("ignore partitions");
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);
        return future;
    }

    @Override
    public synchronized CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("NATS Streaming driver is closed"));
        }
        if (natsStreamingPublisher == null) {
            String clientId = "ProducerInstance" + ResourceNames.randomSuffix();
            try {
                natsStreamingPublisher =
                        connectionFactory.connect(clusterId, clientId, optsBuilder.build());
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.warn("nats streaming create producer exception", e);
                return CompletableFuture.failedFuture(e);
            }
        }

        return CompletableFuture.completedFuture(
                new NatsStreamingBenchmarkProducer(natsStreamingPublisher, topic));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("NATS Streaming driver is closed"));
        }
        StreamingConnection streamingConnection = null;
        String clientId = "ConsumerInstance" + ResourceNames.randomSuffix();
        try {
            streamingConnection = connectionFactory.connect(clusterId, clientId, optsBuilder.build());
            streamingConnection.subscribe(
                    topic,
                    subscriptionName,
                    // NATS Streaming timestamps are nanoseconds since the epoch
                    message ->
                            consumerCallback.messageReceived(
                                    message.getData(), TimeUnit.NANOSECONDS.toMillis(message.getTimestamp())),
                    subBuilder.build());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            closeAfterFailure(streamingConnection, e);
            log.warn("nats streaming create consumer exception", e);
            return CompletableFuture.failedFuture(e);
        }
        return CompletableFuture.completedFuture(
                new NatsStreamingBenchmarkConsumer(streamingConnection));
    }

    private static void closeAfterFailure(StreamingConnection connection, Exception failure) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (Exception closeFailure) {
            if (closeFailure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    @Override
    public synchronized void close() throws IOException, InterruptedException, TimeoutException {
        closed = true;
        if (natsStreamingPublisher != null) {
            StreamingConnection publisher = natsStreamingPublisher;
            natsStreamingPublisher = null;
            publisher.close();
        }
    }

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(NatsStreamingBenchmarkDriver.class);
}
