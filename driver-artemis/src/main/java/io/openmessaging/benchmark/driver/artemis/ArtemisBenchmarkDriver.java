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
package io.openmessaging.benchmark.driver.artemis;

// import tools.jackson.databind.DeserializationFeature;
import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.yaml.YAMLFactory;

public class ArtemisBenchmarkDriver implements BenchmarkDriver {
    private ArtemisConfig config;

    private ServerLocator serverLocator;
    private ClientSessionFactory sessionFactory;
    private ClientSession session;

    @Override
    public synchronized void initialize(Path configurationFile, MeterRegistry statsLogger)
            throws IOException {
        this.config = readConfig(configurationFile);
        log.info("ActiveMQ Artemis driver configuration: {}", writer.writeValueAsString(config));
        try {
            serverLocator = ActiveMQClient.createServerLocator(config.brokerAddress);
            // confirmation window size is in bytes, set to 1MB
            serverLocator.setConfirmationWindowSize(1024 * 1024);
            // use asynchronous sending of messages where SendAcknowledgementHandler reports
            // success/failure
            serverLocator.setBlockOnDurableSend(false);
            serverLocator.setBlockOnNonDurableSend(false);
            // use async acknowledgement
            serverLocator.setBlockOnAcknowledge(false);

            sessionFactory = serverLocator.createSessionFactory();
            session = sessionFactory.createSession();
        } catch (Exception e) {
            try {
                close();
            } catch (Exception closeFailure) {
                if (closeFailure != e) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw new IOException(e);
        }
    }

    @Override
    public String getTopicNamePrefix() {
        return "test";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        if (partitions != 1) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Partitions are not supported in Artemis"));
        }

        try {
            return CompletableFuture.runAsync(
                    () -> {
                        try {
                            session.createAddress(SimpleString.of(topic), RoutingType.MULTICAST, true);
                        } catch (ActiveMQException e) {
                            throw new CompletionException(e);
                        }
                    });
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        try {
            return CompletableFuture.completedFuture(
                    ArtemisBenchmarkProducer.create(topic, sessionFactory));
        } catch (ActiveMQException | RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        CompletableFuture<BenchmarkConsumer> future = new CompletableFuture<>();
        try {
            CompletableFuture.runAsync(
                            () -> {
                                try {
                                    String queueName = topic + "-" + subscriptionName;
                                    BenchmarkConsumer consumer =
                                            ArtemisBenchmarkConsumer.create(
                                                    topic, queueName, sessionFactory, consumerCallback);
                                    if (!future.complete(consumer)) {
                                        try {
                                            consumer.close();
                                        } catch (Exception closeFailure) {
                                            if (closeFailure instanceof InterruptedException) {
                                                Thread.currentThread().interrupt();
                                            }
                                            log.warn("Failed to close an untransferred Artemis consumer", closeFailure);
                                        }
                                    }
                                } catch (ActiveMQException e) {
                                    throw new CompletionException(e);
                                }
                            })
                    .whenComplete(
                            (unused, failure) -> {
                                if (failure != null) {
                                    future.completeExceptionally(failure);
                                }
                            });
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public void close() throws Exception {
        log.info("Shutting down ActiveMQ Artemis benchmark driver");

        ClientSession sessionToClose;
        ClientSessionFactory factoryToClose;
        ServerLocator locatorToClose;
        synchronized (this) {
            sessionToClose = session;
            factoryToClose = sessionFactory;
            locatorToClose = serverLocator;
            session = null;
            sessionFactory = null;
            serverLocator = null;
        }

        Exception failure = closeResource(sessionToClose, null);
        failure = closeResource(factoryToClose, failure);
        failure = closeResource(locatorToClose, failure);
        if (failure != null) {
            throw failure;
        }

        log.info("ActiveMQ Artemis benchmark driver successfully shut down");
    }

    private static Exception closeResource(AutoCloseable resource, Exception failure) {
        if (resource == null) {
            return failure;
        }
        try {
            resource.close();
        } catch (Exception closeFailure) {
            if (closeFailure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (failure == null) {
                return closeFailure;
            }
            if (failure != closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
        return failure;
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectReader reader =
            mapper
                    .readerFor(ArtemisConfig.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static ArtemisConfig readConfig(Path configurationFile) throws IOException {
        return reader.readValue(Files.newInputStream(configurationFile));
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
    private static final Logger log = LoggerFactory.getLogger(ArtemisBenchmarkDriver.class);
}
