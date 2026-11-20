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
package io.github.mihkels.omb.driver.artemis;

import io.github.mihkels.omb.driver.BenchmarkConsumer;
import io.github.mihkels.omb.driver.BenchmarkDriver;
import io.github.mihkels.omb.driver.BenchmarkProducer;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.DriverConfigs;
import io.github.mihkels.omb.driver.Failures;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
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
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtemisBenchmarkDriver implements BenchmarkDriver {
    private @Nullable ServerLocator serverLocator;
    private @Nullable ClientSessionFactory sessionFactory;
    private @Nullable ClientSession session;

    @Override
    public synchronized void initialize(Path configurationFile, MeterRegistry statsLogger)
            throws IOException {
        ArtemisConfig config = readConfig(configurationFile);
        String brokerAddress = config.brokerAddress;
        if (brokerAddress == null) {
            throw new IllegalArgumentException("Artemis config needs brokerAddress");
        }
        try {
            ServerLocator locator = ActiveMQClient.createServerLocator(brokerAddress);
            serverLocator = locator;
            // confirmation window size is in bytes, set to 1MB
            locator.setConfirmationWindowSize(1024 * 1024);
            // use asynchronous sending of messages where SendAcknowledgementHandler reports
            // success/failure
            locator.setBlockOnDurableSend(false);
            locator.setBlockOnNonDurableSend(false);
            // use async acknowledgement
            locator.setBlockOnAcknowledge(false);

            ClientSessionFactory factory = locator.createSessionFactory();
            sessionFactory = factory;
            session = factory.createSession();
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
    public boolean supportsHeaders() {
        return true;
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

        ClientSession session = this.session;
        if (session == null) {
            return CompletableFuture.failedFuture(notInitialized());
        }
        try {
            return CompletableFuture.runAsync(() -> {
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
        ClientSessionFactory sessionFactory = this.sessionFactory;
        if (sessionFactory == null) {
            return CompletableFuture.failedFuture(notInitialized());
        }
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
        ClientSessionFactory sessionFactory = this.sessionFactory;
        if (sessionFactory == null) {
            future.completeExceptionally(notInitialized());
            return future;
        }
        try {
            CompletableFuture.runAsync(
                            () -> {
                                try {
                                    String queueName = topic + "-" + subscriptionName;
                                    BenchmarkConsumer consumer =
                                            ArtemisBenchmarkConsumer.create(
                                                    topic, queueName, sessionFactory, consumerCallback);
                                    if (!future.complete(consumer)) {
                                        closeUntransferred(consumer);
                                    }
                                } catch (ActiveMQException e) {
                                    throw new CompletionException(e);
                                }
                            })
                    .whenComplete((_, failure) -> {
                        if (failure != null) {
                            future.completeExceptionally(failure);
                        }
                    });
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    private static IllegalStateException notInitialized() {
        return new IllegalStateException("Artemis driver is not initialized or already closed");
    }

    static void closeUntransferred(BenchmarkConsumer consumer) {
        try {
            consumer.close();
        } catch (Exception closeFailure) {
            if (closeFailure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("Failed to close an untransferred Artemis consumer", closeFailure);
        }
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

        Failures failures = new Failures();
        failures.closeRestoringInterruptNow(sessionToClose);
        failures.closeRestoringInterruptNow(factoryToClose);
        failures.closeRestoringInterruptNow(locatorToClose);
        failures.throwIfAny();

        log.info("ActiveMQ Artemis benchmark driver successfully shut down");
    }

    private static ArtemisConfig readConfig(Path configurationFile) throws IOException {
        return DriverConfigs.read(configurationFile, ArtemisConfig.class);
    }

    private static final Logger log = LoggerFactory.getLogger(ArtemisBenchmarkDriver.class);
}
