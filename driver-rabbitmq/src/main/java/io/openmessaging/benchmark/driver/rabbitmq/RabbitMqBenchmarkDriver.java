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
package io.openmessaging.benchmark.driver.rabbitmq;

import static java.util.stream.Collectors.toList;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreator;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.yaml.YAMLFactory;

public class RabbitMqBenchmarkDriver implements BenchmarkDriver {

    private RabbitMqConfig config;
    private final AtomicInteger uriIndex = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Map of client's primary broker to the connection -- the connection may still be able to fall
     * back to secondary brokers.
     */
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();

    @Override
    public void initialize(Path configurationFile, MeterRegistry statsLogger) throws IOException {
        try (InputStream input = Files.newInputStream(configurationFile)) {
            config = reader.readValue(input);
        }
        config.validate();
    }

    @Override
    public void close() throws Exception {
        closed.set(true);
        Exception failure = null;
        for (Map.Entry<String, Connection> entry : connections.entrySet()) {
            Connection connection = entry.getValue();
            if (!connections.remove(entry.getKey(), connection)) {
                continue;
            }
            try {
                connection.close();
            } catch (AlreadyClosedException e) {
                log.warn("Connection already closed", e);
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public String getTopicNamePrefix() {
        // Distribute topics by performing a round-robin on AMQP URIs
        String primaryBrokerUri =
                config.amqpUris.get(uriIndex.getAndIncrement() % config.amqpUris.size());
        URI configUri = URI.create(primaryBrokerUri);
        URI topicUri = configUri.resolve(configUri.getRawPath() + "?exchange=test-exchange");
        return topicUri.toString();
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        if (partitions != 1) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Cannot create topic with partitions in RabbitMQ"));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        CompletableFuture<BenchmarkProducer> future = new CompletableFuture<>();
        try {
            ForkJoinPool.commonPool()
                    .execute(
                            () -> {
                                Channel channel = null;
                                try {
                                    String uri = topic.split("\\?")[0];
                                    Connection connection = getOrCreateConnection(uri);
                                    channel = Objects.requireNonNull(connection.createChannel(), "channel");
                                    String exchange = getExchangeName(topic);
                                    channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT, true);
                                    channel.confirmSelect();
                                    BenchmarkProducer producer =
                                            new RabbitMqBenchmarkProducer(channel, exchange, config.messagePersistence);
                                    channel = null;
                                    if (!future.complete(producer)) {
                                        closeDiscarded(producer);
                                    }
                                } catch (Throwable error) {
                                    closeAfterFailure(channel, error);
                                    future.completeExceptionally(error);
                                }
                            });
        } catch (Throwable error) {
            future.completeExceptionally(error);
        }
        return future;
    }

    @Override
    public CompletableFuture<List<BenchmarkProducer>> createProducers(List<ProducerInfo> producers) {
        return new ResourceCreator<ProducerInfo, BenchmarkProducer>(
                        "producer",
                        config.producerCreationBatchSize,
                        config.producerCreationDelay,
                        config.resourceCreationMaxRetries,
                        producer -> createProducer(producer.topic()))
                .create(producers);
    }

    @Override
    public CompletableFuture<List<BenchmarkConsumer>> createConsumers(List<ConsumerInfo> consumers) {
        return new ResourceCreator<ConsumerInfo, BenchmarkConsumer>(
                        "consumer",
                        config.consumerCreationBatchSize,
                        config.consumerCreationDelay,
                        config.resourceCreationMaxRetries,
                        this::getConsumer)
                .create(consumers);
    }

    private CompletableFuture<BenchmarkConsumer> getConsumer(ConsumerInfo c) {
        return createConsumer(c.topic(), c.subscriptionName(), c.consumerCallback());
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {

        CompletableFuture<BenchmarkConsumer> future = new CompletableFuture<>();
        try {
            ForkJoinPool.commonPool()
                    .execute(
                            () -> {
                                Channel channel = null;
                                try {
                                    String uri = topic.split("\\?")[0];
                                    Connection connection = getOrCreateConnection(uri);
                                    channel = Objects.requireNonNull(connection.createChannel(), "channel");
                                    String exchange = getExchangeName(topic);
                                    String queueName = exchange + "-" + subscriptionName;
                                    channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT, true);
                                    channel.queueDeclare(
                                            queueName, true, false, false, config.queueType.queueOptions());
                                    channel.queueBind(queueName, exchange, "");
                                    BenchmarkConsumer consumer =
                                            RabbitMqBenchmarkConsumer.create(
                                                    channel,
                                                    queueName,
                                                    consumerCallback,
                                                    config.queueType == RabbitMqConfig.QueueType.STREAM);
                                    channel = null;
                                    if (!future.complete(consumer)) {
                                        closeDiscarded(consumer);
                                    }
                                } catch (Throwable error) {
                                    closeAfterFailure(channel, error);
                                    future.completeExceptionally(error);
                                }
                            });
        } catch (Throwable error) {
            future.completeExceptionally(error);
        }
        return future;
    }

    private String getExchangeName(String uri) {
        QueryStringDecoder decoder = new QueryStringDecoder(uri);
        Map<String, List<String>> parameters = decoder.parameters();

        if (!parameters.containsKey("exchange")) {
            throw new IllegalArgumentException("Missing exchange param");
        }
        return parameters.get("exchange").get(0);
    }

    private Connection getOrCreateConnection(String primaryBrokerUri) {
        if (closed.get()) {
            throw new IllegalStateException("RabbitMQ driver is closed");
        }
        Connection connection =
                connections.computeIfAbsent(
                        primaryBrokerUri,
                        p -> {
                            // RabbitMQ will pick the first available address from the list. Future reconnection
                            // attempts will pick a random accessible address from the provided list.
                            List<Address> addresses =
                                    Stream.concat(Stream.of(p), config.amqpUris.stream().filter(s -> !s.equals(p)))
                                            .map(s -> newURI(s))
                                            .map(u -> new Address(u.getHost(), u.getPort()))
                                            .collect(toList());
                            try {
                                ConnectionFactory connectionFactory = new ConnectionFactory();
                                connectionFactory.setAutomaticRecoveryEnabled(true);
                                String userInfo = newURI(primaryBrokerUri).getUserInfo();
                                if (userInfo != null) {
                                    String[] userInfoElems = userInfo.split(":");
                                    connectionFactory.setUsername(userInfoElems[0]);
                                    connectionFactory.setPassword(userInfoElems[1]);
                                }
                                return connectionFactory.newConnection(addresses);
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Couldn't establish connection to: " + primaryBrokerUri, e);
                            }
                        });
        if (closed.get()) {
            if (connections.remove(primaryBrokerUri, connection)) {
                closeDiscarded(connection);
            }
            throw new IllegalStateException("RabbitMQ driver is closed");
        }
        return connection;
    }

    private static void closeAfterFailure(AutoCloseable resource, Throwable failure) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Throwable closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private static void closeDiscarded(AutoCloseable resource) {
        try {
            resource.close();
        } catch (Exception e) {
            log.warn("Could not close a resource created after cancellation", e);
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private static final ObjectReader reader =
            mapper
                    .readerFor(RabbitMqConfig.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static final Logger log = LoggerFactory.getLogger(RabbitMqBenchmarkDriver.class);

    private static URI newURI(String uri) {
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
