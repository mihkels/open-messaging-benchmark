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
package io.openmessaging.benchmark.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.rabbitmq.RabbitMqBenchmarkDriver;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/** End-to-end tests for the RabbitMQ driver. */
@Testcontainers
class RabbitMqE2eIT extends BaseE2eIT {
    @Container
    static RabbitMQContainer rabbitmq =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management"));

    private Path driverConfigFile;

    @BeforeAll
    static void verifyRabbitMqConnection() throws Exception {
        try (Connection connection = connectionFactory().newConnection();
                var channel = connection.createChannel()) {
            channel.queueDeclare("test-connection-queue", false, true, true, null);
        }
    }

    @BeforeEach
    void setupDriver() throws Exception {
        driverConfigFile = writeDriverConfig("CLASSIC", false);
    }

    private Path writeDriverConfig(String queueType, boolean persistence) throws Exception {
        String amqpUri =
                String.format(
                        "amqp://%s:%s@%s:%d",
                        rabbitmq.getAdminUsername(),
                        rabbitmq.getAdminPassword(),
                        rabbitmq.getHost(),
                        rabbitmq.getAmqpPort());
        String driverConfig =
                """
                name: RabbitMQ
                driverClass: io.openmessaging.benchmark.driver.rabbitmq.RabbitMqBenchmarkDriver
                amqpUris:
                  - %s
                messagePersistence: %s
                queueType: %s
                producerCreationDelay: 100
                producerCreationBatchSize: 5
                consumerCreationDelay: 100
                consumerCreationBatchSize: 5
                """
                        .formatted(amqpUri, persistence, queueType);
        Path configPath = tempDir.resolve("rabbitmq-" + queueType + "-" + persistence + ".yaml");
        Files.writeString(configPath, driverConfig);
        return configPath;
    }

    @Test
    @DriverContract("rabbitmq")
    void testSimpleProduceConsume() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.producersPerTopic = 2;
        workload.consumerPerSubscription = 2;
        SmokeResult result = runBenchmark("RabbitMQ", driverConfigFile, workload);
        validateResults(result, workload);
    }

    @Test
    void testMultipleTopics() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 3;
        Set<String> before = exchanges();
        SmokeResult result = runBenchmark("RabbitMQ", driverConfigFile, workload);
        validateResults(result, workload);
        Set<String> created = new HashSet<>(exchanges());
        created.removeAll(before);
        created.removeIf(exchange -> !exchange.startsWith("test-exchange"));
        assertThat(created).hasSize(workload.topics);
    }

    @Test
    void testMultipleProducersAndConsumers() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.messageSize = 100;
        workload.producersPerTopic = 4;
        workload.consumerPerSubscription = 4;
        SmokeResult result = runBenchmark("RabbitMQ", driverConfigFile, workload);
        validateResults(result, workload);
    }

    @ParameterizedTest
    @ValueSource(strings = {"QUORUM", "STREAM"})
    void testQueueTypesAndFanout(String queueType) throws Exception {
        Workload workload = createSimpleWorkload();
        workload.subscriptionsPerTopic = 2;
        Path config = writeDriverConfig(queueType, true);
        SmokeResult result = runBenchmark("RabbitMQ", config, workload);
        validateResults(result, workload);
        verifyFanoutPayload(config);
    }

    @Test
    void testClassicQueueFanoutPayload() throws Exception {
        verifyFanoutPayload(driverConfigFile);
    }

    private void verifyFanoutPayload(Path config) throws Exception {
        byte[] payload = {0, 1, (byte) 0x80, (byte) 0xff, 42};
        MessageCapture first = new MessageCapture();
        MessageCapture second = new MessageCapture();
        try (RabbitMqBenchmarkDriver driver = new RabbitMqBenchmarkDriver()) {
            driver.initialize(config, null);
            String topic = driver.getTopicNamePrefix() + "-" + UUID.randomUUID();
            driver.createTopic(topic, 1).get(10, TimeUnit.SECONDS);
            try (var firstConsumer = driver.createConsumer(topic, "group-a", first).get(10, TimeUnit.SECONDS);
                    var secondConsumer =
                            driver.createConsumer(topic, "group-b", second).get(10, TimeUnit.SECONDS);
                    var producer = driver.createProducer(topic).get(10, TimeUnit.SECONDS)) {
                producer.sendAsync(Optional.empty(), payload).get(10, TimeUnit.SECONDS);
                for (MessageCapture capture : List.of(first, second)) {
                    var delivery = capture.await();
                    assertThat(delivery.payload()).isEqualTo(payload);
                    assertThat(delivery.timestamp()).isPositive();
                }
            }
        }
    }

    @Test
    void testMessagePersistence() throws Exception {
        byte[] payload = ("persistent-" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        String subscription = "persistent";
        String queue;
        AtomicBoolean unexpectedDelivery = new AtomicBoolean();
        try (RabbitMqBenchmarkDriver driver = new RabbitMqBenchmarkDriver()) {
            driver.initialize(writeDriverConfig("CLASSIC", true), null);
            String topic = driver.getTopicNamePrefix() + "-" + UUID.randomUUID();
            queue = URI.create(topic).getQuery().substring("exchange=".length()) + "-" + subscription;
            driver.createTopic(topic, 1).get(10, TimeUnit.SECONDS);
            try (var consumer =
                    driver.createConsumer(
                                    topic,
                                    subscription,
                                    new ConsumerCallback() {
                                        @Override
                                        public void messageReceived(byte[] message, long timestamp) {
                                            unexpectedDelivery.set(true);
                                        }

                                        @Override
                                        public void messageReceived(ByteBuffer message, long timestamp) {
                                            unexpectedDelivery.set(true);
                                        }
                                    })
                            .get(10, TimeUnit.SECONDS)) {
                assertThat(consumer).isNotNull();
            }
            try (var producer = driver.createProducer(topic).get(10, TimeUnit.SECONDS)) {
                producer.sendAsync(Optional.empty(), payload).get(10, TimeUnit.SECONDS);
            }
        }
        assertThat(unexpectedDelivery).isFalse();

        var stop = rabbitmq.execInContainer("rabbitmqctl", "stop_app");
        assertThat(stop.getExitCode()).as("stopping RabbitMQ: %s", stop.getStderr()).isZero();
        var start = rabbitmq.execInContainer("rabbitmqctl", "start_app");
        assertThat(start.getExitCode()).as("restarting RabbitMQ: %s", start.getStderr()).isZero();
        try (Connection connection = waitForRabbitMq();
                var channel = connection.createChannel()) {
            assertThat(channel.queueDeclarePassive(queue).getMessageCount()).isEqualTo(1);
            var message = channel.basicGet(queue, true);
            assertThat(message).isNotNull();
            assertThat(message.getProps().getDeliveryMode()).isEqualTo(2);
            assertThat(message.getBody()).isEqualTo(payload);
        }
    }

    private static Set<String> exchanges() throws Exception {
        var result = rabbitmq.execInContainer("rabbitmqctl", "-q", "list_exchanges", "name");
        assertThat(result.getExitCode())
                .as("listing RabbitMQ exchanges: %s", result.getStderr())
                .isZero();
        return result.getStdout().lines().map(String::trim).collect(Collectors.toSet());
    }

    private static ConnectionFactory connectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitmq.getHost());
        factory.setPort(rabbitmq.getAmqpPort());
        factory.setUsername(rabbitmq.getAdminUsername());
        factory.setPassword(rabbitmq.getAdminPassword());
        factory.setConnectionTimeout(1000);
        return factory;
    }

    private static Connection waitForRabbitMq() throws Exception {
        ConnectionFactory factory = connectionFactory();
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        Exception lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                return factory.newConnection();
            } catch (IOException | TimeoutException e) {
                lastFailure = e;
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("RabbitMQ did not become ready after restart", lastFailure);
    }
}
