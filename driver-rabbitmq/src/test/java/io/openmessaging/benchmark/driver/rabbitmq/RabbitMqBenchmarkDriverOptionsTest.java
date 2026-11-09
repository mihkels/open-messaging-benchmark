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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RabbitMqBenchmarkDriverOptionsTest {
    @TempDir Path dir;

    @Test
    void topicPrefixRoundRobinsOverBrokers() throws Exception {
        RabbitMqBenchmarkDriver driver = driver("amqpUris: [amqp://a:5672/vh, amqp://b:5672]\n");
        assertThat(driver.getTopicNamePrefix()).isEqualTo("amqp://a:5672/vh?exchange=test-exchange");
        assertThat(driver.getTopicNamePrefix()).isEqualTo("amqp://b:5672?exchange=test-exchange");
        assertThat(driver.getTopicNamePrefix()).startsWith("amqp://a:5672");
        assertThat(driver.createTopic("t", 1)).isCompleted();
        driver.close();
    }

    @Test
    void createsProducerAndStreamConsumerOnACachedConnection() throws Exception {
        RabbitMqBenchmarkDriver driver =
                driver("amqpUris: [amqp://test]\nqueueType: STREAM\nmessagePersistence: true\n");
        List<String> calls = new ArrayList<>();
        Channel channel = recording(Channel.class, calls);
        connections(driver).put("amqp://test", recording(Connection.class, calls, channel));

        BenchmarkProducer producer = driver.createProducer("amqp://test?exchange=ex").join();
        BenchmarkConsumer consumer =
                driver.createConsumer("amqp://test?exchange=ex", "sub", null).join();
        assertThat(calls)
                .contains("exchangeDeclare", "confirmSelect", "queueDeclare", "queueBind", "basicQos");
        producer.close();
        consumer.close();
        driver.close();
    }

    @Test
    void missingExchangeParameterFailsAndClosesTheChannel() throws Exception {
        RabbitMqBenchmarkDriver driver = driver("amqpUris: [amqp://test]\n");
        List<String> calls = new ArrayList<>();
        connections(driver)
                .put("amqp://test", recording(Connection.class, calls, recording(Channel.class, calls)));
        assertThatThrownBy(() -> driver.createProducer("amqp://test").join())
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing exchange param");
        assertThatThrownBy(() -> driver.createConsumer("amqp://test", "s", null).join())
                .hasMessageContaining("Missing exchange param");
        assertThat(calls).contains("close");
        driver.close();
    }

    @Test
    void nullChannelFailsWithoutAnythingToClose() throws Exception {
        RabbitMqBenchmarkDriver driver = driver("amqpUris: [amqp://test]\n");
        connections(driver).put("amqp://test", recording(Connection.class, new ArrayList<>()));
        assertThatThrownBy(() -> driver.createProducer("amqp://test?exchange=e").join())
                .hasCauseInstanceOf(NullPointerException.class);
        driver.close();
    }

    @Test
    void credentialsStayOutOfTopicNamesAndConnectErrors() throws Exception {
        RabbitMqBenchmarkDriver driver =
                driver("amqpUris: [amqp://user:p%40ss@127.0.0.1:1, amqp://user@127.0.0.1:2]\n");
        String withPassword = driver.getTopicNamePrefix();
        String withoutPassword = driver.getTopicNamePrefix();
        assertThat(withPassword).isEqualTo("amqp://127.0.0.1:1?exchange=test-exchange");
        assertThat(withoutPassword).isEqualTo("amqp://127.0.0.1:2?exchange=test-exchange");

        Throwable producerFailure =
                org.assertj.core.api.Assertions.catchThrowable(
                        () -> driver.createProducer(withPassword).join());
        assertThat(producerFailure)
                .hasMessageContaining("Couldn't establish connection to: amqp://127.0.0.1:1")
                .hasRootCauseInstanceOf(IOException.class);
        java.io.StringWriter trace = new java.io.StringWriter();
        producerFailure.printStackTrace(new java.io.PrintWriter(trace));
        assertThat(trace.toString()).doesNotContain("p%40ss", "p@ss");

        // A user without a password parses and reaches the connect attempt.
        assertThatThrownBy(() -> driver.createConsumer(withoutPassword, "s", null).join())
                .hasMessageContaining("Couldn't establish connection to: amqp://127.0.0.1:2")
                .hasRootCauseInstanceOf(IOException.class);
        driver.close();
    }

    @Test
    void closedDriverRejectsNewResources() throws Exception {
        RabbitMqBenchmarkDriver driver = driver("amqpUris: [amqp://test]\n");
        driver.close();
        assertThatThrownBy(() -> driver.createProducer("amqp://test?exchange=e").join())
                .hasMessageContaining("RabbitMQ driver is closed");
    }

    @Test
    void closeReportsTheFirstFailureAndSuppressesTheRest() throws Exception {
        RabbitMqBenchmarkDriver driver = driver("amqpUris: [amqp://test]\n");
        IOException first = new IOException("first");
        IOException second = new IOException("second");
        Map<String, Connection> connections = connections(driver);
        connections.put("a", failingConnection(first));
        connections.put("b", failingConnection(second));
        connections.put(
                "c",
                failingConnection(
                        new AlreadyClosedException(new ShutdownSignalException(false, false, null, null))));
        connections.put("d", failingConnection(first));

        Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(driver::close);
        assertThat(thrown).isIn(first, second);
        assertThat(thrown.getSuppressed()).contains(thrown == first ? second : first);
        assertThat(connections).isEmpty();
    }

    @Test
    void configValidationRejectsEachInvalidSetting() {
        assertThatThrownBy(() -> config(c -> c.consumerCreationBatchSize = 0).validate())
                .hasMessageContaining("batch sizes");
        assertThatThrownBy(() -> config(c -> c.producerCreationBatchSize = 0).validate())
                .hasMessageContaining("batch sizes");
        assertThatThrownBy(() -> config(c -> c.consumerCreationDelay = -1).validate())
                .hasMessageContaining("delays");
        assertThatThrownBy(() -> config(c -> c.producerCreationDelay = -1).validate())
                .hasMessageContaining("delays");
        config(c -> {}).validate();
    }

    private static RabbitMqConfig config(java.util.function.Consumer<RabbitMqConfig> change) {
        RabbitMqConfig config = new RabbitMqConfig();
        change.accept(config);
        return config;
    }

    private RabbitMqBenchmarkDriver driver(String yaml) throws Exception {
        Path file = dir.resolve("rabbit-" + System.nanoTime() + ".yaml");
        Files.writeString(file, "producerCreationDelay: 0\nconsumerCreationDelay: 0\n" + yaml);
        RabbitMqBenchmarkDriver driver = new RabbitMqBenchmarkDriver();
        driver.initialize(file, null);
        return driver;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Connection> connections(RabbitMqBenchmarkDriver driver)
            throws Exception {
        Field field = RabbitMqBenchmarkDriver.class.getDeclaredField("connections");
        field.setAccessible(true);
        return (Map<String, Connection>) field.get(driver);
    }

    private static Connection failingConnection(Exception failure) {
        return (Connection)
                Proxy.newProxyInstance(
                        Connection.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("close")) {
                                throw failure;
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private static <T> T recording(Class<T> type, List<String> calls, Object... returns) {
        return type.cast(
                Proxy.newProxyInstance(
                        type.getClassLoader(),
                        new Class<?>[] {type},
                        (proxy, method, args) -> {
                            calls.add(method.getName());
                            for (Object value : returns) {
                                if (method.getReturnType().isInstance(value)) {
                                    return value;
                                }
                            }
                            if (method.getName().equals("isOpen")) {
                                return true;
                            }
                            return defaultValue(method.getReturnType());
                        }));
    }

    private static Object defaultValue(Class<?> type) {
        if (type == boolean.class) {
            return false;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == int.class) {
            return 0;
        }
        return null;
    }
}
