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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver.ConsumerInfo;
import io.openmessaging.benchmark.driver.BenchmarkDriver.ProducerInfo;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class RabbitMqBenchmarkDriverTest {
    @Test
    void unsupportedPartitionsCompleteExceptionally() {
        RabbitMqBenchmarkDriver driver = new RabbitMqBenchmarkDriver();

        CompletionException failure =
                assertThrows(CompletionException.class, () -> driver.createTopic("topic", 2).join());
        assertTrue(failure.getCause() instanceof IllegalArgumentException);
    }

    @Test
    void producerAndConsumerExhaustionUsesElevenAttemptsByDefault() throws Exception {
        FailureDriver driver = new FailureDriver();
        initialize(driver, "rabbitmq-creation-test.yaml");
        try {
            CompletionException producerFailure =
                    assertThrows(
                            CompletionException.class,
                            () -> driver.createProducers(List.of(new ProducerInfo(0, "topic"))).join());
            assertSame(driver.cause, producerFailure.getCause());
            assertEquals(11, driver.producerAttempts.get());

            CompletionException consumerFailure =
                    assertThrows(
                            CompletionException.class,
                            () ->
                                    driver.createConsumers(List.of(new ConsumerInfo(0, "topic", "s", null))).join());
            assertSame(driver.cause, consumerFailure.getCause());
            assertEquals(11, driver.consumerAttempts.get());
        } finally {
            driver.close();
        }
    }

    @Test
    void zeroRetriesFailsOnceAndClosesPartialAndLateResults() throws Exception {
        FailureDriver driver = new FailureDriver();
        initialize(driver, "rabbitmq-no-retries.yaml");
        TestProducer early = new TestProducer();
        TestProducer late = new TestProducer();
        CompletableFuture<BenchmarkProducer> delayed = new CompletableFuture<>();
        CompletableFuture<BenchmarkProducer> failed = new CompletableFuture<>();
        driver.producerFunction =
                topic ->
                        switch (topic) {
                            case "early" -> CompletableFuture.completedFuture(early);
                            case "late" -> delayed;
                            default -> failed;
                        };
        try {
            CompletableFuture<List<BenchmarkProducer>> batch =
                    driver.createProducers(
                            List.of(
                                    new ProducerInfo(0, "early"),
                                    new ProducerInfo(1, "late"),
                                    new ProducerInfo(2, "failed")));
            await(() -> failed.getNumberOfDependents() > 0);
            failed.completeExceptionally(driver.cause);
            assertSame(driver.cause, assertThrows(CompletionException.class, batch::join).getCause());
            assertEquals(3, driver.producerAttempts.get());
            assertEquals(1, early.closes.get());
            assertFalse(delayed.isCancelled());
            delayed.complete(late);
            assertEquals(1, late.closes.get());
        } finally {
            driver.close();
        }
    }

    @Test
    void producerCreationCanSucceedAfterRetries() throws Exception {
        FailureDriver driver = new FailureDriver();
        initialize(driver, "rabbitmq-creation-test.yaml");
        TestProducer producer = new TestProducer();
        driver.producerFunction =
                topic ->
                        driver.producerAttempts.get() < 3
                                ? CompletableFuture.failedFuture(driver.cause)
                                : CompletableFuture.completedFuture(producer);
        try {
            assertEquals(
                    List.of(producer), driver.createProducers(List.of(new ProducerInfo(0, "topic"))).join());
            assertEquals(3, driver.producerAttempts.get());
            assertEquals(0, producer.closes.get());
            producer.close();
            assertEquals(1, producer.closes.get());
        } finally {
            driver.close();
        }
    }

    @Test
    void invalidConfigurationIsRejectedDuringInitialization() {
        RabbitMqBenchmarkDriver driver = new RabbitMqBenchmarkDriver();
        assertThrows(
                IllegalArgumentException.class, () -> initialize(driver, "rabbitmq-invalid-retries.yaml"));
    }

    @Test
    void closesProducerChannelWhenDeclarationFailsAndKeepsTheCause() throws Exception {
        RabbitMqBenchmarkDriver driver = new RabbitMqBenchmarkDriver();
        initialize(driver, "rabbitmq-creation-test.yaml");
        AtomicInteger channelCloses = new AtomicInteger();
        IOException cause = new IOException("exchange declaration");
        IOException closeFailure = new IOException("close channel");
        Channel channel =
                proxy(
                        Channel.class,
                        (method, args) -> {
                            if (method.equals("exchangeDeclare")) {
                                throw cause;
                            }
                            if (method.equals("close")) {
                                channelCloses.incrementAndGet();
                                throw closeFailure;
                            }
                            return null;
                        });
        Connection connection =
                proxy(Connection.class, (method, args) -> method.equals("createChannel") ? channel : null);
        connections(driver).put("amqp://test", connection);
        try {
            CompletionException failure =
                    assertThrows(
                            CompletionException.class,
                            () -> driver.createProducer("amqp://test?exchange=example").join());
            assertSame(cause, failure.getCause());
            assertSame(closeFailure, cause.getSuppressed()[0]);
            assertEquals(1, channelCloses.get());
        } finally {
            driver.close();
        }
    }

    @Test
    void closesConsumerChannelOnRuntimeFailure() throws Exception {
        RabbitMqBenchmarkDriver driver = new RabbitMqBenchmarkDriver();
        initialize(driver, "rabbitmq-creation-test.yaml");
        AtomicInteger channelCloses = new AtomicInteger();
        IllegalStateException cause = new IllegalStateException("queue declaration");
        Channel channel =
                proxy(
                        Channel.class,
                        (method, args) -> {
                            if (method.equals("queueDeclare")) {
                                throw cause;
                            }
                            if (method.equals("close")) {
                                channelCloses.incrementAndGet();
                            }
                            return null;
                        });
        Connection connection =
                proxy(Connection.class, (method, args) -> method.equals("createChannel") ? channel : null);
        connections(driver).put("amqp://test", connection);
        try {
            CompletionException failure =
                    assertThrows(
                            CompletionException.class,
                            () -> driver.createConsumer("amqp://test?exchange=example", "sub", null).join());
            assertSame(cause, failure.getCause());
            assertEquals(1, channelCloses.get());
        } finally {
            driver.close();
        }
    }

    @Test
    void cancelledSingleCreationClosesAChannelProducedLater() throws Exception {
        RabbitMqBenchmarkDriver driver = new RabbitMqBenchmarkDriver();
        initialize(driver, "rabbitmq-creation-test.yaml");
        AtomicInteger channelCloses = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Channel channel =
                proxy(
                        Channel.class,
                        (method, args) -> {
                            if (method.equals("exchangeDeclare")) {
                                entered.countDown();
                                release.await();
                            } else if (method.equals("close")) {
                                channelCloses.incrementAndGet();
                            } else if (method.equals("getNextPublishSeqNo")) {
                                return 1L;
                            } else if (method.equals("isOpen")) {
                                return true;
                            }
                            return null;
                        });
        Connection connection =
                proxy(Connection.class, (method, args) -> method.equals("createChannel") ? channel : null);
        connections(driver).put("amqp://test", connection);
        try {
            CompletableFuture<BenchmarkProducer> future =
                    driver.createProducer("amqp://test?exchange=example");
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertTrue(future.cancel(true));
            release.countDown();
            await(() -> channelCloses.get() == 1);
        } finally {
            release.countDown();
            driver.close();
        }
    }

    private static void initialize(RabbitMqBenchmarkDriver driver, String resource) throws Exception {
        driver.initialize(
                Path.of(RabbitMqBenchmarkDriverTest.class.getResource("/" + resource).toURI()),
                new SimpleMeterRegistry());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Connection> connections(RabbitMqBenchmarkDriver driver)
            throws Exception {
        Field field = RabbitMqBenchmarkDriver.class.getDeclaredField("connections");
        field.setAccessible(true);
        return (Map<String, Connection>) field.get(driver);
    }

    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return type.cast(
                Proxy.newProxyInstance(
                        type.getClassLoader(),
                        new Class<?>[] {type},
                        (instance, method, args) -> invocation.invoke(method.getName(), args)));
    }

    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(condition.getAsBoolean(), "condition not met before timeout");
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(String method, Object[] args) throws Throwable;
    }

    private static final class TestProducer implements BenchmarkProducer {
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    private static final class FailureDriver extends RabbitMqBenchmarkDriver {
        private final AtomicInteger producerAttempts = new AtomicInteger();
        private final AtomicInteger consumerAttempts = new AtomicInteger();
        private final RuntimeException cause = new RuntimeException("creation failed");
        private java.util.function.Function<String, CompletableFuture<BenchmarkProducer>>
                producerFunction = topic -> CompletableFuture.failedFuture(cause);

        @Override
        public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
            producerAttempts.incrementAndGet();
            return producerFunction.apply(topic);
        }

        @Override
        public CompletableFuture<BenchmarkConsumer> createConsumer(
                String topic, String subscriptionName, ConsumerCallback callback) {
            consumerAttempts.incrementAndGet();
            return CompletableFuture.failedFuture(cause);
        }
    }
}
