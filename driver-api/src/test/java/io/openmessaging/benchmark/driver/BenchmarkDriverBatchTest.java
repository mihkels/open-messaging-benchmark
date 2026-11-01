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
package io.openmessaging.benchmark.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class BenchmarkDriverBatchTest {
    @Test
    void successfulBatchTransfersResourcesInRequestOrder() {
        CompletableFuture<BenchmarkProducer> first = new CompletableFuture<>();
        CompletableFuture<BenchmarkProducer> second = new CompletableFuture<>();
        TestDriver driver = new TestDriver(topic -> topic.equals("first") ? first : second);
        CompletableFuture<List<BenchmarkProducer>> batch =
                driver.createProducers(producers("first", "second"));
        TestProducer firstProducer = new TestProducer();
        TestProducer secondProducer = new TestProducer();

        second.complete(secondProducer);
        first.complete(firstProducer);

        assertEquals(List.of(firstProducer, secondProducer), batch.join());
        assertEquals(0, firstProducer.closes.get());
        assertEquals(0, secondProducer.closes.get());
    }

    @Test
    void failureClosesKnownAndLateSuccessesWithoutLosingTheOriginalCause() {
        TestProducer brokenClose = new TestProducer();
        brokenClose.closeFailure = new IllegalStateException("close failed");
        TestProducer other = new TestProducer();
        TestProducer late = new TestProducer();
        CompletableFuture<BenchmarkProducer> lateFuture = new CompletableFuture<>();
        CompletableFuture<BenchmarkProducer> failedFuture = new CompletableFuture<>();
        TestDriver driver =
                new TestDriver(
                        topic ->
                                switch (topic) {
                                    case "first" -> CompletableFuture.completedFuture(brokenClose);
                                    case "second" -> CompletableFuture.completedFuture(other);
                                    case "late" -> lateFuture;
                                    default -> failedFuture;
                                });
        CompletableFuture<List<BenchmarkProducer>> batch =
                driver.createProducers(producers("first", "second", "late", "failed"));
        IllegalArgumentException cause = new IllegalArgumentException("creation failed");
        failedFuture.completeExceptionally(cause);

        assertSame(cause, assertThrows(CompletionException.class, batch::join).getCause());
        assertEquals(1, brokenClose.closes.get());
        assertEquals(1, other.closes.get());
        assertSame(brokenClose.closeFailure, cause.getSuppressed()[0]);
        assertFalse(lateFuture.isCancelled());
        lateFuture.complete(late);
        assertEquals(1, late.closes.get());
    }

    @Test
    void cancellationDoesNotCancelIndividualFuturesAndClosesLateConsumers() {
        CompletableFuture<BenchmarkConsumer> first = new CompletableFuture<>();
        CompletableFuture<BenchmarkConsumer> second = new CompletableFuture<>();
        TestDriver driver =
                new TestDriver(topic -> CompletableFuture.completedFuture(new TestProducer()));
        driver.consumers = topic -> topic.equals("first") ? first : second;
        CompletableFuture<List<BenchmarkConsumer>> batch =
                driver.createConsumers(
                        List.of(
                                new BenchmarkDriver.ConsumerInfo(0, "first", "s", null),
                                new BenchmarkDriver.ConsumerInfo(1, "second", "s", null)));
        CompletableFuture<Void> quiescence = ((ResourceCreationCompletion) batch).quiescence();
        TestConsumer early = new TestConsumer();
        TestConsumer late = new TestConsumer();
        first.complete(early);

        assertTrue(batch.cancel(true));
        assertTrue(batch.isCancelled());
        assertEquals(1, early.closes.get());
        assertFalse(second.isCancelled());
        assertFalse(quiescence.isDone());
        second.complete(late);
        quiescence.join();
        assertEquals(1, late.closes.get());
        assertTrue(batch.cancel(true));
    }

    @Test
    void synchronousCreationFailureStillClosesAnEarlierLateSuccess() {
        CompletableFuture<BenchmarkProducer> lateFuture = new CompletableFuture<>();
        IllegalStateException cause = new IllegalStateException("failed to start");
        TestDriver driver =
                new TestDriver(
                        topic -> {
                            if (topic.equals("first")) {
                                return lateFuture;
                            }
                            throw cause;
                        });
        CompletableFuture<List<BenchmarkProducer>> batch =
                driver.createProducers(producers("first", "second"));
        assertSame(cause, assertThrows(CompletionException.class, batch::join).getCause());
        TestProducer late = new TestProducer();
        lateFuture.complete(late);
        assertEquals(1, late.closes.get());
    }

    @Test
    void cancellationRacingSuccessNeverLosesOrDoubleClosesAHandle() throws Exception {
        for (int i = 0; i < 200; i++) {
            CompletableFuture<BenchmarkProducer> first = new CompletableFuture<>();
            CompletableFuture<BenchmarkProducer> second = new CompletableFuture<>();
            TestProducer a = new TestProducer();
            TestProducer b = new TestProducer();
            TestDriver driver = new TestDriver(topic -> topic.equals("first") ? first : second);
            CompletableFuture<List<BenchmarkProducer>> batch =
                    driver.createProducers(producers("first", "second"));
            CountDownLatch start = new CountDownLatch(1);
            Thread producer =
                    Thread.ofVirtual()
                            .start(
                                    () -> {
                                        await(start);
                                        first.complete(a);
                                        second.complete(b);
                                    });
            Thread canceller =
                    Thread.ofVirtual()
                            .start(
                                    () -> {
                                        await(start);
                                        batch.cancel(true);
                                    });
            start.countDown();
            producer.join();
            canceller.join();
            if (!batch.isCancelled()) {
                for (BenchmarkProducer resource : batch.join()) {
                    resource.close();
                }
            }
            assertEquals(1, a.closes.get());
            assertEquals(1, b.closes.get());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new AssertionError(error);
        }
    }

    private static List<BenchmarkDriver.ProducerInfo> producers(String... topics) {
        return java.util.stream.IntStream.range(0, topics.length)
                .mapToObj(index -> new BenchmarkDriver.ProducerInfo(index, topics[index]))
                .toList();
    }

    private static final class TestProducer implements BenchmarkProducer {
        private final AtomicInteger closes = new AtomicInteger();
        private RuntimeException closeFailure;

        @Override
        public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closes.incrementAndGet();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class TestConsumer implements BenchmarkConsumer {
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    private static final class TestDriver implements BenchmarkDriver {
        private final Function<String, CompletableFuture<BenchmarkProducer>> producers;
        private Function<String, CompletableFuture<BenchmarkConsumer>> consumers;

        private TestDriver(Function<String, CompletableFuture<BenchmarkProducer>> producers) {
            this.producers = producers;
        }

        @Override
        public void initialize(Path configurationFile, MeterRegistry statsLogger) {}

        @Override
        public String getTopicNamePrefix() {
            return "test";
        }

        @Override
        public CompletableFuture<Void> createTopic(String topic, int partitions) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
            return producers.apply(topic);
        }

        @Override
        public CompletableFuture<BenchmarkConsumer> createConsumer(
                String topic, String subscriptionName, ConsumerCallback callback) {
            return consumers.apply(topic);
        }

        @Override
        public void close() {}
    }
}
