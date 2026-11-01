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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class ResourceCreatorTest {
    @Test
    void defaultRabbitMqLimitMeansElevenAttemptsAndPreservesFirstCause() {
        AtomicInteger attempts = new AtomicInteger();
        IllegalArgumentException original = new IllegalArgumentException("first failure");
        ResourceCreator<String, TestResource> creator =
                new ResourceCreator<>(
                        "test",
                        1,
                        0,
                        10,
                        request ->
                                CompletableFuture.failedFuture(
                                        attempts.getAndIncrement() == 0
                                                ? original
                                                : new IllegalStateException("later failure")));

        CompletionException failure =
                assertThrows(CompletionException.class, () -> creator.create(List.of("a")).join());
        assertEquals(11, attempts.get());
        assertSame(original, failure.getCause());
    }

    @Test
    void zeroRetriesAttemptsOnceAndRetryCanSucceed() {
        AtomicInteger attempts = new AtomicInteger();
        RuntimeException cause = new RuntimeException("failure");
        ResourceCreator<String, TestResource> noRetry =
                new ResourceCreator<>(
                        "test",
                        1,
                        0,
                        0,
                        request -> {
                            attempts.incrementAndGet();
                            return CompletableFuture.failedFuture(cause);
                        });
        assertSame(
                cause,
                assertThrows(CompletionException.class, () -> noRetry.create(List.of("a")).join())
                        .getCause());
        assertEquals(1, attempts.get());

        attempts.set(0);
        TestResource resource = new TestResource();
        ResourceCreator<String, TestResource> retry =
                new ResourceCreator<>(
                        "test",
                        1,
                        0,
                        2,
                        request ->
                                attempts.incrementAndGet() < 3
                                        ? CompletableFuture.failedFuture(cause)
                                        : CompletableFuture.completedFuture(resource));
        assertEquals(List.of(resource), retry.create(List.of("a")).join());
        assertEquals(3, attempts.get());
        assertEquals(0, resource.closes.get());
    }

    @Test
    void exhaustionClosesPartialAndLateResultsWithoutCancellingTheirFutures() {
        TestResource partial = new TestResource();
        TestResource late = new TestResource();
        CompletableFuture<TestResource> pending = new CompletableFuture<>();
        CompletableFuture<TestResource> failure = new CompletableFuture<>();
        RuntimeException cause = new RuntimeException("creation failed");
        ResourceCreator<String, TestResource> creator =
                new ResourceCreator<>(
                        "test",
                        3,
                        0,
                        0,
                        request ->
                                switch (request) {
                                    case "partial" -> CompletableFuture.completedFuture(partial);
                                    case "late" -> pending;
                                    default -> failure;
                                });
        CompletableFuture<List<TestResource>> batch =
                creator.create(List.of("partial", "late", "fail"));
        await(() -> failure.getNumberOfDependents() > 0);
        failure.completeExceptionally(cause);

        assertSame(cause, assertThrows(CompletionException.class, batch::join).getCause());
        assertEquals(1, partial.closes.get());
        assertFalse(pending.isCancelled());
        pending.complete(late);
        assertEquals(1, late.closes.get());
    }

    @Test
    void interruptionIsNeverRetried() {
        AtomicInteger attempts = new AtomicInteger();
        InterruptedException interrupted = new InterruptedException("interrupted");
        ResourceCreator<String, TestResource> creator =
                new ResourceCreator<>(
                        "test",
                        1,
                        0,
                        10,
                        request -> {
                            attempts.incrementAndGet();
                            return CompletableFuture.failedFuture(new CompletionException(interrupted));
                        });
        assertSame(
                interrupted,
                assertThrows(CompletionException.class, () -> creator.create(List.of("a")).join())
                        .getCause());
        assertEquals(1, attempts.get());
    }

    @Test
    void interruptionClosesPartialAndLateResults() {
        TestResource partial = new TestResource();
        TestResource late = new TestResource();
        CompletableFuture<TestResource> delayed = new CompletableFuture<>();
        CompletableFuture<TestResource> failed = new CompletableFuture<>();
        InterruptedException interrupted = new InterruptedException("interrupted");
        ResourceCreator<String, TestResource> creator =
                new ResourceCreator<>(
                        "test",
                        3,
                        0,
                        10,
                        request ->
                                switch (request) {
                                    case "partial" -> CompletableFuture.completedFuture(partial);
                                    case "late" -> delayed;
                                    default -> failed;
                                });
        CompletableFuture<List<TestResource>> batch =
                creator.create(List.of("partial", "late", "failed"));
        await(() -> failed.getNumberOfDependents() > 0);
        failed.completeExceptionally(interrupted);

        assertSame(interrupted, assertThrows(CompletionException.class, batch::join).getCause());
        assertEquals(1, partial.closes.get());
        delayed.complete(late);
        assertEquals(1, late.closes.get());
    }

    @Test
    void batchesWaitForAllAttemptsAndPreserveDuplicateRequests() {
        ConcurrentLinkedQueue<CompletableFuture<TestResource>> attempts = new ConcurrentLinkedQueue<>();
        ResourceCreator<String, TestResource> creator =
                new ResourceCreator<>(
                        "test",
                        2,
                        0,
                        0,
                        request -> {
                            CompletableFuture<TestResource> attempt = new CompletableFuture<>();
                            attempts.add(attempt);
                            return attempt;
                        });
        CompletableFuture<List<TestResource>> batch = creator.create(List.of("same", "same", "other"));
        await(() -> attempts.size() == 2);
        CompletableFuture<TestResource> first = attempts.remove();
        CompletableFuture<TestResource> second = attempts.remove();
        TestResource a = new TestResource();
        TestResource b = new TestResource();
        TestResource c = new TestResource();
        first.complete(a);
        assertTrue(attempts.isEmpty());
        second.complete(b);
        await(() -> attempts.size() == 1);
        attempts.remove().complete(c);
        assertEquals(List.of(a, b, c), batch.join());
    }

    @Test
    void waitsBeforeTheFirstAndEachSubsequentBatch() {
        List<Long> launches = new java.util.concurrent.CopyOnWriteArrayList<>();
        long start = System.nanoTime();
        ResourceCreator<String, TestResource> creator =
                new ResourceCreator<>(
                        "test",
                        1,
                        40,
                        0,
                        request -> {
                            launches.add(System.nanoTime());
                            return CompletableFuture.completedFuture(new TestResource());
                        });
        creator.create(List.of("first", "second")).join();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(launches.get(0) - start) >= 20);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(launches.get(1) - launches.get(0)) >= 20);
    }

    @Test
    void cancellationInterruptsSchedulerButStillClosesSupplierResultProducedLater() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        TestResource late = new TestResource();
        ResourceCreator<String, TestResource> creator =
                new ResourceCreator<>(
                        "test",
                        1,
                        0,
                        10,
                        request -> {
                            worker.set(Thread.currentThread());
                            entered.countDown();
                            try {
                                release.await();
                            } catch (InterruptedException ignored) {
                                interrupted.countDown();
                                try {
                                    release.await();
                                } catch (InterruptedException second) {
                                    throw new AssertionError(second);
                                }
                            }
                            return CompletableFuture.completedFuture(late);
                        });
        CompletableFuture<List<TestResource>> batch = creator.create(List.of("a"));
        CompletableFuture<Void> quiescence = ((ResourceCreationCompletion) batch).quiescence();
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        assertTrue(batch.cancel(true));
        assertTrue(interrupted.await(2, TimeUnit.SECONDS));
        assertFalse(quiescence.isDone());
        release.countDown();
        quiescence.get(2, TimeUnit.SECONDS);
        await(() -> late.closes.get() == 1 && !worker.get().isAlive());
        assertTrue(batch.isCancelled());
    }

    @Test
    void rejectsNegativeRetryCountsAndInvalidBatchSettings() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ResourceCreator<>("test", 1, 0, -1, request -> new CompletableFuture<>()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ResourceCreator<>("test", 0, 0, 0, request -> new CompletableFuture<>()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ResourceCreator<>("test", 1, -1, 0, request -> new CompletableFuture<>()));
    }

    @Test
    void emptyBatchIsAlreadyQuiescent() {
        ResourceCreator<String, TestResource> creator =
                new ResourceCreator<>(
                        "test", 1, 0, 0, ignored -> CompletableFuture.completedFuture(new TestResource()));
        CompletableFuture<List<TestResource>> result = creator.create(List.of());
        assertEquals(List.of(), result.join());
        assertTrue(((ResourceCreationCompletion) result).quiescence().isDone());
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(error);
            }
        }
        assertTrue(condition.getAsBoolean(), "condition not met before timeout");
    }

    private static final class TestResource implements AutoCloseable {
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }
}
