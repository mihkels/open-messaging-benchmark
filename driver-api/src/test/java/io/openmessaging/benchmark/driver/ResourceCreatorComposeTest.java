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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ResourceCreatorComposeTest {
    @Test
    void transfersAnOrderedAggregateOnlyAfterEveryCreationSucceeds() throws Exception {
        CompletableFuture<List<Integer>> lookup = new CompletableFuture<>();
        CompletableFuture<Resource> first = new CompletableFuture<>();
        CompletableFuture<Resource> second = new CompletableFuture<>();
        Resource one = new Resource();
        Resource two = new Resource();
        CompletableFuture<Aggregate> result =
                ResourceCreator.compose(lookup, index -> index == 0 ? first : second, Aggregate::new);

        lookup.complete(List.of(0, 1));
        second.complete(two);
        assertFalse(result.isDone());
        first.complete(one);
        Aggregate aggregate = result.join();
        assertEquals(List.of(one, two), aggregate.resources);
        assertEquals(0, one.closes.get());
        assertEquals(0, two.closes.get());
        aggregate.close();
        assertEquals(1, one.closes.get());
        assertEquals(1, two.closes.get());
    }

    @Test
    void failureClosesKnownAndLateResourcesAndPreservesCleanupFailures() {
        CompletableFuture<Resource> first = new CompletableFuture<>();
        CompletableFuture<Resource> second = new CompletableFuture<>();
        CompletableFuture<Resource> third = new CompletableFuture<>();
        RuntimeException failure = new RuntimeException("creation failed");
        RuntimeException cleanup = new RuntimeException("cleanup failed");
        Resource known = new Resource(cleanup);
        Resource late = new Resource();
        List<CompletableFuture<Resource>> attempts = List.of(first, second, third);
        CompletableFuture<Aggregate> result =
                ResourceCreator.compose(
                        CompletableFuture.completedFuture(List.of(0, 1, 2)), attempts::get, Aggregate::new);

        first.complete(known);
        second.completeExceptionally(failure);
        assertSame(failure, assertThrows(CompletionException.class, result::join).getCause());
        assertEquals(List.of(cleanup), List.of(failure.getSuppressed()));
        assertEquals(1, known.closes.get());
        assertFalse(third.isCancelled());
        third.complete(late);
        assertEquals(1, late.closes.get());
    }

    @Test
    void cancellationBeforeLookupDoesNotStartCreations() {
        CompletableFuture<List<Integer>> lookup = new CompletableFuture<>();
        AtomicInteger started = new AtomicInteger();
        CompletableFuture<Aggregate> result =
                ResourceCreator.compose(
                        lookup,
                        ignored -> {
                            started.incrementAndGet();
                            return CompletableFuture.completedFuture(new Resource());
                        },
                        Aggregate::new);
        CompletableFuture<Void> quiescence = quiescence(result);

        assertTrue(result.cancel(false));
        assertFalse(quiescence.isDone());
        lookup.complete(List.of(1));
        quiescence.join();
        assertTrue(result.isCancelled());
        assertEquals(0, started.get());
    }

    @Test
    void cancellationClosesKnownResourcesWhileAnotherCreationIsStillStarting() throws Exception {
        CompletableFuture<List<Integer>> lookup = new CompletableFuture<>();
        CooperativeCreation lateCreation = new CooperativeCreation();
        Resource known = new Resource();
        Resource late = new Resource();
        CountDownLatch starting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger started = new AtomicInteger();
        CompletableFuture<Aggregate> result =
                ResourceCreator.compose(
                        lookup,
                        index -> {
                            started.incrementAndGet();
                            if (index == 0) {
                                return CompletableFuture.completedFuture(known);
                            }
                            starting.countDown();
                            await(release);
                            return lateCreation;
                        },
                        Aggregate::new);
        CompletableFuture<Void> quiescence = quiescence(result);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> completingLookup = executor.submit(() -> lookup.complete(List.of(0, 1, 2)));
            assertTrue(starting.await(5, TimeUnit.SECONDS));
            assertTrue(result.cancel(false));
            assertEquals(1, known.closes.get());
            assertFalse(quiescence.isDone());
            release.countDown();
            completingLookup.get(5, TimeUnit.SECONDS);
            assertFalse(lateCreation.isCancelled());
            assertEquals(1, lateCreation.abandoned.get());
            assertFalse(quiescence.isDone());
            lateCreation.complete(late);
            quiescence.get(5, TimeUnit.SECONDS);
            assertEquals(1, late.closes.get());
            assertEquals(2, started.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void failingBatchRequestsCooperativeStopWithoutDiscardingLateSuccess() {
        CooperativeCreation first = new CooperativeCreation();
        CompletableFuture<Resource> second = new CompletableFuture<>();
        Resource late = new Resource();
        RuntimeException failure = new RuntimeException("creation failed");
        CompletableFuture<Aggregate> result =
                ResourceCreator.compose(
                        CompletableFuture.completedFuture(List.of(0, 1)),
                        index -> index == 0 ? first : second,
                        Aggregate::new);

        second.completeExceptionally(failure);
        assertSame(failure, assertThrows(CompletionException.class, result::join).getCause());
        assertEquals(1, first.abandoned.get());
        assertFalse(first.isCancelled());
        first.complete(late);
        assertEquals(1, late.closes.get());
    }

    @Test
    void aggregateConstructionFailureClosesEveryResource() {
        RuntimeException failure = new RuntimeException("aggregate failed");
        RuntimeException cleanup = new RuntimeException("cleanup failed");
        Resource first = new Resource(cleanup);
        Resource second = new Resource();
        CompletableFuture<Aggregate> result =
                ResourceCreator.compose(
                        CompletableFuture.completedFuture(List.of(first, second)),
                        CompletableFuture::completedFuture,
                        resources -> {
                            throw failure;
                        });

        assertSame(failure, assertThrows(CompletionException.class, result::join).getCause());
        assertEquals(List.of(cleanup), List.of(failure.getSuppressed()));
        assertEquals(1, first.closes.get());
        assertEquals(1, second.closes.get());
    }

    @Test
    void cancellationWhileAggregateIsConstructedClosesTheResult() throws Exception {
        Resource resource = new Resource();
        CompletableFuture<Resource> creation = new CompletableFuture<>();
        CountDownLatch constructing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Aggregate> result =
                ResourceCreator.compose(
                        CompletableFuture.completedFuture(List.of(1)),
                        ignored -> creation,
                        resources -> {
                            constructing.countDown();
                            await(release);
                            return new Aggregate(resources);
                        });
        CompletableFuture<Void> quiescence = quiescence(result);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> completing = executor.submit(() -> creation.complete(resource));
            assertTrue(constructing.await(5, TimeUnit.SECONDS));
            assertTrue(result.cancel(false));
            assertFalse(quiescence.isDone());
            release.countDown();
            completing.get(5, TimeUnit.SECONDS);
            quiescence.get(5, TimeUnit.SECONDS);
            assertEquals(1, resource.closes.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void nestedAbandonmentWaitsForLateCleanupAndReportsItsFailure() {
        CompletableFuture<Resource> lateCreation = new CompletableFuture<>();
        RuntimeException cleanup = new RuntimeException("late cleanup failed");
        Resource late = new Resource(cleanup);
        CompletableFuture<Aggregate> nested =
                ResourceCreator.compose(
                        CompletableFuture.completedFuture(List.of(1)), ignored -> lateCreation, Aggregate::new);
        CompletableFuture<List<Aggregate>> outer = ResourceBatch.create(List.of(1), ignored -> nested);
        CompletableFuture<Void> quiescence = quiescence(outer);

        assertTrue(outer.cancel(true));
        assertTrue(nested.isCancelled());
        assertFalse(lateCreation.isCancelled());
        assertFalse(quiescence.isDone());
        lateCreation.complete(late);

        assertSame(cleanup, assertThrows(CompletionException.class, quiescence::join).getCause());
        assertEquals(1, late.closes.get());
    }

    @Test
    void lateAggregateCleanupFailureIsReportedAfterCancellation() throws Exception {
        RuntimeException cleanup = new RuntimeException("aggregate cleanup failed");
        CompletableFuture<Resource> creation = new CompletableFuture<>();
        Resource resource = new Resource(cleanup);
        CountDownLatch constructing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Aggregate> result =
                ResourceCreator.compose(
                        CompletableFuture.completedFuture(List.of(1)),
                        ignored -> creation,
                        resources -> {
                            constructing.countDown();
                            await(release);
                            return new Aggregate(resources);
                        });
        CompletableFuture<Void> quiescence = quiescence(result);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> completing = executor.submit(() -> creation.complete(resource));
            assertTrue(constructing.await(5, TimeUnit.SECONDS));
            assertTrue(result.cancel(false));
            assertFalse(quiescence.isDone());
            release.countDown();
            completing.get(5, TimeUnit.SECONDS);
            assertSame(cleanup, assertThrows(CompletionException.class, quiescence::join).getCause());
            assertEquals(1, resource.closes.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void quiescenceWaitsForCancellationCleanupEvenWhenLastCallbackFinishes() throws Exception {
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AutoCloseable blocking =
                () -> {
                    closing.countDown();
                    await(release);
                };
        CompletableFuture<AutoCloseable> later = new CompletableFuture<>();
        CompletableFuture<List<AutoCloseable>> batch =
                ResourceBatch.create(
                        List.of(0, 1),
                        index -> index == 0 ? CompletableFuture.completedFuture(blocking) : later);
        CompletableFuture<Void> quiescence = quiescence(batch);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> cancelling = executor.submit(() -> batch.cancel(true));
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            later.complete(() -> {});
            assertFalse(quiescence.isDone());
            release.countDown();
            assertTrue(cancelling.get(5, TimeUnit.SECONDS));
            quiescence.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    private static CompletableFuture<Void> quiescence(CompletableFuture<?> future) {
        return ((ResourceCreationCompletion) future).quiescence();
    }

    @Test
    void cancellationAndSuccessfulHandoffHaveExactlyOneOwner() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int iteration = 0; iteration < 100; iteration++) {
                Resource resource = new Resource();
                CompletableFuture<Resource> creation = new CompletableFuture<>();
                CompletableFuture<Aggregate> result =
                        ResourceCreator.compose(
                                CompletableFuture.completedFuture(List.of(1)), ignored -> creation, Aggregate::new);
                CountDownLatch start = new CountDownLatch(1);
                Future<?> complete =
                        executor.submit(
                                () -> {
                                    await(start);
                                    creation.complete(resource);
                                });
                Future<Boolean> cancel =
                        executor.submit(
                                () -> {
                                    await(start);
                                    return result.cancel(false);
                                });
                start.countDown();
                complete.get(5, TimeUnit.SECONDS);
                if (!cancel.get(5, TimeUnit.SECONDS)) {
                    result.join().close();
                }
                assertEquals(1, resource.closes.get());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for test coordination");
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new AssertionError(error);
        }
    }

    private static final class Resource implements AutoCloseable {
        private final AtomicInteger closes = new AtomicInteger();
        private final RuntimeException closeFailure;

        private Resource() {
            this(null);
        }

        private Resource(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closes.incrementAndGet();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class CooperativeCreation extends CompletableFuture<Resource>
            implements AbandonableCreation {
        private final AtomicInteger abandoned = new AtomicInteger();

        @Override
        public void abandonCreation() {
            abandoned.incrementAndGet();
        }
    }

    private static final class Aggregate implements AutoCloseable {
        private final List<Resource> resources;

        private Aggregate(List<Resource> resources) {
            this.resources = new ArrayList<>(resources);
        }

        @Override
        public void close() throws Exception {
            for (Resource resource : resources) {
                resource.close();
            }
        }
    }
}
