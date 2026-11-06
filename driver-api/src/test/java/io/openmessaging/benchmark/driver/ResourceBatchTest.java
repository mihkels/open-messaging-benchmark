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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ResourceBatchTest {
    @Test
    void emptyRequestsCompleteImmediately() {
        CompletableFuture<List<Resource>> batch =
                ResourceBatch.create(
                        List.<Integer>of(),
                        ignored -> {
                            throw new AssertionError("no creation expected");
                        });

        assertEquals(List.of(), batch.join());
        assertTrue(((ResourceCreationCompletion) batch).quiescence().isDone());
    }

    @Test
    void nullResourceFailsTheBatch() {
        CompletableFuture<List<Resource>> batch =
                ResourceBatch.create(List.of(1), ignored -> CompletableFuture.completedFuture(null));

        assertInstanceOf(
                NullPointerException.class,
                assertThrows(CompletionException.class, batch::join).getCause());
    }

    @Test
    void cancellingAfterACancelledCreationReportsCancelled() {
        CompletableFuture<Resource> cancelled = new CompletableFuture<>();
        cancelled.cancel(false);
        CompletableFuture<List<Resource>> batch =
                ResourceBatch.create(
                        List.of(1, 2), index -> index == 1 ? cancelled : new CompletableFuture<>());

        assertThrows(CancellationException.class, batch::join);
        assertTrue(batch.cancel(false));
    }

    @Test
    void cancellingAfterAnOrdinaryFailureReportsNotCancelled() {
        CompletableFuture<List<Resource>> batch =
                ResourceBatch.create(
                        List.of(1), ignored -> CompletableFuture.failedFuture(new IllegalStateException()));

        assertThrows(CompletionException.class, batch::join);
        assertFalse(batch.cancel(false));
    }

    @Test
    void resourceReturnedForSeveralRequestsIsClosedOnce() {
        Resource shared = new Resource(null);
        CompletableFuture<List<Resource>> batch =
                ResourceBatch.create(
                        List.of(1, 2, 3),
                        index ->
                                index < 3
                                        ? CompletableFuture.completedFuture(shared)
                                        : CompletableFuture.failedFuture(new IllegalStateException("failed")));

        assertThrows(CompletionException.class, batch::join);
        assertEquals(1, shared.closes.get());
    }

    @Test
    void interruptedCloseRestoresInterruptFlagAndIsSuppressed() {
        InterruptedException interruption = new InterruptedException("close");
        Resource resource = new Resource(interruption);
        IllegalStateException failure = new IllegalStateException("failed");
        CompletableFuture<List<Resource>> batch =
                ResourceBatch.create(
                        List.of(1, 2),
                        index ->
                                index == 1
                                        ? CompletableFuture.completedFuture(resource)
                                        : CompletableFuture.failedFuture(failure));

        try {
            assertSame(failure, assertThrows(CompletionException.class, batch::join).getCause());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertSame(interruption, failure.getSuppressed()[0]);
        assertSame(
                interruption,
                assertThrows(
                                CompletionException.class,
                                () -> ((ResourceCreationCompletion) batch).quiescence().join())
                        .getCause());
    }

    @Test
    void closeFailingWithTheBatchCauseIsNotSuppressedIntoItself() {
        IllegalStateException failure = new IllegalStateException("broken");
        Resource resource = new Resource(failure);
        CompletableFuture<List<Resource>> batch =
                ResourceBatch.create(
                        List.of(1, 2),
                        index ->
                                index == 1
                                        ? CompletableFuture.completedFuture(resource)
                                        : CompletableFuture.failedFuture(failure));

        assertSame(failure, assertThrows(CompletionException.class, batch::join).getCause());
        assertEquals(0, failure.getSuppressed().length);
    }

    @Test
    void interruptedAbandonmentRestoresInterruptFlag() {
        InterruptedException interruption = new InterruptedException("abandon");
        Creation pending = new Creation(interruption);
        IllegalStateException failure = new IllegalStateException("failed");
        CompletableFuture<List<Resource>> batch =
                ResourceBatch.create(
                        List.of(1, 2), index -> index == 1 ? pending : CompletableFuture.failedFuture(failure));

        try {
            assertSame(failure, assertThrows(CompletionException.class, batch::join).getCause());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertEquals(1, pending.abandoned.get());
        assertSame(interruption, failure.getSuppressed()[0]);
    }

    @Test
    void creationObservedAfterCancellationIsAbandonedImmediately() {
        ResourceBatch<Resource> batch = new ResourceBatch<>(1, () -> {});
        assertTrue(batch.future().cancel(false));
        Creation late = new Creation(null);

        batch.watch(late, (value, error) -> {});
        batch.finishedStarting();

        assertEquals(1, late.abandoned.get());
        Resource resource = new Resource(null);
        late.complete(resource);
        batch.succeed(0, resource);
        assertEquals(1, resource.closes.get());
    }

    @Test
    void creationObservedAfterHandoffIsLeftAlone() {
        ResourceBatch<Resource> batch = new ResourceBatch<>(1, () -> {});
        Resource resource = new Resource(null);
        batch.succeed(0, resource);
        Creation late = new Creation(null);

        batch.observe(late);
        batch.succeed(0, new Resource(null));
        batch.finishedStarting();

        assertEquals(List.of(resource), batch.future().join());
        assertEquals(0, late.abandoned.get());
        assertEquals(0, resource.closes.get());
    }

    @Test
    void finishFailureWithoutBatchFailureIsReportedThroughQuiescence() {
        IllegalStateException finishFailure = new IllegalStateException("finish");
        ResourceBatch<Resource> batch =
                new ResourceBatch<>(
                        1,
                        () -> {
                            throw finishFailure;
                        });

        batch.succeed(0, new Resource(null));
        batch.finishedStarting();

        assertEquals(1, batch.future().join().size());
        assertSame(
                finishFailure,
                assertThrows(CompletionException.class, batch.quiescence()::join).getCause());
    }

    private static final class Resource implements AutoCloseable {
        private final AtomicInteger closes = new AtomicInteger();
        private final Exception closeFailure;

        private Resource(Exception closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() throws Exception {
            closes.incrementAndGet();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class Creation extends CompletableFuture<Resource>
            implements AbandonableCreation {
        private final AtomicInteger abandoned = new AtomicInteger();
        private final Exception abandonFailure;

        private Creation(Exception abandonFailure) {
            this.abandonFailure = abandonFailure;
        }

        @Override
        public void abandonCreation() {
            abandoned.incrementAndGet();
            if (abandonFailure != null) {
                ResourceBatchTest.<RuntimeException>sneakyThrow(abandonFailure);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable error) throws E {
        throw (E) error;
    }
}
