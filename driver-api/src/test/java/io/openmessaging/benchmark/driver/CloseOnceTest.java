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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

class CloseOnceTest {

    @Test
    void runsTheActionOnceAndReportsItsFailureToEveryCaller() {
        CloseOnce<Exception> close = new CloseOnce<>();
        AtomicInteger runs = new AtomicInteger();
        Exception failure = new Exception("close failed");
        assertFalse(close.isClosed());

        PublicationLifecycle.ClientCloser<Exception> action =
                () -> {
                    runs.incrementAndGet();
                    throw failure;
                };
        assertSame(failure, assertThrows(Exception.class, () -> close.close(action)));
        assertSame(failure, assertThrows(Exception.class, () -> close.close(action)));
        assertSame(failure, assertThrows(Exception.class, () -> close.closeInterruptibly(action)));

        assertTrue(close.isClosed());
        assertEquals(1, runs.get());
    }

    @Test
    void rethrowsAnUncheckedOwnerFailureUnchanged() {
        CloseOnce<RuntimeException> close = new CloseOnce<>();
        AssertionError failure = new AssertionError("close failed");

        assertSame(
                failure,
                assertThrows(
                        AssertionError.class,
                        () ->
                                close.close(() -> {
                                    throw failure;
                                })));
        assertSame(failure, assertThrows(AssertionError.class, () -> close.close(() -> {})));
    }

    @Test
    void closeReenteredOnTheOwnerThreadReturnsAtOnce() throws Exception {
        CloseOnce<Exception> close = new CloseOnce<>();
        AtomicInteger runs = new AtomicInteger();
        AtomicReference<@Nullable Exception> reentrant = new AtomicReference<>();
        PublicationLifecycle.ClientCloser<Exception> action = runs::incrementAndGet;

        close.close(() -> {
            runs.incrementAndGet();
            try {
                close.close(action);
                close.closeInterruptibly(action);
            } catch (Exception e) {
                reentrant.set(e);
            }
        });

        assertNull(reentrant.get());
        assertEquals(1, runs.get());
    }

    @Test
    void makesConcurrentCallersWaitForTheOwner() throws Exception {
        CloseOnce<RuntimeException> close = new CloseOnce<>();
        CountDownLatch closing = new CountDownLatch(1);
        CompletableFuture<Void> release = new CompletableFuture<>();
        RuntimeException failure = new RuntimeException("close failed");
        ExecutorService closers = Executors.newFixedThreadPool(3);
        try {
            Future<?> owner =
                    closers.submit(() ->
                            close.close(() -> {
                                closing.countDown();
                                release.join();
                                throw failure;
                            }));
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            Future<?> waiter = closers.submit(() -> close.close(() -> {}));
            Future<?> interruptibleWaiter =
                    closers.submit(() -> {
                        close.closeInterruptibly(() -> {});
                        return null;
                    });
            assertThrows(TimeoutException.class, () -> waiter.get(100, TimeUnit.MILLISECONDS));
            assertFalse(interruptibleWaiter.isDone());

            release.complete(null);

            for (Future<?> closer : List.of(owner, waiter, interruptibleWaiter)) {
                Exception thrown = assertThrows(Exception.class, () -> closer.get(5, TimeUnit.SECONDS));
                assertSame(failure, thrown.getCause());
            }
        } finally {
            release.complete(null);
            closers.shutdownNow();
        }
    }

    @Test
    void interruptedWaiterIsDeferredUnlessInterruptible() throws Exception {
        CloseOnce<RuntimeException> close = new CloseOnce<>();
        CountDownLatch closing = new CountDownLatch(1);
        CompletableFuture<Void> release = new CompletableFuture<>();
        ExecutorService closers = Executors.newFixedThreadPool(3);
        try {
            closers.submit(() ->
                    close.close(() -> {
                        closing.countDown();
                        release.join();
                    }));
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            Future<Boolean> interruptible =
                    closers.submit(() -> {
                        Thread.currentThread().interrupt();
                        assertThrows(InterruptedException.class, () -> close.closeInterruptibly(() -> {}));
                        return Thread.interrupted();
                    });
            assertTrue(interruptible.get(5, TimeUnit.SECONDS));
            Future<Boolean> deferred =
                    closers.submit(() -> {
                        Thread.currentThread().interrupt();
                        close.close(() -> {});
                        return Thread.interrupted();
                    });
            assertThrows(TimeoutException.class, () -> deferred.get(100, TimeUnit.MILLISECONDS));

            release.complete(null);

            assertTrue(deferred.get(5, TimeUnit.SECONDS));
        } finally {
            release.complete(null);
            closers.shutdownNow();
        }
    }
}
