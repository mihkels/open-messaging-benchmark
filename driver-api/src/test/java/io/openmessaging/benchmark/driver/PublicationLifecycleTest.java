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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
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

class PublicationLifecycleTest {

    @Test
    void closeFailsPendingSendsAndRejectsLaterOnes() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        AtomicInteger clientCalls = new AtomicInteger();
        CompletableFuture<Void> send = lifecycle.publish(_ -> clientCalls.incrementAndGet());
        assertFalse(send.isDone());

        lifecycle.close();

        assertTrue(send.isCompletedExceptionally());
        CompletableFuture<Void> late = lifecycle.publish(_ -> clientCalls.incrementAndGet());
        assertTrue(late.isCompletedExceptionally());
        assertThrows(CompletionException.class, late::join);
        assertEquals(1, clientCalls.get());
    }

    @Test
    void sendSettledByTheClientIsNotFailedOnClose() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CompletableFuture<Void> send = lifecycle.publish(settle -> settle.complete(null));

        lifecycle.close();

        assertTrue(send.isDone());
        assertFalse(send.isCompletedExceptionally());
    }

    @Test
    void clientRejectionCompletesTheSendExceptionally() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        Exception rejection = new Exception("send rejected");
        RuntimeException failure = new RuntimeException("acknowledgement failed");

        CompletableFuture<Void> rejected =
                lifecycle.publish(_ -> {
                    throw rejection;
                });
        CompletableFuture<Void> failed = lifecycle.publish(settle -> settle.complete(failure));

        assertSame(rejection, assertThrows(CompletionException.class, rejected::join).getCause());
        assertSame(failure, assertThrows(CompletionException.class, failed::join).getCause());
        assertFalse(Thread.currentThread().isInterrupted());
        lifecycle.close();
    }

    @Test
    void interruptedClientCallFailsTheSendAndRestoresTheInterrupt() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        InterruptedException interrupted = new InterruptedException("interrupted");
        try {
            CompletableFuture<Void> send =
                    lifecycle.publish(_ -> {
                        throw interrupted;
                    });

            assertTrue(Thread.currentThread().isInterrupted());
            assertSame(interrupted, assertThrows(CompletionException.class, send::join).getCause());
        } finally {
            Thread.interrupted();
        }
        lifecycle.close();
    }

    @Test
    void closeDuringAPublicationOnTheSameThreadIsRejected() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        AtomicReference<IllegalStateException> rejected = new AtomicReference<>();
        lifecycle.publish(_ ->
                lifecycle.publish(
                        _ -> rejected.set(assertThrows(IllegalStateException.class, lifecycle::close))));

        assertTrue(rejected.get().getMessage().contains("test producer"));
        assertFalse(lifecycle.isClosed());
        lifecycle.close();
        assertTrue(lifecycle.isClosed());
    }

    @Test
    void reentrantCloseIsRejectedUntilNestedPublicationsReturn() throws Exception {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        FakeClient client = new FakeClient();
        CompletableFuture<Void> first = lifecycle.publish(client::accept);
        AtomicReference<CompletableFuture<Void>> nested = new AtomicReference<>();
        CompletableFuture<Void> closeAttempt =
                first.thenRun(() -> {
                    nested.set(lifecycle.publish(client::accept));
                    lifecycle.close();
                });
        ExecutorService sender = Executors.newSingleThreadExecutor();
        try {
            // The second publication acknowledges the first while it is still publishing
            Future<CompletableFuture<Void>> second =
                    sender.submit(() ->
                            lifecycle.publish(settle -> {
                                client.accept(settle);
                                client.settle(0, null);
                            }));
            CompletableFuture<Void> secondSend = second.get(5, TimeUnit.SECONDS);

            CompletionException rejected = assertThrows(CompletionException.class, closeAttempt::join);
            assertInstanceOf(IllegalStateException.class, rejected.getCause());
            assertTrue(rejected.getCause().getMessage().contains("retry after sendAsync returns"));
            assertEquals(3, client.calls());
            assertNull(first.join());
            assertFalse(secondSend.isDone());
            assertFalse(nested.get().isDone());

            lifecycle.close();

            assertFailedOnClose(secondSend);
            assertFailedOnClose(nested.get());
        } finally {
            sender.shutdownNow();
        }
    }

    @Test
    void asynchronousAcknowledgementCannotCloseWhileAnotherPublicationIsActive() throws Exception {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        FakeClient client = new FakeClient();
        CompletableFuture<Void> first = lifecycle.publish(client::accept);
        AtomicReference<Throwable> rejected = new AtomicReference<>();
        first.thenRun(() -> {
            try {
                lifecycle.close();
            } catch (RuntimeException failure) {
                rejected.set(failure);
            }
        });
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<CompletableFuture<Void>> second =
                    workers.submit(() ->
                            lifecycle.publish(settle -> {
                                client.accept(settle);
                                publishing.countDown();
                                await(release);
                            }));
            assertTrue(publishing.await(5, TimeUnit.SECONDS));

            workers.submit(() -> client.settle(0, null)).get(5, TimeUnit.SECONDS);

            assertInstanceOf(IllegalStateException.class, rejected.get());
            assertTrue(rejected.get().getMessage().contains("client completion callback"));
            release.countDown();
            CompletableFuture<Void> secondSend = second.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> third = lifecycle.publish(client::accept);
            assertFalse(third.isDone());

            lifecycle.close();

            assertFailedOnClose(secondSend);
            assertFailedOnClose(third);
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void failedAcknowledgementAlsoGuardsReentrantClose() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        FakeClient client = new FakeClient();
        CompletableFuture<Void> send = lifecycle.publish(client::accept);
        CompletableFuture<Throwable> rejected =
                send.handle((_, _) -> {
                    try {
                        lifecycle.close();
                        return null;
                    } catch (RuntimeException failure) {
                        return failure;
                    }
                });
        RuntimeException failure = new RuntimeException("acknowledgement failed");

        client.settle(0, failure);

        assertInstanceOf(IllegalStateException.class, rejected.join());
        assertSame(failure, assertThrows(CompletionException.class, send::join).getCause());
        CompletableFuture<Void> next = lifecycle.publish(client::accept);
        assertFalse(next.isDone());
        lifecycle.close();
        assertFailedOnClose(next);
    }

    @Test
    void closeWaitsForAPublicationInProgressOnAnotherThread() throws Exception {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CompletableFuture<Void> started = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        CompletableFuture<CompletableFuture<Void>> publication =
                CompletableFuture.supplyAsync(() ->
                        lifecycle.publish(_ -> {
                            started.complete(null);
                            release.join();
                        }));
        started.get(5, TimeUnit.SECONDS);

        CompletableFuture<Void> close = CompletableFuture.runAsync(lifecycle::close);
        assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));

        release.complete(null);
        CompletableFuture<Void> send = publication.get(5, TimeUnit.SECONDS);
        close.get(5, TimeUnit.SECONDS);
        assertFailedOnClose(send);
    }

    @Test
    void secondCloseWaitsForTheFirstToFinish() throws Exception {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CompletableFuture<Void> started = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        CompletableFuture<CompletableFuture<Void>> publication =
                CompletableFuture.supplyAsync(() ->
                        lifecycle.publish(_ -> {
                            started.complete(null);
                            release.join();
                        }));
        started.get(5, TimeUnit.SECONDS);
        CompletableFuture<Void> first = CompletableFuture.runAsync(lifecycle::close);
        CompletableFuture<Void> second = CompletableFuture.runAsync(lifecycle::close);
        assertThrows(TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));

        release.complete(null);
        publication.get(5, TimeUnit.SECONDS);
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
    }

    @Test
    void interruptedCloseStillWaitsForTheActivePublicationAndRestoresTheInterrupt() throws Exception {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<CompletableFuture<Void>> send =
                    workers.submit(() ->
                            lifecycle.publish(_ -> {
                                publishing.countDown();
                                await(release);
                            }));
            assertTrue(publishing.await(5, TimeUnit.SECONDS));
            Future<Boolean> close =
                    workers.submit(() -> {
                        Thread.currentThread().interrupt();
                        closing.countDown();
                        lifecycle.close();
                        return Thread.interrupted();
                    });
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> close.get(150, TimeUnit.MILLISECONDS));

            release.countDown();

            assertFailedOnClose(send.get(5, TimeUnit.SECONDS));
            assertTrue(close.get(5, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void interruptWhileWaitingIsRestoredAfterClose() throws Exception {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CompletableFuture<Void> started = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        Thread publisher =
                Thread.ofPlatform()
                        .start(() ->
                                lifecycle.publish(_ -> {
                                    started.complete(null);
                                    release.join();
                                }));
        started.get(5, TimeUnit.SECONDS);
        CompletableFuture<Boolean> interruptedAfterClose = new CompletableFuture<>();
        Thread closer =
                Thread.ofPlatform()
                        .start(() -> {
                            lifecycle.close();
                            interruptedAfterClose.complete(Thread.currentThread().isInterrupted());
                        });

        closer.interrupt();
        release.complete(null);
        publisher.join(5000);
        closer.join(5000);

        assertSame(true, interruptedAfterClose.get(5, TimeUnit.SECONDS));
    }

    @Test
    void onlyTheFirstCloseClosesTheClient() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        AtomicInteger clientCloses = new AtomicInteger();
        assertFalse(lifecycle.isClosed());

        lifecycle.close(clientCloses::incrementAndGet);
        lifecycle.close(clientCloses::incrementAndGet);

        assertTrue(lifecycle.isClosed());
        assertEquals(1, clientCloses.get());
    }

    @Test
    void clientCloseFailureIsThrownAndBecomesThePendingSendsCause() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CompletableFuture<Void> send = lifecycle.publish(_ -> {});
        Exception closeFailure = new Exception("close failed");

        Exception thrown =
                assertThrows(
                        Exception.class,
                        () ->
                                lifecycle.close(() -> {
                                    throw closeFailure;
                                }));

        assertSame(closeFailure, thrown);
        CompletionException failure = assertThrows(CompletionException.class, send::join);
        assertEquals("test producer is closed", failure.getCause().getMessage());
        assertSame(closeFailure, failure.getCause().getCause());
    }

    private static void assertFailedOnClose(CompletableFuture<Void> send) {
        CompletionException failure = assertThrows(CompletionException.class, send::join);
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        assertEquals("test producer is closed", failure.getCause().getMessage());
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("publication was not released");
        }
    }

    /** A client that records each send's settle callback so a test can acknowledge it later. */
    private static final class FakeClient {
        private final List<PublicationLifecycle.Settle> settles = new CopyOnWriteArrayList<>();

        void accept(PublicationLifecycle.Settle settle) {
            settles.add(settle);
        }

        void settle(int send, @Nullable Throwable failure) {
            settles.get(send).complete(failure);
        }

        int calls() {
            return settles.size();
        }
    }
}
