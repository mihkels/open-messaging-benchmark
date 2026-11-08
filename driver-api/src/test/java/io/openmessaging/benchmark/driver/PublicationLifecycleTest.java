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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PublicationLifecycleTest {

    @Test
    void closeFailsPendingSendsAndRejectsLaterOnes() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CompletableFuture<Void> send = lifecycle.begin();
        lifecycle.end();
        assertFalse(send.isDone());

        lifecycle.close();

        assertTrue(send.isCompletedExceptionally());
        CompletableFuture<Void> late = lifecycle.begin();
        assertTrue(late.isCompletedExceptionally());
        assertThrows(CompletionException.class, late::join);
    }

    @Test
    void sendSettledByTheClientIsNotFailedOnClose() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CompletableFuture<Void> send = lifecycle.begin();
        lifecycle.pending().completeFromClient(send, null);
        lifecycle.end();

        lifecycle.close();

        assertTrue(send.isDone());
        assertFalse(send.isCompletedExceptionally());
    }

    @Test
    void closeDuringAPublicationOnTheSameThreadIsRejected() {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        lifecycle.begin();
        lifecycle.begin();
        lifecycle.end();

        IllegalStateException rejected = assertThrows(IllegalStateException.class, lifecycle::close);
        assertTrue(rejected.getMessage().contains("test producer"));

        lifecycle.end();
        lifecycle.close();
    }

    @Test
    void closeWaitsForAPublicationInProgressOnAnotherThread() throws Exception {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CompletableFuture<Void> started = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        CompletableFuture<Void> publication =
                CompletableFuture.runAsync(() -> {
                    lifecycle.begin();
                    started.complete(null);
                    release.join();
                    lifecycle.end();
                });
        started.get(5, TimeUnit.SECONDS);

        CompletableFuture<Void> close = CompletableFuture.runAsync(lifecycle::close);
        assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));

        release.complete(null);
        publication.get(5, TimeUnit.SECONDS);
        close.get(5, TimeUnit.SECONDS);
    }

    @Test
    void secondCloseWaitsForTheFirstToFinish() throws Exception {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CompletableFuture<Void> started = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        CompletableFuture<Void> publication =
                CompletableFuture.runAsync(() -> {
                    lifecycle.begin();
                    started.complete(null);
                    release.join();
                    lifecycle.end();
                });
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
    void interruptWhileWaitingIsRestoredAfterClose() throws Exception {
        PublicationLifecycle lifecycle = new PublicationLifecycle("test producer");
        CompletableFuture<Void> started = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        Thread publisher =
                Thread.ofPlatform()
                        .start(() -> {
                            lifecycle.begin();
                            started.complete(null);
                            release.join();
                            lifecycle.end();
                        });
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

        assertSame(Boolean.TRUE, interruptedAfterClose.get(5, TimeUnit.SECONDS));
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
        CompletableFuture<Void> send = lifecycle.begin();
        lifecycle.end();
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
}
