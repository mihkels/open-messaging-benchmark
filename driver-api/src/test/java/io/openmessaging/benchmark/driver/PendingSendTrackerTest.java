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
import org.junit.jupiter.api.Test;

class PendingSendTrackerTest {
    @Test
    void preservesAcknowledgementsDuringCloseAndFailsRemainingSends() {
        PendingSendTracker tracker = new PendingSendTracker("test producer");
        CompletableFuture<Void> acknowledged = tracker.register();
        CompletableFuture<Void> unresolved = tracker.register();

        assertTrue(tracker.beginClose());
        tracker.completeFromClient(acknowledged, null);
        tracker.failRemaining();

        acknowledged.join();
        assertThrows(CompletionException.class, unresolved::join);
        CompletableFuture<Void> afterClose = tracker.register();
        assertThrows(CompletionException.class, afterClose::join);
        assertFalse(tracker.beginClose());
    }

    @Test
    void closesTheClientOnceAndFailsUnsettledSends() throws Exception {
        PendingSendTracker tracker = new PendingSendTracker("test producer");
        CompletableFuture<Void> unresolved = tracker.register();
        int[] closes = new int[1];

        tracker.close(() -> closes[0]++);
        tracker.close(() -> closes[0]++);

        assertEquals(1, closes[0]);
        assertThrows(CompletionException.class, unresolved::join);
    }

    @Test
    void attachesTheCloseFailureAndRestoresAnInterrupt() {
        PendingSendTracker tracker = new PendingSendTracker("test producer");
        CompletableFuture<Void> unresolved = tracker.register();
        InterruptedException interrupted = new InterruptedException("close interrupted");

        try {
            assertSame(
                    interrupted,
                    assertThrows(
                            InterruptedException.class,
                            () ->
                                    tracker.close(() -> {
                                        throw interrupted;
                                    })));
            assertTrue(Thread.currentThread().isInterrupted());
            CompletionException failure = assertThrows(CompletionException.class, unresolved::join);
            assertSame(interrupted, failure.getCause().getCause());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void rejectsCloseFromClientCallbackBeforeChangingState() {
        PendingSendTracker tracker = new PendingSendTracker("test producer");
        CompletableFuture<Void> send = tracker.register();
        CompletableFuture<Void> callback =
                send.thenRun(() -> assertThrows(IllegalStateException.class, tracker::beginClose));

        tracker.completeFromClient(send, null);
        callback.join();

        assertTrue(tracker.beginClose());
        tracker.failRemaining();
    }

    @Test
    void cancelledSendDoesNotChangeOtherSends() {
        PendingSendTracker tracker = new PendingSendTracker("test producer");
        CompletableFuture<Void> cancelled = tracker.register();
        CompletableFuture<Void> pending = tracker.register();

        assertTrue(cancelled.cancel(false));
        tracker.completeFromClient(cancelled, null);
        assertTrue(tracker.beginClose());
        tracker.failRemaining();

        assertTrue(cancelled.isCancelled());
        assertThrows(CompletionException.class, pending::join);
    }

    @Test
    void preservesCloseFailureAsThePendingSendCause() {
        PendingSendTracker tracker = new PendingSendTracker("test producer");
        CompletableFuture<Void> send = tracker.register();
        IllegalStateException failure = new IllegalStateException("client close failed");

        assertTrue(tracker.beginClose());
        tracker.failRemaining(failure);

        CompletionException completion = assertThrows(CompletionException.class, send::join);
        assertSame(failure, completion.getCause().getCause());
    }

    @Test
    void rejectsReentrantCloseFromFailureCallbacks() {
        PendingSendTracker tracker = new PendingSendTracker("test producer");
        CompletableFuture<Void> first = tracker.register();
        CompletableFuture<Void> second = tracker.register();
        CompletableFuture<Void> callback =
                first.handle((_, failure) -> {
                    assertThrows(IllegalStateException.class, tracker::beginClose);
                    return null;
                });

        assertTrue(tracker.beginClose());
        tracker.failRemaining();

        callback.join();
        assertThrows(CompletionException.class, first::join);
        assertThrows(CompletionException.class, second::join);
    }
}
