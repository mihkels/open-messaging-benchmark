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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/**
 * Tracks the sends of a producer whose client publishes synchronously on the caller's thread, and
 * closes it only after every publication that has started has returned.
 *
 * <p>A send goes through {@link #publish(Send)}, which registers it, hands the client call a
 * callback that settles it, and counts the publication as active until the client call returns.
 * {@link #close()} waits for publications in progress, fails the sends still pending, and makes
 * concurrent closers wait for the first one. A client that settles every send before its call
 * returns can use {@link #publishSync(SyncSend)} instead, which skips the pending-send tracking.
 */
public final class PublicationLifecycle {
    private final String producerName;
    private final PendingSendTracker pending;
    // Per-thread depth of publications; a reusable holder avoids set/remove churn per send
    private final ThreadLocal<int[]> publishingDepth = ThreadLocal.withInitial(() -> new int[1]);
    private final CloseOnce<RuntimeException> closer = new CloseOnce<>();

    /**
     * Creates the lifecycle of one producer.
     *
     * @param producerName the producer name used in failure messages
     */
    public PublicationLifecycle(String producerName) {
        this.producerName = producerName;
        this.pending = new PendingSendTracker(producerName);
    }

    /**
     * Publishes one send. Once close has begun the send fails without calling the client. Otherwise
     * the publication counts as active until {@code send} returns, so {@link #close()} waits for it,
     * and the future settles when the client calls the {@link Settle} callback. A failure thrown by
     * {@code send} fails the future; an {@link InterruptedException} also restores the interrupt
     * status.
     *
     * @param <E> the checked exception the client call may throw
     * @param send makes the client call and arranges for the callback to be called once the client
     *     acknowledges or rejects the send
     * @return the future for the send
     */
    // The interrupt is restored before the failure is recorded on the future
    @SuppressWarnings("java:S2142")
    public <E extends Exception> CompletableFuture<Void> publish(Send<E> send) {
        // One tracker lock both registers the send and counts the publication as active
        CompletableFuture<Void> future = pending.beginTrackedPublication();
        if (future.isDone()) {
            return future;
        }
        int[] depth = publishingDepth.get();
        depth[0]++;
        try {
            send.publish(failure -> pending.completeFromClient(future, failure));
        } catch (Exception failure) {
            future.completeExceptionally(restoringInterrupt(failure));
        } finally {
            depth[0]--;
            pending.endPublication();
        }
        return future;
    }

    /**
     * Publishes one send through a client call that settles it before returning. Once close has begun
     * the send fails without calling the client. Otherwise the publication counts as active until
     * {@code send} returns, so {@link #close()} waits for it; the send succeeds when {@code send}
     * returns and fails with what it throws. An {@link InterruptedException} also restores the
     * interrupt status.
     *
     * @param <E> the checked exception the client call may throw
     * @param send makes the client call, returning once the client has acknowledged the send
     * @return the already settled future for the send
     */
    // The interrupt is restored before the failure is recorded on the future
    @SuppressWarnings("java:S2142")
    public <E extends Exception> CompletableFuture<Void> publishSync(SyncSend<E> send) {
        if (!pending.beginUntrackedPublication()) {
            return CompletableFuture.failedFuture(pending.closedFailure());
        }
        int[] depth = publishingDepth.get();
        depth[0]++;
        try {
            send.publish();
            return CompletableFuture.completedFuture(null);
        } catch (Exception failure) {
            return CompletableFuture.failedFuture(restoringInterrupt(failure));
        } finally {
            depth[0]--;
            pending.endPublication();
        }
    }

    // A generic client call cannot be caught as InterruptedException, so restore it here
    private static Exception restoringInterrupt(Exception failure) {
        if (failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return failure;
    }

    /**
     * Returns whether close has begun.
     *
     * @return true once the first {@link #close()} has started
     */
    public boolean isClosed() {
        return closer.isClosed();
    }

    /**
     * Closes the producer: waits for publications in progress, then fails the sends still pending. A
     * concurrent close waits until the first one has finished. The interrupt status is preserved.
     *
     * @throws IllegalStateException if called from a publication on the same thread
     */
    public void close() {
        close(() -> {});
    }

    /**
     * Closes the producer like {@link #close()}, and closes its client once publications in progress
     * have returned. Only the first closer closes the client; the sends still pending are then failed
     * with the client's close failure, if any, as their cause.
     *
     * @param closeClient closes the underlying client
     * @param <E> the failure type of closing the client
     * @throws E the client's close failure
     * @throws IllegalStateException if called from a publication on the same thread
     */
    public <E extends Exception> void close(ClientCloser<E> closeClient) throws E {
        if (publishingDepth.get()[0] != 0) {
            throw new IllegalStateException(
                    "Cannot close "
                            + producerName
                            + " while publishing on the same thread; retry after sendAsync returns");
        }
        // Not publishing on this thread, and publish never reaches the counter once closed
        publishingDepth.remove();
        pending.rejectCloseFromClientCallback();
        // Only the owner sees the client's close failure; concurrent closers just wait for it
        AtomicReference<@Nullable Throwable> clientFailure = new AtomicReference<>();
        closer.close(() -> clientFailure.set(closeOwned(closeClient)));
        CloseOnce.<E>rethrow(clientFailure.get());
    }

    // An interrupt while waiting is deferred so close still completes, then restored in finally
    private <E extends Exception> @Nullable Throwable closeOwned(ClientCloser<E> closeClient) {
        boolean interrupted = pending.beginCloseAndAwaitPublications();
        Throwable closeFailure = null;
        try {
            closeClient.close();
        } catch (Exception | Error failure) {
            closeFailure = failure;
        } finally {
            try {
                pending.failRemaining(closeFailure);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return closeFailure;
    }

    /**
     * Makes the client call of one send.
     *
     * @param <E> the checked exception the client call may throw
     */
    @FunctionalInterface
    public interface Send<E extends Exception> {
        /**
         * Hands the send to the client.
         *
         * @param settle settles the send; the client calls it once it acknowledges or rejects the send,
         *     possibly before this method returns
         * @throws E if the client rejects the send synchronously
         */
        void publish(Settle settle) throws E;
    }

    /**
     * Makes the client call of one send that the client settles before the call returns.
     *
     * @param <E> the checked exception the client call may throw
     */
    @FunctionalInterface
    public interface SyncSend<E extends Exception> {
        /**
         * Hands the send to the client and returns once the client has acknowledged it.
         *
         * @throws E if the client rejects the send
         */
        void publish() throws E;
    }

    /** Settles one send from the client's acknowledgement. */
    @FunctionalInterface
    public interface Settle {
        /**
         * Completes the send, without holding any lifecycle lock.
         *
         * @param failure the client failure, or null on success
         */
        void complete(@Nullable Throwable failure);
    }

    /**
     * Closes a producer's underlying client.
     *
     * @param <E> the failure type of closing the client
     */
    @FunctionalInterface
    public interface ClientCloser<E extends Exception> {
        /**
         * Closes the client.
         *
         * @throws E if the client fails to close
         */
        void close() throws E;
    }
}
