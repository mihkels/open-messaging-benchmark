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
import org.jspecify.annotations.Nullable;

/**
 * Tracks the sends of a producer whose client publishes synchronously on the caller's thread, and
 * closes it only after every publication that has started has returned.
 *
 * <p>A send goes through {@link #publish(Send)}, which registers it, hands the client call a
 * callback that settles it, and counts the publication as active until the client call returns.
 * {@link #close()} waits for publications in progress, fails the sends still pending, and makes
 * concurrent closers wait for the first one.
 */
public final class PublicationLifecycle {
    private final String producerName;
    private final PendingSendTracker pending;
    private final Object lock = new Object();
    private final ThreadLocal<Integer> publishingDepth = new ThreadLocal<>();
    private volatile boolean closed;
    private boolean closeComplete;
    private int activePublications;

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
     * Returns the tracker that settles this producer's sends.
     *
     * @return the pending send tracker
     */
    public PendingSendTracker pending() {
        return pending;
    }

    /**
     * Publishes one send. Once close has begun the send fails without calling the client. Otherwise
     * the publication counts as active until {@code send} returns, so {@link #close()} waits for it,
     * and the future settles when the client calls the {@link Settle} callback. A failure thrown by
     * {@code send} fails the future; an {@link InterruptedException} also restores the interrupt
     * status.
     *
     * @param send makes the client call and arranges for the callback to be called once the client
     *     acknowledges or rejects the send
     * @return the future for the send
     */
    // The interrupt is restored before the failure is recorded on the future
    @SuppressWarnings("java:S2142")
    public CompletableFuture<Void> publish(Send send) {
        CompletableFuture<Void> future = begin();
        if (future.isDone()) {
            return future;
        }
        try {
            send.publish(failure -> pending.completeFromClient(future, failure));
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            future.completeExceptionally(failure);
        } finally {
            end();
        }
        return future;
    }

    private CompletableFuture<Void> begin() {
        synchronized (lock) {
            CompletableFuture<Void> future = pending.register();
            if (future.isDone()) {
                return future;
            }
            activePublications++;
            Integer depth = publishingDepth.get();
            publishingDepth.set(depth == null ? 1 : depth + 1);
            return future;
        }
    }

    private void end() {
        int depth = publishingDepth.get() - 1;
        if (depth == 0) {
            publishingDepth.remove();
        } else {
            publishingDepth.set(depth);
        }
        synchronized (lock) {
            activePublications--;
            // Only the close owner waits on activePublications, and only once closed is set
            if (closed && activePublications == 0) {
                lock.notifyAll();
            }
        }
    }

    /**
     * Returns whether close has begun.
     *
     * @return true once the first {@link #close()} has started
     */
    public boolean isClosed() {
        return closed;
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
    // An interrupt while waiting is deferred so close still completes, then restored in finally
    @SuppressWarnings("java:S2142")
    public <E extends Exception> void close(ClientCloser<E> closeClient) throws E {
        if (publishingDepth.get() != null) {
            throw new IllegalStateException(
                    "Cannot close "
                            + producerName
                            + " while publishing on the same thread; retry after sendAsync returns");
        }
        publishingDepth.remove();
        boolean interrupted = false;
        boolean closeOwner;
        synchronized (lock) {
            closeOwner = pending.beginClose();
            if (closeOwner) {
                closed = true;
            }
            while (closeOwner ? activePublications != 0 : !closeComplete) {
                try {
                    lock.wait();
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        }
        Throwable closeFailure = null;
        try {
            if (closeOwner) {
                closeClient.close();
            }
        } catch (Exception | Error failure) {
            closeFailure = failure;
            throw failure;
        } finally {
            try {
                if (closeOwner) {
                    pending.failRemaining(closeFailure);
                }
            } finally {
                if (closeOwner) {
                    synchronized (lock) {
                        closeComplete = true;
                        lock.notifyAll();
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Runs a producer's close exactly once for producers whose sends are not synchronous
     * publications.
     *
     * <p>The first caller owns the close and runs the close action. A close re-entered on the owner's
     * thread while the action runs, for example from a send completion callback, returns at once.
     * Every other close waits until the owner has finished and then rethrows the owner's failure, if
     * any, so a failed close keeps being reported.
     *
     * @param <E> the checked failure type of the close action
     */
    public static final class OwnedClose<E extends Exception> {
        private final Object lock = new Object();
        private volatile boolean closed;
        private @Nullable Thread owner;
        private boolean complete;
        private @Nullable Throwable failure;

        /**
         * Returns whether close has begun.
         *
         * @return true once the first close has started
         */
        public boolean isClosed() {
            return closed;
        }

        /**
         * Closes once, waiting for a concurrent owner without being interruptible. An interrupt while
         * waiting is deferred and restored before returning.
         *
         * @param closeAction closes the producer; run only by the first caller
         * @throws E the owner's close failure
         */
        // An interrupt while waiting is deferred so close still completes, then restored
        @SuppressWarnings("java:S2142")
        public void close(ClientCloser<E> closeAction) throws E {
            if (claim()) {
                runOwner(closeAction);
                return;
            }
            boolean interrupted = false;
            synchronized (lock) {
                if (owner == Thread.currentThread()) {
                    return;
                }
                while (!complete) {
                    try {
                        lock.wait();
                    } catch (InterruptedException _) {
                        interrupted = true;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            rethrowFailure();
        }

        /**
         * Closes once like {@link #close(ClientCloser)}, but a caller waiting for a concurrent owner
         * can be interrupted.
         *
         * @param closeAction closes the producer; run only by the first caller
         * @throws E the owner's close failure
         * @throws InterruptedException if interrupted while waiting for the owner; the interrupt status
         *     is restored
         */
        public void closeInterruptibly(ClientCloser<E> closeAction) throws E, InterruptedException {
            if (claim()) {
                runOwner(closeAction);
                return;
            }
            synchronized (lock) {
                if (owner == Thread.currentThread()) {
                    return;
                }
                while (!complete) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }
            }
            rethrowFailure();
        }

        private boolean claim() {
            synchronized (lock) {
                if (closed) {
                    return false;
                }
                closed = true;
                owner = Thread.currentThread();
                return true;
            }
        }

        private void runOwner(ClientCloser<E> closeAction) throws E {
            Throwable closeFailure = null;
            try {
                closeAction.close();
            } catch (Exception | Error e) {
                closeFailure = e;
                throw e;
            } finally {
                synchronized (lock) {
                    failure = closeFailure;
                    owner = null;
                    complete = true;
                    lock.notifyAll();
                }
            }
        }

        // Only the owner's action can have stored a checked failure, and it throws E
        @SuppressWarnings("unchecked")
        private void rethrowFailure() throws E {
            Throwable closeFailure;
            synchronized (lock) {
                closeFailure = failure;
            }
            if (closeFailure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (closeFailure instanceof Error error) {
                throw error;
            }
            if (closeFailure != null) {
                throw (E) closeFailure;
            }
        }
    }

    /** Makes the client call of one send. */
    @FunctionalInterface
    public interface Send {
        /**
         * Hands the send to the client.
         *
         * @param settle settles the send; the client calls it once it acknowledges or rejects the send,
         *     possibly before this method returns
         * @throws Exception if the client rejects the send synchronously
         */
        void publish(Settle settle) throws Exception;
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
