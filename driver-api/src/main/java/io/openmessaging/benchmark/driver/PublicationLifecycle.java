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

/**
 * Tracks the sends of a producer whose client publishes synchronously on the caller's thread, and
 * closes it only after every publication that has started has returned.
 *
 * <p>A send calls {@link #begin()} and, when the returned future is not already done, publishes and
 * then calls {@link #end()} in a {@code finally} block. {@link #close()} waits for publications in
 * progress, fails the sends still pending, and makes concurrent closers wait for the first one.
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
     * Registers a send. If the returned future is not done, a publication has started and the caller
     * must call {@link #end()} once the client call returns.
     *
     * @return the future for the send, already failed once close has begun
     */
    public CompletableFuture<Void> begin() {
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

    /** Ends a publication started by {@link #begin()}. */
    public void end() {
        int depth = publishingDepth.get() - 1;
        if (depth == 0) {
            publishingDepth.remove();
        } else {
            publishingDepth.set(depth);
        }
        synchronized (lock) {
            activePublications--;
            lock.notifyAll();
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
