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

import org.jspecify.annotations.Nullable;

/**
 * Runs a producer's close exactly once.
 *
 * <p>The first caller owns the close and runs the close action. A close re-entered on the owner's
 * thread while the action runs, for example from a send completion callback, returns at once. Every
 * other close waits until the owner has finished and then rethrows the owner's failure, if any, so
 * a failed close keeps being reported.
 *
 * @param <E> the checked failure type of the close action
 */
public final class CloseOnce<E extends Exception> {
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
    public void close(PublicationLifecycle.ClientCloser<E> closeAction) throws E {
        if (claim()) {
            runOwner(closeAction);
            return;
        }
        awaitOwner(false);
        rethrow(ownerFailure());
    }

    /**
     * Closes once like {@link #close(PublicationLifecycle.ClientCloser)}, but a caller waiting for a
     * concurrent owner can be interrupted.
     *
     * @param closeAction closes the producer; run only by the first caller
     * @throws E the owner's close failure
     * @throws InterruptedException if interrupted while waiting for the owner; the interrupt status
     *     is restored
     */
    public void closeInterruptibly(PublicationLifecycle.ClientCloser<E> closeAction)
            throws E, InterruptedException {
        if (claim()) {
            runOwner(closeAction);
            return;
        }
        InterruptedException interruption = awaitOwner(true);
        if (interruption != null) {
            throw interruption;
        }
        rethrow(ownerFailure());
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

    private void runOwner(PublicationLifecycle.ClientCloser<E> closeAction) throws E {
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

    /**
     * Waits until the owner has finished, or returns at once on the owner's own thread. Either way an
     * interrupt while waiting restores the interrupt status: an interruptible wait stops and returns
     * the interruption, any other wait carries on and restores it once the owner finishes.
     *
     * @param interruptible whether an interrupt stops the wait
     * @return the interruption that stopped an interruptible wait, or null
     */
    // The interrupt is either restored at once with the returned interruption or after waiting
    @SuppressWarnings("java:S2142")
    private @Nullable InterruptedException awaitOwner(boolean interruptible) {
        boolean interrupted = false;
        try {
            synchronized (lock) {
                while (!complete && owner != Thread.currentThread()) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                        if (interruptible) {
                            return e;
                        }
                    }
                }
            }
            return null;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // A close re-entered on the owner's thread finds no failure, as the owner has not finished
    private @Nullable Throwable ownerFailure() {
        synchronized (lock) {
            return failure;
        }
    }

    /**
     * Rethrows a close failure unchanged. A {@code null} failure is ignored.
     *
     * @param failure a failure thrown by a close action declared to throw {@code E}, or null
     * @param <E> the checked failure type of the close action
     * @throws E the failure
     */
    // Only a close action declared to throw E can have produced a checked failure
    @SuppressWarnings("unchecked")
    static <E extends Exception> void rethrow(@Nullable Throwable failure) throws E {
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw (E) failure;
        }
    }
}
