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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.jspecify.annotations.Nullable;

/** Tracks a producer's sends independently of the underlying client's close behavior. */
public final class PendingSendTracker {
    private static final int MIN_PRUNE_THRESHOLD = 64;
    private final String producerName;
    // Identity set: futures have no value equality
    private final Set<CompletableFuture<Void>> pending =
            Collections.newSetFromMap(new IdentityHashMap<>());
    // Per-thread depth of client callbacks; a reusable holder avoids set/remove churn per send
    private final ThreadLocal<int[]> inClientCallback = ThreadLocal.withInitial(() -> new int[1]);
    private final ThreadLocal<Boolean> closingOnThread = new ThreadLocal<>();
    // Sends failRemaining has chosen to fail but not yet completed, guarded by pending
    private final Map<CompletableFuture<Void>, Throwable> failing = new IdentityHashMap<>();
    private volatile boolean failingStarted;
    private boolean closed;
    // Guarded by pending: publications of a PublicationLifecycle whose client call has not returned
    private int activePublications;
    // Guarded by pending: settled sends are pruned once pending reaches this size
    private int pruneThreshold = MIN_PRUNE_THRESHOLD;

    /**
     * Creates a tracker for one producer.
     *
     * @param producerName the producer name used in failure messages
     */
    public PendingSendTracker(String producerName) {
        this.producerName = Objects.requireNonNull(producerName, "producerName");
    }

    /**
     * Registers a send, or returns an already failed future after close has begun.
     *
     * @return the future for the send
     */
    public CompletableFuture<Void> register() {
        return register(false);
    }

    /**
     * Registers a send and counts its publication as active until {@link #endPublication()}.
     *
     * @return the future for the send, already failed and not counted once close has begun
     */
    CompletableFuture<Void> beginTrackedPublication() {
        return register(true);
    }

    private CompletableFuture<Void> register(boolean countPublication) {
        CompletableFuture<Void> send = new CompletableFuture<>();
        synchronized (pending) {
            if (closed) {
                return CompletableFuture.failedFuture(closedFailure());
            }
            add(send);
            if (countPublication) {
                activePublications++;
            }
        }
        return send;
    }

    /**
     * Counts a publication as active until {@link #endPublication()} without registering a send.
     *
     * @return false, without counting it, once close has begun
     */
    boolean beginUntrackedPublication() {
        synchronized (pending) {
            if (closed) {
                return false;
            }
            activePublications++;
            return true;
        }
    }

    /** Ends a publication counted by one of the begin methods. */
    void endPublication() {
        synchronized (pending) {
            activePublications--;
            // Only a closer waits on activePublications, and only once close has begun
            if (closed && activePublications == 0) {
                pending.notifyAll();
            }
        }
    }

    /**
     * Begins closing like {@link #beginClose()}, then waits for the active publications to end. An
     * interrupt while waiting is deferred so the wait still completes.
     *
     * @return whether the thread was interrupted while waiting
     */
    // The caller restores the deferred interrupt once close completes
    @SuppressWarnings("java:S2142")
    boolean beginCloseAndAwaitPublications() {
        boolean interrupted = false;
        synchronized (pending) {
            beginClose();
            while (activePublications != 0) {
                try {
                    pending.wait();
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        }
        return interrupted;
    }

    // Settled sends stay in pending until the next prune, which keeps the per-send path to one lock;
    // failRemaining skips them. Doubling the threshold keeps pruning amortized O(1) per send.
    private void add(CompletableFuture<Void> send) {
        if (pending.size() >= pruneThreshold) {
            pending.removeIf(CompletableFuture::isDone);
            pruneThreshold = Math.max(MIN_PRUNE_THRESHOLD, 2 * pending.size());
        }
        pending.add(send);
    }

    /**
     * Begins closing, rejecting a close invoked from this producer's own client callback.
     *
     * @return true for the caller responsible for closing the client
     */
    public boolean beginClose() {
        synchronized (pending) {
            rejectCloseFromClientCallback();
            if (closed) {
                return false;
            }
            closed = true;
            return true;
        }
    }

    /**
     * Rejects a close invoked from this producer's own client callback.
     *
     * @throws IllegalStateException if called from a client completion callback
     */
    void rejectCloseFromClientCallback() {
        if (inClientCallback.get()[0] != 0) {
            throw new IllegalStateException(
                    "Cannot close " + producerName + " from its client completion callback");
        }
    }

    /**
     * Settles a send from a client acknowledgement without holding the tracker lock. Once {@link
     * #failRemaining(Throwable)} has chosen to fail the send, the acknowledgement settles it with
     * that close failure instead.
     *
     * @param send the registered future
     * @param failure the client failure, or null on success
     */
    public void completeFromClient(CompletableFuture<Void> send, @Nullable Throwable failure) {
        settle(send, failingStarted ? closeFailureOf(send, failure) : failure);
    }

    // pending's lock guards failing; failing is an identity map of futures
    @SuppressWarnings("UndefinedEquals")
    private @Nullable Throwable closeFailureOf(
            CompletableFuture<Void> send, @Nullable Throwable failure) {
        synchronized (pending) {
            Throwable closeFailure = failing.get(send);
            return closeFailure == null ? failure : closeFailure;
        }
    }

    private void settle(CompletableFuture<Void> send, @Nullable Throwable failure) {
        int[] depth = inClientCallback.get();
        depth[0]++;
        try {
            if (failure == null) {
                send.complete(null);
            } else {
                send.completeExceptionally(failure);
            }
        } finally {
            depth[0]--;
        }
    }

    /**
     * Runs the whole close sequence of a producer whose client can be closed directly: begins
     * closing, closes the client only for the first caller, and then fails every send the client did
     * not settle, attaching any close failure as their cause. A concurrent close does not wait for
     * the first one, and fails the sends still pending at once.
     *
     * @param closeClient closes the underlying client
     * @throws Exception the client's close failure; an interruption also restores the interrupt
     *     status
     * @throws IllegalStateException if called from a client completion callback, or re-entered on the
     *     thread that is already closing this producer
     */
    public void close(AutoCloseable closeClient) throws Exception {
        boolean closeNow = beginClose();
        if (Boolean.TRUE.equals(closingOnThread.get())) {
            throw new IllegalStateException("Cannot close " + producerName + " reentrantly");
        }
        closingOnThread.set(true);
        Throwable closeFailure = null;
        try {
            if (closeNow) {
                closeClient.close();
            }
        } catch (Exception | Error e) {
            closeFailure = e;
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        } finally {
            try {
                failRemaining(closeFailure);
            } finally {
                closingOnThread.remove();
            }
        }
    }

    /** Fails sends still pending after the client has been asked to close. */
    public void failRemaining() {
        failRemaining(null);
    }

    /**
     * Fails sends still pending after the client has been asked to close.
     *
     * @param closeFailure the client's close failure, or null if close returned normally
     */
    // failing is an identity map of futures, so removing a future by reference is well defined
    @SuppressWarnings("UndefinedEquals")
    public void failRemaining(@Nullable Throwable closeFailure) {
        List<CompletableFuture<Void>> sends;
        // Choose every outcome before completing any, as completion runs callers' callbacks and a
        // late acknowledgement must not win over a close that has already begun failing its send
        synchronized (pending) {
            pending.removeIf(CompletableFuture::isDone);
            sends = List.copyOf(pending);
            for (CompletableFuture<Void> send : sends) {
                failing.putIfAbsent(
                        send, new IllegalStateException(producerName + " is closed", closeFailure));
            }
            failingStarted = true;
        }
        for (CompletableFuture<Void> send : sends) {
            try {
                if (!send.isDone()) {
                    completeFromClient(send, null);
                }
            } finally {
                synchronized (pending) {
                    failing.remove(send);
                }
            }
        }
    }

    /**
     * Creates the failure of a send rejected because this producer is closed.
     *
     * @return a new failure naming the producer
     */
    public IllegalStateException closedFailure() {
        return new IllegalStateException(producerName + " is closed");
    }
}
