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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Tracks a producer's sends independently of the underlying client's close behavior. */
public final class PendingSendTracker {
    private final String producerName;
    private final Set<CompletableFuture<Void>> pending = new HashSet<>();
    private final ThreadLocal<Boolean> inClientCallback = new ThreadLocal<>();
    private boolean closed;

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
        CompletableFuture<Void> send = new CompletableFuture<>();
        synchronized (pending) {
            if (closed) {
                return CompletableFuture.failedFuture(closedFailure());
            }
            pending.add(send);
        }
        send.whenComplete(
                (unused, failure) -> {
                    synchronized (pending) {
                        pending.remove(send);
                    }
                });
        return send;
    }

    /**
     * Begins closing, rejecting a close invoked from this producer's own client callback.
     *
     * @return true for the caller responsible for closing the client
     */
    public boolean beginClose() {
        synchronized (pending) {
            if (Boolean.TRUE.equals(inClientCallback.get())) {
                throw new IllegalStateException(
                        "Cannot close " + producerName + " from its client completion callback");
            }
            if (closed) {
                return false;
            }
            closed = true;
            return true;
        }
    }

    /**
     * Settles a send from a client acknowledgement without holding the tracker lock.
     *
     * @param send the registered future
     * @param failure the client failure, or null on success
     */
    public void completeFromClient(CompletableFuture<Void> send, Throwable failure) {
        Boolean previous = inClientCallback.get();
        inClientCallback.set(true);
        try {
            if (failure == null) {
                send.complete(null);
            } else {
                send.completeExceptionally(failure);
            }
        } finally {
            if (previous == null) {
                inClientCallback.remove();
            } else {
                inClientCallback.set(previous);
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
    public void failRemaining(Throwable closeFailure) {
        List<CompletableFuture<Void>> sends;
        synchronized (pending) {
            sends = List.copyOf(pending);
        }
        for (CompletableFuture<Void> send : sends) {
            if (!send.isDone()) {
                completeFromClient(
                        send, new IllegalStateException(producerName + " is closed", closeFailure));
            }
        }
    }

    private IllegalStateException closedFailure() {
        return new IllegalStateException(producerName + " is closed");
    }
}
