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
package io.openmessaging.benchmark.driver.nats;

import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class NatsBenchmarkProducer implements BenchmarkProducer {
    private final String topic;

    private final JetStream jetStream;

    private final Object lifecycleLock = new Object();
    private final Set<PendingSend> pendingSends = new HashSet<>();
    private final ThreadLocal<Boolean> completingOnThread = new ThreadLocal<>();
    private volatile boolean closed;

    public NatsBenchmarkProducer(JetStream jetStream, final String topic) {
        this.jetStream = jetStream;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        if (closed) {
            return closedFuture();
        }
        if (payload.length < Long.BYTES) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("NATS payload must be at least 8 bytes"));
        }

        // Close may settle the send immediately after registration, so copy the caller's data first.
        byte[] message = payload.clone();
        PendingSend send = new PendingSend(this);
        send.future.whenComplete((ignored, failure) -> removePending(send));
        synchronized (lifecycleLock) {
            if (closed) {
                return closedFuture();
            }
            pendingSends.add(send);
        }

        try {
            if (send.outcomeSelected) {
                complete(send);
                return send.future;
            }
            writeLongToBytes(System.currentTimeMillis(), message);
            CompletableFuture<PublishAck> acknowledgement = jetStream.publishAsync(topic, message);
            if (send.outcomeSelected) {
                complete(send);
            } else {
                acknowledgement.thenApply(ack -> (Void) null).whenComplete(send);
            }
        } catch (RuntimeException failure) {
            settle(send, failure);
        } catch (Error failure) {
            settle(send, failure);
            throw failure;
        }
        return send.future;
    }

    @Override
    public void close() {
        if (Boolean.TRUE.equals(completingOnThread.get())) {
            throw new IllegalStateException("Cannot close NATS producer from its completion callback");
        }
        List<PendingSend> sends;
        IllegalStateException failure = new IllegalStateException("NATS producer is closed");
        synchronized (lifecycleLock) {
            closed = true;
            sends = new ArrayList<>(pendingSends);
            for (PendingSend send : sends) {
                if (!send.outcomeSelected) {
                    send.failure = failure;
                    send.outcomeSelected = true;
                }
            }
        }
        // Complete outside the lock because completion can run caller callbacks.
        sends.forEach(this::complete);
    }

    private void settle(PendingSend send, Throwable failure) {
        synchronized (lifecycleLock) {
            if (!send.outcomeSelected) {
                send.failure = failure;
                send.outcomeSelected = true;
            }
        }
        complete(send);
    }

    private void removePending(PendingSend send) {
        synchronized (lifecycleLock) {
            pendingSends.remove(send);
            // A shared JetStream acknowledgement may never complete.
            send.owner = null;
        }
    }

    private void complete(PendingSend send) {
        Boolean previous = completingOnThread.get();
        completingOnThread.set(true);
        try {
            if (send.failure == null) {
                send.future.complete(null);
            } else {
                send.future.completeExceptionally(send.failure);
            }
        } finally {
            if (previous == null) {
                completingOnThread.remove();
            } else {
                completingOnThread.set(previous);
            }
        }
    }

    private static CompletableFuture<Void> closedFuture() {
        return CompletableFuture.failedFuture(new IllegalStateException("NATS producer is closed"));
    }

    public static void writeLongToBytes(long l, byte[] dst) {
        for (int i = 7; i >= 0; i--) {
            dst[i] = (byte) (l & 0xFF);
            l >>= 8;
        }
    }

    private static final class PendingSend implements BiConsumer<Void, Throwable> {
        private final CompletableFuture<Void> future = new CompletableFuture<>();
        private volatile NatsBenchmarkProducer owner;
        private volatile boolean outcomeSelected;
        private Throwable failure;

        private PendingSend(NatsBenchmarkProducer owner) {
            this.owner = owner;
        }

        @Override
        public void accept(Void ignored, Throwable failure) {
            NatsBenchmarkProducer producer = owner;
            if (producer != null) {
                producer.settle(this, failure);
            }
        }
    }
}
