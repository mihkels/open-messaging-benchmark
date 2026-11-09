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
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PendingSendTracker;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class NatsBenchmarkProducer implements BenchmarkProducer {
    private final String topic;

    private final JetStream jetStream;

    private final PendingSendTracker pending = new PendingSendTracker("NATS producer");

    public NatsBenchmarkProducer(JetStream jetStream, final String topic) {
        this.jetStream = jetStream;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        // Close may settle the send immediately after registration, so copy the caller's data first.
        byte[] message = payload.length < Long.BYTES ? null : payload.clone();
        CompletableFuture<Void> send = pending.register();
        if (send.isDone()) {
            return send;
        }
        if (message == null) {
            send.completeExceptionally(
                    new IllegalArgumentException("NATS payload must be at least 8 bytes"));
            return send;
        }

        // The acknowledgement callback holds only the tracker and the send, not this producer,
        // since a shared JetStream acknowledgement may never complete.
        PendingSendTracker tracker = pending;
        try {
            writeLongToBytes(System.currentTimeMillis(), message);
            jetStream
                    .publishAsync(topic, message)
                    .<Void>thenApply(_ -> null)
                    .whenComplete((_, failure) -> tracker.completeFromClient(send, failure));
        } catch (RuntimeException failure) {
            tracker.completeFromClient(send, failure);
        } catch (Error failure) {
            tracker.completeFromClient(send, failure);
            throw failure;
        }
        return send;
    }

    @Override
    public void close() {
        // Rejects a close from a completion callback; every other close settles the sends again
        pending.beginClose();
        pending.failRemaining();
    }

    public static void writeLongToBytes(long l, byte[] dst) {
        for (int i = 7; i >= 0; i--) {
            dst[i] = (byte) (l & 0xFF);
            l >>= 8;
        }
    }
}
