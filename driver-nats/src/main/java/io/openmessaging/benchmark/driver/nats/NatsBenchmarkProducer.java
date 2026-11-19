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
import io.nats.client.impl.Headers;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PendingSendTracker;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class NatsBenchmarkProducer implements BenchmarkProducer {
    /**
     * Header carrying the producer's send time in epoch milliseconds; the payload stays untouched.
     */
    static final String PUBLISH_TIMESTAMP_HEADER = "omb-ts";

    private final String topic;

    private final JetStream jetStream;

    private final PendingSendTracker pending = new PendingSendTracker("NATS producer");

    public NatsBenchmarkProducer(JetStream jetStream, final String topic) {
        this.jetStream = jetStream;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        return sendAsync(key, payload, Map.of());
    }

    @Override
    public CompletableFuture<Void> sendAsync(
            Optional<String> key, byte[] payload, Map<String, byte[]> headers) {
        // Close may settle the send immediately after registration, so copy the caller's data first.
        byte[] message = payload.clone();
        CompletableFuture<Void> send = pending.register();
        if (send.isDone()) {
            return send;
        }

        // The acknowledgement callback holds only the tracker and the send, not this producer,
        // since a shared JetStream acknowledgement may never complete.
        PendingSendTracker tracker = pending;
        try {
            Headers natsHeaders = new Headers();
            // NATS header values are ASCII strings; the client rejects other bytes
            headers.forEach(
                    (name, value) -> natsHeaders.put(name, new String(value, StandardCharsets.ISO_8859_1)));
            // Written last so a workload header cannot replace the publish time
            natsHeaders.put(PUBLISH_TIMESTAMP_HEADER, Long.toString(System.currentTimeMillis()));
            jetStream
                    .publishAsync(topic, natsHeaders, message)
                    .<Void>thenApply(_ -> null)
                    .whenComplete((_, failure) -> tracker.completeFromClient(send, failure));
        } catch (RuntimeException failure) {
            send.completeExceptionally(failure);
        } catch (Error failure) {
            send.completeExceptionally(failure);
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
}
