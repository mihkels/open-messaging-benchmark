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
package io.openmessaging.benchmark.driver.natsstreaming;

import io.nats.streaming.AckHandler;
import io.nats.streaming.StreamingConnection;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PendingSendTracker;
import io.openmessaging.benchmark.driver.PublicationLifecycle;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class NatsStreamingBenchmarkProducer implements BenchmarkProducer {
    private final StreamingConnection natsStreamingPublisher;
    private final String topic;
    private final PublicationLifecycle lifecycle =
            new PublicationLifecycle("NATS Streaming producer");
    private final PendingSendTracker pending = lifecycle.pending();

    public NatsStreamingBenchmarkProducer(StreamingConnection natsStreamingPublisher, String topic) {
        this.natsStreamingPublisher = natsStreamingPublisher;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        byte[] ownedPayload;
        try {
            ownedPayload = payload.clone();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<Void> future = lifecycle.begin();
        if (future.isDone()) {
            return future;
        }
        CompletableFuture<String> expectedGuid = new CompletableFuture<>();
        AckHandler acb = ackHandler(future, expectedGuid);
        boolean wasInterrupted = Thread.currentThread().isInterrupted();
        try {
            String guid = natsStreamingPublisher.publish(topic, ownedPayload, acb);
            if (guid == null || guid.isEmpty()) {
                future.completeExceptionally(
                        new IllegalStateException("Expected non-empty guid to be returned."));
            } else {
                expectedGuid.complete(guid);
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            future.completeExceptionally(e);
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
            lifecycle.end();
        }
        return future;
    }

    private AckHandler ackHandler(
            CompletableFuture<Void> future, CompletableFuture<String> expectedGuid) {
        return (s, e) -> {
            if (e != null) {
                pending.completeFromClient(future, e);
                return;
            }
            expectedGuid.thenAccept(guid -> {
                if (guid.equals(s)) {
                    pending.completeFromClient(future, null);
                } else {
                    pending.completeFromClient(future, new IllegalStateException("guid != nuid"));
                }
            });
        };
    }

    @Override
    public void close() {
        lifecycle.close();
    }
}
