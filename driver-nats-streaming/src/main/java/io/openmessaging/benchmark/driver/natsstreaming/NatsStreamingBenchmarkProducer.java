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
import io.openmessaging.benchmark.driver.PublicationLifecycle;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public class NatsStreamingBenchmarkProducer implements BenchmarkProducer {
    private final StreamingConnection natsStreamingPublisher;
    private final String topic;
    private final PublicationLifecycle lifecycle =
            new PublicationLifecycle("NATS Streaming producer");

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
        return lifecycle.publish(settle -> publish(ownedPayload, settle));
    }

    private void publish(byte[] ownedPayload, PublicationLifecycle.Settle settle)
            throws IOException, InterruptedException, TimeoutException {
        CompletableFuture<String> expectedGuid = new CompletableFuture<>();
        AckHandler acb = ackHandler(settle, expectedGuid);
        // The client may clear the caller's interrupt status while publishing
        boolean wasInterrupted = Thread.currentThread().isInterrupted();
        try {
            String guid = natsStreamingPublisher.publish(topic, ownedPayload, acb);
            if (guid == null || guid.isEmpty()) {
                throw new IllegalStateException("Expected non-empty guid to be returned.");
            }
            expectedGuid.complete(guid);
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static AckHandler ackHandler(
            PublicationLifecycle.Settle settle, CompletableFuture<String> expectedGuid) {
        return (s, e) -> {
            if (e != null) {
                settle.complete(e);
                return;
            }
            expectedGuid.thenAccept(guid -> {
                if (guid.equals(s)) {
                    settle.complete(null);
                } else {
                    settle.complete(new IllegalStateException("guid != nuid"));
                }
            });
        };
    }

    @Override
    public void close() {
        lifecycle.close();
    }
}
