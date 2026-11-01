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
package io.openmessaging.benchmark.driver.natsStreaming;

import io.nats.streaming.AckHandler;
import io.nats.streaming.StreamingConnection;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PendingSendTracker;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class NatsStreamingBenchmarkProducer implements BenchmarkProducer {
    private final StreamingConnection natsStreamingPublisher;
    private final String topic;
    private final PendingSendTracker pending = new PendingSendTracker("NATS Streaming producer");
    private final Object lifecycleLock = new Object();
    private final ThreadLocal<Integer> publishingDepth = new ThreadLocal<>();
    private boolean closeComplete;
    private int activePublications;

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
        CompletableFuture<Void> future;
        synchronized (lifecycleLock) {
            future = pending.register();
            if (future.isDone()) {
                return future;
            }
            activePublications++;
            Integer depth = publishingDepth.get();
            publishingDepth.set(depth == null ? 1 : depth + 1);
        }
        CompletableFuture<String> expectedGuid = new CompletableFuture<>();
        AckHandler acb =
                new AckHandler() {
                    @Override
                    public void onAck(String s, Exception e) {
                        if (e != null) {
                            pending.completeFromClient(future, e);
                            return;
                        }
                        expectedGuid.thenAccept(
                                guid -> {
                                    if (guid.equals(s)) {
                                        pending.completeFromClient(future, null);
                                    } else {
                                        pending.completeFromClient(future, new IllegalStateException("guid != nuid"));
                                    }
                                });
                    }
                };
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
            int depth = publishingDepth.get() - 1;
            if (depth == 0) {
                publishingDepth.remove();
            } else {
                publishingDepth.set(depth);
            }
            synchronized (lifecycleLock) {
                activePublications--;
                lifecycleLock.notifyAll();
            }
        }
        return future;
    }

    @Override
    public void close() {
        if (publishingDepth.get() != null) {
            throw new IllegalStateException(
                    "Cannot close a NATS Streaming producer while publishing on the same thread; "
                            + "retry after sendAsync returns");
        }
        publishingDepth.remove();
        boolean interrupted = false;
        boolean closeOwner;
        synchronized (lifecycleLock) {
            closeOwner = pending.beginClose();
            while (closeOwner ? activePublications != 0 : !closeComplete) {
                try {
                    lifecycleLock.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        try {
            if (closeOwner) {
                pending.failRemaining();
            }
        } finally {
            if (closeOwner) {
                synchronized (lifecycleLock) {
                    closeComplete = true;
                    lifecycleLock.notifyAll();
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
