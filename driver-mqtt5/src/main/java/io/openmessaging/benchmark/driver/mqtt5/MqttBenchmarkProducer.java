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
package io.openmessaging.benchmark.driver.mqtt5;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PendingSendTracker;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttBenchmarkProducer implements BenchmarkProducer {

    private static final Logger log = LoggerFactory.getLogger(MqttBenchmarkProducer.class);

    private final String topic;
    private final MqttQos qos;
    private volatile Mqtt5AsyncClient client;
    private final PendingSendTracker pending = new PendingSendTracker("MQTT producer");
    private final Object lifecycleLock = new Object();
    private final ThreadLocal<Integer> publishingDepth = new ThreadLocal<>();
    private volatile boolean closed;
    private boolean closeComplete;
    private int activePublications;

    public MqttBenchmarkProducer(String topic, int qosCode) {
        this.topic = topic;
        this.qos = MqttBenchmarkDriver.intToQoS(qosCode);
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        Mqtt5Publish message;
        try {
            Mqtt5UserProperties properties =
                    Mqtt5UserProperties.builder()
                            .add(
                                    MqttBenchmarkDriver.USER_PROPERTY_KEY_PUBLISH_TIMESTAMP,
                                    MqttUtf8String.of(String.valueOf(System.currentTimeMillis())))
                            .build();
            message =
                    Mqtt5Publish.builder()
                            .topic(topic)
                            .payload(payload.clone())
                            .qos(qos)
                            .userProperties(properties)
                            .build();
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
        try {
            this.client
                    .publish(message)
                    .whenComplete(
                            (result, ex) -> {
                                try {
                                    if (ex != null) {
                                        pending.completeFromClient(future, ex);
                                        log.error("Failed to publish MQTT message, topic={}", topic, ex);
                                    } else if (result == null) {
                                        pending.completeFromClient(
                                                future, new IllegalStateException("Missing MQTT publish result"));
                                    } else if (result.getError().isPresent()) {
                                        Throwable error = result.getError().get();
                                        pending.completeFromClient(future, error);
                                        log.error("Failed to publish MQTT message, topic={}", topic, error);
                                    } else {
                                        pending.completeFromClient(future, null);
                                    }
                                } catch (Exception failure) {
                                    pending.completeFromClient(future, failure);
                                }
                            });
        } catch (Exception failure) {
            future.completeExceptionally(failure);
        } finally {
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
    public void close() throws Exception {
        if (publishingDepth.get() != null) {
            throw new IllegalStateException(
                    "Cannot close an MQTT producer while publishing on the same thread; "
                            + "retry after sendAsync returns");
        }
        publishingDepth.remove();
        boolean interrupted = false;
        boolean closeOwner;
        synchronized (lifecycleLock) {
            closeOwner = pending.beginClose();
            if (closeOwner) {
                closed = true;
            }
            while (closeOwner ? activePublications != 0 : !closeComplete) {
                try {
                    lifecycleLock.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        Throwable closeFailure = null;
        try {
            if (closeOwner) {
                MqttBenchmarkDriver.closeClient(client);
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

    public void setClient(Mqtt5AsyncClient client) {
        this.client = client;
    }

    public boolean isClosed() {
        return closed;
    }
}
