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

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class MqttBenchmarkConsumer implements BenchmarkConsumer {
    private volatile Mqtt5AsyncClient client;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<Void> closeCompletion = new CompletableFuture<>();
    private final ThreadLocal<Boolean> inCallback = new ThreadLocal<>();

    public MqttBenchmarkConsumer() {}

    @Override
    public void close() throws Exception {
        if (Boolean.TRUE.equals(inCallback.get())) {
            throw new IllegalStateException("Cannot close MQTT consumer from its own callback");
        }
        if (closed.compareAndSet(false, true)) {
            try {
                MqttBenchmarkDriver.closeClient(client);
                closeCompletion.complete(null);
            } catch (Exception | Error failure) {
                closeCompletion.completeExceptionally(failure);
            }
        }
        try {
            closeCompletion.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception failure) {
                throw failure;
            }
            if (e.getCause() instanceof Error failure) {
                throw failure;
            }
            throw e;
        }
    }

    ConsumerCallback guardedCallback(ConsumerCallback callback) {
        return new ConsumerCallback() {
            @Override
            public void messageReceived(byte[] payload, long publishTimestamp) {
                invokeCallback(() -> callback.messageReceived(payload, publishTimestamp));
            }

            @Override
            public void messageReceived(ByteBuffer payload, long publishTimestamp) {
                invokeCallback(() -> callback.messageReceived(payload, publishTimestamp));
            }
        };
    }

    private void invokeCallback(Runnable callback) {
        Boolean previous = inCallback.get();
        inCallback.set(true);
        try {
            callback.run();
        } finally {
            if (previous == null) {
                inCallback.remove();
            } else {
                inCallback.set(previous);
            }
        }
    }

    public void setClient(Mqtt5AsyncClient client) {
        this.client = client;
    }

    public boolean isClosed() {
        return closed.get();
    }
}
