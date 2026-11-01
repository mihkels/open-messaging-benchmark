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
package io.openmessaging.benchmark.e2e;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

final class MessageCapture implements ConsumerCallback {
    private final CompletableFuture<Delivery> delivery = new CompletableFuture<>();

    @Override
    public void messageReceived(byte[] payload, long timestamp) {
        delivery.complete(new Delivery(Arrays.copyOf(payload, payload.length), timestamp));
    }

    @Override
    public void messageReceived(ByteBuffer payload, long timestamp) {
        ByteBuffer copy = payload.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        delivery.complete(new Delivery(bytes, timestamp));
    }

    Delivery await() throws Exception {
        return delivery.get(10, TimeUnit.SECONDS);
    }

    record Delivery(byte[] payload, long timestamp) {}
}
