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
package io.openmessaging.benchmark.worker;

import static io.openmessaging.benchmark.utils.UniformRateLimiter.interruptibleSleepNs;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.utils.UniformRateLimiter;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

public class MessageProducer {

    private final RunCallbacks callbacks;
    private final UniformRateLimiter rateLimiter;
    private final LongSupplier nanoClock;

    MessageProducer(UniformRateLimiter rateLimiter, RunCallbacks callbacks) {
        this(System::nanoTime, rateLimiter, callbacks);
    }

    MessageProducer(LongSupplier nanoClock, UniformRateLimiter rateLimiter, RunCallbacks callbacks) {
        this.nanoClock = nanoClock;
        this.rateLimiter = rateLimiter;
        this.callbacks = callbacks;
    }

    public void sendMessage(BenchmarkProducer producer, Optional<String> key, byte[] payload)
            throws InterruptedException {
        final long intendedSendTime = rateLimiter.acquire();
        interruptibleSleepNs(intendedSendTime);
        if (!callbacks.beginSend()) {
            return;
        }
        try {
            final long sendTime = nanoClock.getAsLong();
            CompletableFuture<Void> send =
                    Objects.requireNonNull(producer.sendAsync(key, payload), "sendAsync returned null");
            send.whenComplete(
                    (ignored, failure) ->
                            callbacks.completeSend(
                                    payload.length, intendedSendTime, sendTime, nanoClock, failure));
        } catch (Throwable t) {
            callbacks.abortSend();
            throw t;
        }
    }
}
