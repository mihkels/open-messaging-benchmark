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
package io.github.mihkels.omb.worker;

import static io.github.mihkels.omb.utils.UniformRateLimiter.interruptibleSleepNs;

import io.github.mihkels.omb.driver.BenchmarkProducer;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.utils.UniformRateLimiter;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

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

    /**
     * Changes the publish rate. Sends already waiting for their slot keep it.
     *
     * @param publishRate the new rate in messages per second
     */
    public void setPublishRate(double publishRate) {
        rateLimiter.setRate(publishRate);
    }

    public void sendMessage(BenchmarkProducer producer, Optional<String> key, byte[] payload)
            throws InterruptedException {
        sendMessage(producer, key, payload, false, null);
    }

    /**
     * Sends one message once the rate limiter allows it.
     *
     * @param producer the producer to send with
     * @param key the message key
     * @param payload the payload
     * @param stamped whether the payload carries a {@link DeliveryStamp}, so its acknowledgement
     *     counts towards the verified sends
     * @param headers the message headers, or {@code null} to send without headers
     * @throws InterruptedException if interrupted while waiting for the send slot
     */
    public void sendMessage(
            BenchmarkProducer producer,
            Optional<String> key,
            byte[] payload,
            boolean stamped,
            @Nullable Map<String, byte[]> headers)
            throws InterruptedException {
        sendMessage(producer, key, payload, stamped, headers, null);
    }

    /**
     * Sends one message once the rate limiter allows it, and with a topic records the partition the
     * driver reports into that partition's telemetry.
     *
     * @param producer the producer to send with
     * @param key the message key
     * @param payload the payload
     * @param stamped whether the payload carries a {@link DeliveryStamp}
     * @param headers the message headers, or {@code null} to send without headers
     * @param telemetryTopic the producer's topic when partition telemetry is on, else {@code null}
     * @throws InterruptedException if interrupted while waiting for the send slot
     */
    public void sendMessage(
            BenchmarkProducer producer,
            Optional<String> key,
            byte[] payload,
            boolean stamped,
            @Nullable Map<String, byte[]> headers,
            @Nullable String telemetryTopic)
            throws InterruptedException {
        final long intendedSendTime = rateLimiter.acquire();
        interruptibleSleepNs(intendedSendTime);
        if (!callbacks.beginSend()) {
            return;
        }
        try {
            final long sendTime = nanoClock.getAsLong();
            if (telemetryTopic != null) {
                Objects.requireNonNull(
                                producer.sendAsyncWithResult(key, payload, headers),
                                "sendAsyncWithResult returned null")
                        .whenComplete((result, failure) ->
                                callbacks.completeSend(
                                        payload.length,
                                        intendedSendTime,
                                        sendTime,
                                        nanoClock,
                                        failure,
                                        stamped,
                                        telemetryTopic,
                                        result == null ? ConsumerCallback.UNKNOWN_PARTITION : result.partition()));
                return;
            }
            CompletableFuture<Void> send =
                    Objects.requireNonNull(
                            headers == null
                                    ? producer.sendAsync(key, payload)
                                    : producer.sendAsync(key, payload, headers),
                            "sendAsync returned null");
            send.whenComplete((ignored, failure) ->
                    callbacks.completeSend(
                            payload.length, intendedSendTime, sendTime, nanoClock, failure, stamped));
        } catch (Throwable t) {
            callbacks.abortSend();
            throw t;
        }
    }
}
