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
package io.openmessaging.benchmark.driver;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A producer handle owned by the caller after successful creation.
 *
 * <p>Closing the producer releases its own resources and settles outstanding send futures, either
 * successfully or exceptionally. Shared clients and connections belong to the driver.
 */
public interface BenchmarkProducer extends AutoCloseable {

    /**
     * Publish a message asynchronously.
     *
     * <p>The returned future completes normally at the driver's documented send-completion point,
     * which depends on the broker and configuration. It may represent local acceptance, buffering, or
     * a broker acknowledgement; it does not necessarily imply durable storage. An operational failure
     * completes the future exceptionally with its cause, rather than reporting success.
     *
     * <p>The payload is caller-owned and read-only to the driver. The caller may use the same array
     * for concurrent sends, but must not mutate it until all associated futures have completed. The
     * driver may retain the array only until this send's future completes, including exceptional
     * completion or cancellation. It must use owned storage to mutate the payload or retain it beyond
     * completion.
     *
     * <p>Cancellation does not guarantee that the broker did not receive the message.
     *
     * @param key the key associated with this message
     * @param payload the caller-owned message payload
     * @return a non-null future that tracks the send's completion
     */
    CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload);
}
