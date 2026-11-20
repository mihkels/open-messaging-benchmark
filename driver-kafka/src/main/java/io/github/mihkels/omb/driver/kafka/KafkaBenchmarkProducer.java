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
package io.github.mihkels.omb.driver.kafka;

import io.github.mihkels.omb.driver.BenchmarkProducer;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.PendingSendTracker;
import io.github.mihkels.omb.driver.SendResult;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.Nullable;

public class KafkaBenchmarkProducer implements BenchmarkProducer {

    private final Producer<String, byte[]> producer;
    private final String topic;
    private final PendingSendTracker pending = new PendingSendTracker("Kafka producer");

    public KafkaBenchmarkProducer(Producer<String, byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        return sendAsync(key, payload, Map.of());
    }

    @Override
    public CompletableFuture<Void> sendAsync(
            Optional<String> key, byte[] payload, Map<String, byte[]> headers) {
        return send(key, payload, headers, null);
    }

    @Override
    public CompletableFuture<SendResult> sendAsyncWithResult(
            Optional<String> key, byte[] payload, @Nullable Map<String, byte[]> headers) {
        // Written by the client callback before it completes the send, so thenApply sees it
        int[] partition = {ConsumerCallback.UNKNOWN_PARTITION};
        return send(key, payload, headers == null ? Map.of() : headers, partition)
                .thenApply(ignored -> new SendResult(partition[0]));
    }

    private CompletableFuture<Void> send(
            Optional<String> key,
            byte[] payload,
            Map<String, byte[]> headers,
            int @Nullable [] partition) {
        CompletableFuture<Void> future = pending.register();
        if (future.isDone()) {
            return future;
        }
        try {
            var producerRecord = new ProducerRecord<>(topic, key.orElse(null), payload.clone());
            headers.forEach(producerRecord.headers()::add);
            producer.send(
                    producerRecord,
                    (metadata, exception) -> {
                        if (partition != null && metadata != null) {
                            partition[0] = metadata.partition();
                        }
                        pending.completeFromClient(future, exception);
                    });
        } catch (RuntimeException error) {
            future.completeExceptionally(error);
        }

        return future;
    }

    @Override
    public void close() throws Exception {
        pending.close(producer);
    }
}
