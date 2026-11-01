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
package io.openmessaging.benchmark.driver.kafka;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PendingSendTracker;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaBenchmarkProducer implements BenchmarkProducer {

    private final Producer<String, byte[]> producer;
    private final String topic;
    private final PendingSendTracker pending = new PendingSendTracker("Kafka producer");
    private final ThreadLocal<Boolean> closingOnThread = new ThreadLocal<>();

    public KafkaBenchmarkProducer(Producer<String, byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        CompletableFuture<Void> future = pending.register();
        if (future.isDone()) {
            return future;
        }
        try {
            var producerRecord = new ProducerRecord<>(topic, key.orElse(null), payload.clone());
            producer.send(
                    producerRecord, (metadata, exception) -> pending.completeFromClient(future, exception));
        } catch (RuntimeException error) {
            future.completeExceptionally(error);
        }

        return future;
    }

    @Override
    public void close() throws Exception {
        if (Boolean.TRUE.equals(closingOnThread.get())) {
            throw new IllegalStateException("Cannot close Kafka producer reentrantly");
        }
        boolean closeProducer = pending.beginClose();
        closingOnThread.set(true);
        Exception failure = null;
        try {
            if (closeProducer) {
                producer.close();
            }
        } catch (Exception closeFailure) {
            failure = closeFailure;
            throw closeFailure;
        } finally {
            try {
                pending.failRemaining(failure);
            } finally {
                closingOnThread.remove();
            }
        }
    }
}
