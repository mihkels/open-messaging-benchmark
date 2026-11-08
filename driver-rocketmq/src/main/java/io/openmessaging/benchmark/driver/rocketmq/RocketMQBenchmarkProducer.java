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
package io.openmessaging.benchmark.driver.rocketmq;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PublicationLifecycle;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class RocketMQBenchmarkProducer implements BenchmarkProducer {
    private final MQProducer rmqProducer;
    private final String rmqTopic;
    private final PublicationLifecycle lifecycle = new PublicationLifecycle("RocketMQ producer");

    public RocketMQBenchmarkProducer(final MQProducer rmqProducer, final String rmqTopic) {
        this.rmqProducer = rmqProducer;
        this.rmqTopic = rmqTopic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(final Optional<String> key, final byte[] payload) {
        Message message;
        try {
            message = new Message(this.rmqTopic, payload.clone());
            if (key.isPresent()) {
                message.setKeys(key.get());
            }
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        return lifecycle.publish(settle ->
                this.rmqProducer.send(
                        message,
                        new SendCallback() {
                            @Override
                            public void onSuccess(final SendResult sendResult) {
                                settle.complete(null);
                            }

                            @Override
                            public void onException(final Throwable e) {
                                settle.complete(
                                        e != null ? e : new IllegalStateException("Missing RocketMQ send failure"));
                            }
                        }));
    }

    @Override
    public void close() {
        lifecycle.close();
    }
}
