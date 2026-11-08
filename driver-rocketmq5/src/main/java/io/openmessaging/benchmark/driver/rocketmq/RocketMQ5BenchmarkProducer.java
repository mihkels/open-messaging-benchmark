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
import io.openmessaging.benchmark.driver.PendingSendTracker;
import io.openmessaging.benchmark.driver.PublicationLifecycle;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;

public class RocketMQ5BenchmarkProducer implements BenchmarkProducer {
    private final Producer rmqProducer;
    private final String rmqTopic;
    private final Boolean sendDelayMsg;
    private final Long delayTimeInSec;
    private final PublicationLifecycle lifecycle = new PublicationLifecycle("RocketMQ 5 producer");
    private final PendingSendTracker pending = lifecycle.pending();

    public RocketMQ5BenchmarkProducer(final Producer rmqProducer, final String rmqTopic) {
        this.rmqProducer = rmqProducer;
        this.rmqTopic = rmqTopic;
        this.sendDelayMsg = false;
        this.delayTimeInSec = 0L;
    }

    public RocketMQ5BenchmarkProducer(
            final Producer rmqProducer,
            final String rmqTopic,
            Boolean sendDelayMsg,
            Long delayTimeInSec) {
        this.rmqProducer = rmqProducer;
        this.rmqTopic = rmqTopic;
        this.sendDelayMsg = sendDelayMsg;
        this.delayTimeInSec = delayTimeInSec;
    }

    @Override
    public CompletableFuture<Void> sendAsync(final Optional<String> key, final byte[] payload) {
        org.apache.rocketmq.client.apis.message.Message message;
        try {
            MessageBuilder messageBuilder = new MessageBuilderImpl();
            messageBuilder.setBody(payload.clone());
            messageBuilder.setTopic(this.rmqTopic);
            key.ifPresent(messageBuilder::setKeys);
            if (Boolean.TRUE.equals(this.sendDelayMsg)) {
                long delayTime = System.currentTimeMillis() + this.delayTimeInSec * 1000;
                messageBuilder.setDeliveryTimestamp(delayTime);
            }
            message = messageBuilder.build();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Void> future = lifecycle.begin();
        if (future.isDone()) {
            return future;
        }
        try {
            this.rmqProducer
                    .sendAsync(message)
                    .whenComplete((sendReceipt, throwable) -> {
                        if (throwable != null) {
                            pending.completeFromClient(future, throwable);
                        } else if (sendReceipt == null) {
                            pending.completeFromClient(
                                    future, new IllegalStateException("Missing RocketMQ 5 send receipt"));
                        } else {
                            pending.completeFromClient(future, null);
                        }
                    });
        } catch (Exception failure) {
            future.completeExceptionally(failure);
        } finally {
            lifecycle.end();
        }
        return future;
    }

    @Override
    public void close() throws Exception {
        lifecycle.close(() -> {
            if (rmqProducer != null) {
                rmqProducer.close();
            }
        });
    }
}
