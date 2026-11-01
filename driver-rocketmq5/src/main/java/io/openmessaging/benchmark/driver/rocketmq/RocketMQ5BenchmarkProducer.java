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
    private final PendingSendTracker pending = new PendingSendTracker("RocketMQ 5 producer");
    private final Object lifecycleLock = new Object();
    private final ThreadLocal<Integer> publishingDepth = new ThreadLocal<>();
    private boolean closeComplete;
    private int activePublications;

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
            if (this.sendDelayMsg) {
                long delayTime = System.currentTimeMillis() + this.delayTimeInSec * 1000;
                messageBuilder.setDeliveryTimestamp(delayTime);
            }
            message = messageBuilder.build();
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
            this.rmqProducer
                    .sendAsync(message)
                    .whenComplete(
                            (sendReceipt, throwable) -> {
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
                    "Cannot close a RocketMQ 5 producer while publishing on the same thread; "
                            + "retry after sendAsync returns");
        }
        publishingDepth.remove();
        boolean interrupted = false;
        boolean closeOwner;
        synchronized (lifecycleLock) {
            closeOwner = pending.beginClose();
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
            if (closeOwner && rmqProducer != null) {
                rmqProducer.close();
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
}
