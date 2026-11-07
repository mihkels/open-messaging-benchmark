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
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class RocketMQBenchmarkProducer implements BenchmarkProducer {
    private final MQProducer rmqProducer;
    private final String rmqTopic;
    private final PendingSendTracker pending = new PendingSendTracker("RocketMQ producer");
    private final Object lifecycleLock = new Object();
    private final ThreadLocal<Integer> publishingDepth = new ThreadLocal<>();
    private boolean closeComplete;
    private int activePublications;

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
            if (!future.isDone()) {
                this.rmqProducer.send(
                        message,
                        new SendCallback() {
                            @Override
                            public void onSuccess(final SendResult sendResult) {
                                pending.completeFromClient(future, null);
                            }

                            @Override
                            public void onException(final Throwable e) {
                                pending.completeFromClient(
                                        future,
                                        e != null ? e : new IllegalStateException("Missing RocketMQ send failure"));
                            }
                        });
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            future.completeExceptionally(e);
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
    public void close() {
        if (publishingDepth.get() != null) {
            throw new IllegalStateException(
                    "Cannot close a RocketMQ producer while publishing on the same thread; "
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
        try {
            if (closeOwner) {
                pending.failRemaining();
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
