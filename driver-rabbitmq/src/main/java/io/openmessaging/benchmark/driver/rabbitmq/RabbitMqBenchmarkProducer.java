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
package io.openmessaging.benchmark.driver.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownListener;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMqBenchmarkProducer implements BenchmarkProducer {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqBenchmarkProducer.class);
    private static final BasicProperties defaultProperties = new BasicProperties();

    private final Channel channel;
    private final String exchange;
    private final boolean messagePersistence;
    private final PendingSends pendingSends = new PendingSends();
    private final ThreadLocal<Boolean> completingOnThread = new ThreadLocal<>();
    private final AtomicReference<Listeners> listeners = new AtomicReference<>();
    private final RecoveryListener recoveryListener =
            new RecoveryListener() {
                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {
                    complete(pendingSends.recoveryStarted());
                }

                @Override
                public void handleRecovery(Recoverable recoverable) {
                    if (pendingSends.recoveryFinished()) {
                        recover();
                    }
                }
            };

    public RabbitMqBenchmarkProducer(Channel channel, String exchange, boolean messagePersistence) {
        this.channel = channel;
        this.exchange = exchange;
        this.messagePersistence = messagePersistence;
        Listeners initial = listenersFor(0);
        listeners.set(initial);
        if (channel instanceof Recoverable recoverable) {
            recoverable.addRecoveryListener(recoveryListener);
        }
        channel.addConfirmListener(initial.confirm());
        channel.addShutdownListener(initial.shutdown());
        if (pendingSends.finishInitialization()) {
            recover();
        }
    }

    @Override
    public void close() throws Exception {
        if (Boolean.TRUE.equals(completingOnThread.get())) {
            throw new IllegalStateException(
                    "Cannot close a RabbitMQ producer from its completion callback");
        }
        PendingSends.CloseResult close = pendingSends.close();
        complete(close.sends());
        if (!close.closeChannel()) {
            return;
        }

        Exception failure = null;
        Listeners current = listeners.get();
        try {
            channel.removeConfirmListener(current.confirm());
        } catch (Exception e) {
            failure = e;
        }
        try {
            channel.removeShutdownListener(current.shutdown());
        } catch (Exception e) {
            failure = addFailure(failure, e);
        }
        if (channel instanceof Recoverable recoverable) {
            try {
                recoverable.removeRecoveryListener(recoveryListener);
            } catch (Exception e) {
                failure = addFailure(failure, e);
            }
        }
        try {
            channel.close();
        } catch (AlreadyClosedException e) {
            log.warn("Channel already closed", e);
        } catch (Exception e) {
            failure = addFailure(failure, e);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static Exception addFailure(Exception failure, Exception additional) {
        if (failure == null) {
            return additional;
        }
        if (failure != additional) {
            failure.addSuppressed(additional);
        }
        return failure;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        BasicProperties.Builder builder = defaultProperties.builder().timestamp(new Date());
        if (messagePersistence) {
            builder.deliveryMode(2);
        }
        BasicProperties props = builder.build();

        PendingSends.PublishPermit permit;
        try {
            permit = pendingSends.beginPublish();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }
        if (permit.rejection() != null) {
            return CompletableFuture.failedFuture(permit.rejection());
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        PendingSends.PendingSend send = null;
        boolean registered = false;
        Throwable failure = null;
        try {
            send =
                    new PendingSends.PendingSend(permit.generation(), channel.getNextPublishSeqNo(), future);
            PendingSends.PendingSend registeredSend = send;
            future.whenComplete((ignored, error) -> pendingSends.remove(registeredSend));
            failure = pendingSends.register(send);
            if (failure == null) {
                registered = true;
                channel.basicPublish(exchange, key.orElse(""), props, payload);
            }
        } catch (Exception e) {
            if (registered) {
                complete(pendingSends.fail(send, e));
            } else {
                failure = e;
            }
        } catch (Error e) {
            if (registered) {
                complete(pendingSends.fail(send, e));
            }
            throw e;
        } finally {
            complete(pendingSends.endPublish());
        }

        if (failure != null) {
            future.completeExceptionally(failure);
        }
        return future;
    }

    private Listeners listenersFor(long generation) {
        ConfirmListener confirm =
                new ConfirmListener() {
                    @Override
                    public void handleNack(long deliveryTag, boolean multiple) {
                        complete(pendingSends.confirm(generation, deliveryTag, multiple, true));
                    }

                    @Override
                    public void handleAck(long deliveryTag, boolean multiple) {
                        complete(pendingSends.confirm(generation, deliveryTag, multiple, false));
                    }
                };
        ShutdownListener shutdown = signal -> complete(pendingSends.shutdown(generation, signal));
        return new Listeners(confirm, shutdown);
    }

    private void recover() {
        boolean retry;
        do {
            PendingSends.RecoveryStart recovery = pendingSends.beginRecovery();
            if (recovery == null) {
                return;
            }
            Listeners next = null;
            boolean installed = false;
            try {
                complete(recovery.sends());
                if (pendingSends.isClosed()) {
                    return;
                }
                // A recovered channel can reuse sequence numbers; old callbacks must not settle new sends.
                next = listenersFor(recovery.generation());
                Listeners previous = listeners.getAndSet(next);
                channel.removeConfirmListener(previous.confirm());
                channel.removeShutdownListener(previous.shutdown());
                channel.addConfirmListener(next.confirm());
                channel.addShutdownListener(next.shutdown());
                installed = true;
            } finally {
                retry = pendingSends.endRecovery(recovery.generation(), installed);
                if (next != null && pendingSends.isClosed()) {
                    channel.removeConfirmListener(next.confirm());
                    channel.removeShutdownListener(next.shutdown());
                }
            }
        } while (retry);
    }

    private void complete(List<PendingSends.PendingSend> sends) {
        for (PendingSends.PendingSend send : sends) {
            Boolean previous = completingOnThread.get();
            completingOnThread.set(true);
            try {
                if (send.failure == null) {
                    send.future.complete(null);
                } else {
                    send.future.completeExceptionally(send.failure);
                }
            } finally {
                try {
                    pendingSends.remove(send);
                } finally {
                    if (previous == null) {
                        completingOnThread.remove();
                    } else {
                        completingOnThread.set(previous);
                    }
                }
            }
        }
    }

    private record Listeners(ConfirmListener confirm, ShutdownListener shutdown) {}
}
