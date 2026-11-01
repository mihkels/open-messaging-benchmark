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
import com.rabbitmq.client.ShutdownSignalException;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
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
        CloseResult close = pendingSends.close();
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

        PublishPermit permit;
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
        PendingSend send = null;
        boolean registered = false;
        Throwable failure = null;
        try {
            send = new PendingSend(permit.generation(), channel.getNextPublishSeqNo(), future);
            PendingSend registeredSend = send;
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
            RecoveryStart recovery = pendingSends.beginRecovery();
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

    private void complete(List<PendingSend> sends) {
        for (PendingSend send : sends) {
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

    private static final class PendingSend {
        private final long generation;
        private final long sequence;
        private final CompletableFuture<Void> future;
        private boolean decided;
        private Throwable failure;

        private PendingSend(long generation, long sequence, CompletableFuture<Void> future) {
            this.generation = generation;
            this.sequence = sequence;
            this.future = future;
        }
    }

    private record CloseResult(List<PendingSend> sends, boolean closeChannel) {}

    private record Listeners(ConfirmListener confirm, ShutdownListener shutdown) {}

    private record PublishPermit(long generation, Throwable rejection) {}

    private record RecoveryStart(long generation, List<PendingSend> sends) {}

    private static final class PendingSends {
        private final NavigableMap<Long, PendingSend> sends = new TreeMap<>(Long::compareUnsigned);
        private final List<PendingSend> deferred = new ArrayList<>();
        private Thread publishingThread;
        private Throwable closedCause;
        private Throwable lastShutdownCause;
        private long generation;
        private long failedGeneration = -1;
        private boolean initialized;
        private boolean recovering;
        private boolean recoveryInProgress;
        private boolean recoveryRequested;
        private boolean closeStarted;

        // The gate serializes sequence lookup and publish without holding the monitor in the client.
        private synchronized PublishPermit beginPublish() throws InterruptedException {
            if (closedCause != null) {
                return new PublishPermit(generation, closedCause);
            }
            if (publishingThread == Thread.currentThread()) {
                return new PublishPermit(
                        generation, new IllegalStateException("Reentrant RabbitMQ publish"));
            }
            while (publishingThread != null && closedCause == null) {
                wait();
            }
            if (closedCause != null) {
                return new PublishPermit(generation, closedCause);
            }
            publishingThread = Thread.currentThread();
            return new PublishPermit(generation, null);
        }

        private synchronized Throwable register(PendingSend send) {
            if (closedCause != null) {
                return closedCause;
            }
            if (send.generation != generation) {
                return lastShutdownCause != null
                        ? lastShutdownCause
                        : new IllegalStateException("RabbitMQ channel changed during publish");
            }
            PendingSend previous = sends.get(send.sequence);
            if (previous != null && previous.future.isDone()) {
                sends.remove(send.sequence);
                previous = null;
            }
            if (previous != null) {
                return new IllegalStateException(
                        "Duplicate RabbitMQ publish sequence: " + Long.toUnsignedString(send.sequence));
            }
            sends.put(send.sequence, send);
            return null;
        }

        private synchronized List<PendingSend> confirm(
                long listenerGeneration, long tag, boolean multiple, boolean nack) {
            if (listenerGeneration != generation) {
                return List.of();
            }
            List<PendingSend> ready = new ArrayList<>();
            if (multiple) {
                Iterator<PendingSend> iterator = sends.headMap(tag, true).values().iterator();
                while (iterator.hasNext()) {
                    PendingSend send = iterator.next();
                    if (send.future.isDone()) {
                        iterator.remove();
                    } else if (select(send, nack)) {
                        ready.add(send);
                    }
                }
            } else {
                PendingSend send = sends.get(tag);
                if (send != null) {
                    if (send.future.isDone()) {
                        sends.remove(tag);
                    } else if (select(send, nack)) {
                        ready.add(send);
                    }
                }
            }
            return deferIfPublishing(ready);
        }

        private boolean select(PendingSend send, boolean nack) {
            if (send.decided) {
                return false;
            }
            send.decided = true;
            if (nack) {
                send.failure = new RuntimeException("Message was negatively acknowledged");
            }
            return true;
        }

        private synchronized List<PendingSend> fail(PendingSend send, Throwable cause) {
            if (sends.get(send.sequence) != send || send.decided || send.future.isDone()) {
                return List.of();
            }
            send.decided = true;
            send.failure = cause;
            return deferIfPublishing(List.of(send));
        }

        private List<PendingSend> deferIfPublishing(List<PendingSend> ready) {
            if (publishingThread == Thread.currentThread()) {
                deferred.addAll(ready);
                return List.of();
            }
            return ready;
        }

        private synchronized List<PendingSend> endPublish() {
            publishingThread = null;
            notifyAll();
            if (deferred.isEmpty()) {
                return List.of();
            }
            List<PendingSend> ready = new ArrayList<>(deferred);
            deferred.clear();
            return ready;
        }

        private synchronized List<PendingSend> shutdown(
                long listenerGeneration, ShutdownSignalException cause) {
            if (listenerGeneration != generation) {
                return List.of();
            }
            Throwable failure =
                    cause != null ? cause : new IllegalStateException("RabbitMQ channel shut down");
            if (!closeStarted && failedGeneration != generation) {
                closedCause = failure;
                lastShutdownCause = failure;
                failedGeneration = generation;
            }
            return stop(failure);
        }

        private synchronized boolean finishInitialization() {
            initialized = true;
            boolean requested = recoveryRequested && !recoveryInProgress;
            if (requested) {
                recoveryRequested = false;
            }
            return requested;
        }

        private synchronized List<PendingSend> recoveryStarted() {
            if (closeStarted) {
                return List.of();
            }
            // A failed recovery attempt need not emit a matching completion callback.
            recoveryInProgress = true;
            Throwable cause =
                    closedCause != null
                            ? closedCause
                            : new IllegalStateException("RabbitMQ channel is recovering");
            lastShutdownCause = cause;
            failedGeneration = generation;
            return stop(cause);
        }

        private synchronized boolean recoveryFinished() {
            recoveryInProgress = false;
            if (closeStarted) {
                return false;
            }
            if (!initialized || recovering) {
                recoveryRequested = true;
                return false;
            }
            return true;
        }

        private synchronized RecoveryStart beginRecovery() {
            if (!initialized || recovering || recoveryInProgress) {
                recoveryRequested = true;
                return null;
            }
            if (closeStarted) {
                return null;
            }
            recovering = true;
            recoveryRequested = false;
            generation++;
            Throwable cause =
                    closedCause != null
                            ? closedCause
                            : new IllegalStateException("RabbitMQ channel is recovering");
            lastShutdownCause = cause;
            return new RecoveryStart(generation, stop(cause));
        }

        private synchronized boolean endRecovery(long recoveryGeneration, boolean installed) {
            if (generation == recoveryGeneration) {
                recovering = false;
                if (installed && !closeStarted && !recoveryInProgress && failedGeneration != generation) {
                    closedCause = null;
                }
            }
            boolean retry = recoveryRequested && !closeStarted && !recoveryInProgress;
            if (retry || closeStarted) {
                recoveryRequested = false;
            }
            return retry;
        }

        private synchronized boolean isClosed() {
            return closeStarted;
        }

        private synchronized CloseResult close() {
            if (publishingThread == Thread.currentThread()) {
                throw new IllegalStateException(
                        "Cannot close a RabbitMQ producer reentrantly while it is publishing");
            }
            boolean closeChannel = !closeStarted;
            closeStarted = true;
            return new CloseResult(
                    stop(new IllegalStateException("RabbitMQ producer is closed")), closeChannel);
        }

        private List<PendingSend> stop(Throwable cause) {
            if (closedCause == null) {
                closedCause = cause;
            }
            notifyAll();
            deferred.clear();
            List<PendingSend> ready = new ArrayList<>(sends.size());
            Iterator<PendingSend> iterator = sends.values().iterator();
            while (iterator.hasNext()) {
                PendingSend send = iterator.next();
                if (send.future.isDone()) {
                    iterator.remove();
                } else {
                    if (!send.decided) {
                        send.decided = true;
                        send.failure = closedCause;
                    }
                    ready.add(send);
                }
            }
            return deferIfPublishing(ready);
        }

        private synchronized void remove(PendingSend send) {
            sends.remove(send.sequence, send);
        }
    }
}
