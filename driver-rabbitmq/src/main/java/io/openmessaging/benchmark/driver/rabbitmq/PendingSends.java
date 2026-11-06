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

import com.rabbitmq.client.ShutdownSignalException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * The sends of one {@link RabbitMqBenchmarkProducer} awaiting publisher confirms, keyed by channel
 * generation and sequence number, including the publish gate and the channel recovery that
 * renumbers them.
 */
final class PendingSends {
    static final class PendingSend {
        final long generation;
        final long sequence;
        final CompletableFuture<Void> future;
        boolean decided;
        Throwable failure;

        PendingSend(long generation, long sequence, CompletableFuture<Void> future) {
            this.generation = generation;
            this.sequence = sequence;
            this.future = future;
        }
    }

    record CloseResult(List<PendingSend> sends, boolean closeChannel) {}

    record PublishPermit(long generation, Throwable rejection) {}

    record RecoveryStart(long generation, List<PendingSend> sends) {}

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
    synchronized PublishPermit beginPublish() throws InterruptedException {
        if (closedCause != null) {
            return new PublishPermit(generation, closedCause);
        }
        if (publishingThread == Thread.currentThread()) {
            return new PublishPermit(generation, new IllegalStateException("Reentrant RabbitMQ publish"));
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

    synchronized Throwable register(PendingSend send) {
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

    synchronized List<PendingSend> confirm(
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

    boolean select(PendingSend send, boolean nack) {
        if (send.decided) {
            return false;
        }
        send.decided = true;
        if (nack) {
            send.failure = new RuntimeException("Message was negatively acknowledged");
        }
        return true;
    }

    synchronized List<PendingSend> fail(PendingSend send, Throwable cause) {
        if (sends.get(send.sequence) != send || send.decided || send.future.isDone()) {
            return List.of();
        }
        send.decided = true;
        send.failure = cause;
        return deferIfPublishing(List.of(send));
    }

    List<PendingSend> deferIfPublishing(List<PendingSend> ready) {
        if (publishingThread == Thread.currentThread()) {
            deferred.addAll(ready);
            return List.of();
        }
        return ready;
    }

    synchronized List<PendingSend> endPublish() {
        publishingThread = null;
        notifyAll();
        if (deferred.isEmpty()) {
            return List.of();
        }
        List<PendingSend> ready = new ArrayList<>(deferred);
        deferred.clear();
        return ready;
    }

    synchronized List<PendingSend> shutdown(long listenerGeneration, ShutdownSignalException cause) {
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

    synchronized boolean finishInitialization() {
        initialized = true;
        boolean requested = recoveryRequested && !recoveryInProgress;
        if (requested) {
            recoveryRequested = false;
        }
        return requested;
    }

    synchronized List<PendingSend> recoveryStarted() {
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

    synchronized boolean recoveryFinished() {
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

    synchronized RecoveryStart beginRecovery() {
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

    synchronized boolean endRecovery(long recoveryGeneration, boolean installed) {
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

    synchronized boolean isClosed() {
        return closeStarted;
    }

    synchronized CloseResult close() {
        if (publishingThread == Thread.currentThread()) {
            throw new IllegalStateException(
                    "Cannot close a RabbitMQ producer reentrantly while it is publishing");
        }
        boolean closeChannel = !closeStarted;
        closeStarted = true;
        return new CloseResult(
                stop(new IllegalStateException("RabbitMQ producer is closed")), closeChannel);
    }

    private synchronized List<PendingSend> stop(Throwable cause) {
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

    synchronized void remove(PendingSend send) {
        sends.remove(send.sequence, send);
    }
}
