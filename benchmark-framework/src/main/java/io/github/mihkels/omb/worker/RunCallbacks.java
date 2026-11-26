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
package io.github.mihkels.omb.worker;

import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.RebalanceEvent;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gates callbacks by run and tracks callbacks that must finish before statistics are reset. */
final class RunCallbacks implements ConsumerCallback {
    private final WorkerStats stats;
    private final Consumer<Throwable> unexpectedFailure;
    private final Gate gate = new Gate();
    // Striped because every send and receive updates them; only read after seal(), when no
    // callback can increment them any more, so a sum of zero means none is in flight.
    private final LongAdder sends = new LongAdder();
    private final LongAdder receives = new LongAdder();
    // State is atomic or volatile and written before signalling; waiters re-check it under this
    // lock, so a signal is only needed once a waiter can exist: while paused or after seal().
    private final ReentrantLock progress = new ReentrantLock();
    private final Condition progressChanged = progress.newCondition();
    private volatile boolean accepting = true;
    private volatile boolean paused;

    RunCallbacks(WorkerStats stats, Consumer<Throwable> unexpectedFailure) {
        this.stats = stats;
        this.unexpectedFailure = unexpectedFailure;
    }

    boolean beginSend() {
        int slot = gate.enter();
        try {
            if (!accepting) {
                return false;
            }
            sends.increment();
            return true;
        } finally {
            gate.exit(slot);
        }
    }

    void completeSend(
            long payloadLength,
            long intendedSendTime,
            long sendTime,
            LongSupplier nanoClock,
            @Nullable Throwable failure) {
        completeSend(payloadLength, intendedSendTime, sendTime, nanoClock, failure, false);
    }

    void completeSend(
            long payloadLength,
            long intendedSendTime,
            long sendTime,
            LongSupplier nanoClock,
            @Nullable Throwable failure,
            boolean stamped) {
        completeSend(
                payloadLength,
                intendedSendTime,
                sendTime,
                nanoClock,
                failure,
                stamped,
                null,
                ConsumerCallback.UNKNOWN_PARTITION);
    }

    // Records a completed send; with a topic and a known partition, also into its partition telemetry
    @SuppressWarnings("checkstyle:ParameterNumber")
    void completeSend(
            long payloadLength,
            long intendedSendTime,
            long sendTime,
            LongSupplier nanoClock,
            @Nullable Throwable failure,
            boolean stamped,
            @Nullable String topic,
            int partition) {
        try {
            int slot = gate.enter();
            try {
                if (accepting) {
                    if (failure == null) {
                        long now = nanoClock.getAsLong();
                        stats.recordProducerSuccess(payloadLength, intendedSendTime, sendTime, now);
                        if (topic != null && partition >= 0) {
                            stats.recordPartitionPublish(
                                    topic, partition, TimeUnit.NANOSECONDS.toMicros(now - sendTime));
                        }
                        if (stamped) {
                            stats.recordVerifiedSend();
                        }
                    } else {
                        stats.recordProducerFailure(failure);
                    }
                }
            } finally {
                gate.exit(slot);
            }
            if (failure != null && accepting) {
                log.debug("Write error on message", failure);
            }
        } catch (Throwable t) {
            unexpectedFailure.accept(t);
        } finally {
            abortSend();
        }
    }

    void completeProbe(@Nullable Throwable failure) {
        try {
            int slot = gate.enter();
            try {
                if (accepting && failure == null) {
                    stats.recordMessageSent();
                }
            } finally {
                gate.exit(slot);
            }
        } catch (Throwable t) {
            unexpectedFailure.accept(t);
        } finally {
            abortSend();
        }
    }

    void abortSend() {
        sends.decrement();
        signalIfObserved();
    }

    @Override
    public void messageReceived(byte[] data, long publishTimestamp) {
        receive(data.length, publishTimestamp);
    }

    @Override
    public void messageReceived(ByteBuffer data, long publishTimestamp) {
        receive(data.remaining(), publishTimestamp);
    }

    void receive(int size, long publishTimestamp) {
        receive(size, publishTimestamp, null, UNKNOWN_PARTITION, null, null, null);
    }

    /**
     * A callback for one consumer's topic that also records partition telemetry: per-partition
     * traffic and latency, and the consumer's rebalance events.
     *
     * @param topic the consumer's topic
     * @return the callback
     */
    ConsumerCallback forTopic(String topic) {
        return forConsumer(topic, null);
    }

    /**
     * A consumer callback gated by this run's lifecycle that optionally records per-partition
     * telemetry and/or checks delivery stamps inside the seal gate.
     *
     * @param topic the consumer's topic when partition telemetry is enabled, or {@code null}
     * @param delivery the subscription verifier when delivery verification is enabled, or {@code
     *     null}
     * @return the consumer callback
     */
    ConsumerCallback forConsumer(
            @Nullable String topic, DeliveryVerifier.@Nullable Subscription delivery) {
        return new ConsumerCallback() {
            @Override
            public void messageReceived(byte[] payload, long publishTimestamp) {
                receive(
                        payload.length, publishTimestamp, topic, UNKNOWN_PARTITION, delivery, payload, null);
            }

            @Override
            public void messageReceived(ByteBuffer payload, long publishTimestamp) {
                receive(
                        payload.remaining(),
                        publishTimestamp,
                        topic,
                        UNKNOWN_PARTITION,
                        delivery,
                        null,
                        payload);
            }

            @Override
            public void messageReceived(byte[] payload, long publishTimestamp, int partition) {
                receive(payload.length, publishTimestamp, topic, partition, delivery, payload, null);
            }

            @Override
            public void messageReceived(ByteBuffer payload, long publishTimestamp, int partition) {
                receive(payload.remaining(), publishTimestamp, topic, partition, delivery, null, payload);
            }

            @Override
            public void partitionsChanged(RebalanceEvent event) {
                if (topic != null) {
                    rebalance(topic, event);
                }
            }
        };
    }

    private void rebalance(String topic, RebalanceEvent event) {
        int slot = gate.enter();
        try {
            if (accepting) {
                stats.recordRebalance(topic, event);
            }
        } catch (Throwable t) {
            unexpectedFailure.accept(t);
        } finally {
            gate.exit(slot);
        }
    }

    private void receive(
            int size,
            long publishTimestamp,
            @Nullable String topic,
            int partition,
            DeliveryVerifier.@Nullable Subscription delivery,
            byte @Nullable [] arrayPayload,
            @Nullable ByteBuffer bufferPayload) {
        int slot = gate.enter();
        try {
            if (!accepting) {
                return;
            }
            receives.increment();
            try {
                long latencyMicros =
                        TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis() - publishTimestamp);
                stats.recordMessageReceived(size, latencyMicros);
                if (topic != null && partition >= 0) {
                    stats.recordPartitionReceive(topic, partition, latencyMicros);
                }
                if (delivery != null) {
                    if (arrayPayload != null) {
                        delivery.verify(arrayPayload);
                    } else if (bufferPayload != null) {
                        delivery.verify(bufferPayload);
                    }
                }
            } catch (Throwable t) {
                receives.decrement();
                signalProgress();
                unexpectedFailure.accept(t);
                throw t;
            }
        } finally {
            gate.exit(slot);
        }

        awaitResumeAndExit();
    }

    private void awaitResumeAndExit() {
        try {
            if (paused) {
                progress.lock();
                try {
                    while (paused && accepting) {
                        progressChanged.await();
                    }
                } finally {
                    progress.unlock();
                }
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } finally {
            receives.decrement();
            signalIfObserved();
        }
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        signalProgress();
    }

    boolean seal(long deadlineNanos) throws InterruptedException {
        accepting = false;
        paused = false;
        signalProgress();
        long remaining = deadlineNanos - System.nanoTime();
        return remaining > 0 && gate.awaitEmpty(deadlineNanos);
    }

    boolean awaitQuiescence(long deadlineNanos) throws InterruptedException {
        progress.lockInterruptibly();
        try {
            long remaining = deadlineNanos - System.nanoTime();
            while (sends.sum() != 0 || receives.sum() != 0) {
                if (remaining <= 0) {
                    return false;
                }
                remaining =
                        Math.min(progressChanged.awaitNanos(remaining), deadlineNanos - System.nanoTime());
            }
            return true;
        } finally {
            progress.unlock();
        }
    }

    // The counter is written before this read and a waiter's flag before its counter check, so a
    // waiter that missed the decrement is seen here.
    private void signalIfObserved() {
        if (!accepting || paused) {
            signalProgress();
        }
    }

    private void signalProgress() {
        progress.lock();
        try {
            progressChanged.signalAll();
        } finally {
            progress.unlock();
        }
    }

    /**
     * Lets {@link #seal(long)} wait for callbacks that saw {@code accepting} still true. A callback
     * increments its thread's slot before it reads {@code accepting} and decrements the same slot
     * when done; seal clears {@code accepting} before it waits for every slot to drain. Unlike a read
     * lock, threads in different slots share no cache line, so concurrent callbacks scale.
     */
    static final class Gate {
        private static final int SLOTS = 16;
        // 16 longs are 128 bytes, the largest common cache line, so slots never share one
        private static final int SPACING = 16;
        private static final long POLL_NANOS = 10_000;

        private final AtomicLongArray active = new AtomicLongArray(SLOTS * SPACING);

        int enter() {
            int slot = ((int) Thread.currentThread().threadId() & (SLOTS - 1)) * SPACING;
            active.getAndIncrement(slot);
            return slot;
        }

        void exit(int slot) {
            active.getAndDecrement(slot);
        }

        boolean awaitEmpty(long deadlineNanos) throws InterruptedException {
            for (int slot = 0; slot < SLOTS * SPACING; slot += SPACING) {
                while (active.get(slot) != 0) {
                    if (Thread.interrupted()) {
                        throw new InterruptedException("Interrupted while sealing the callback gate");
                    }
                    if (deadlineNanos - System.nanoTime() <= 0) {
                        return false;
                    }
                    LockSupport.parkNanos(POLL_NANOS);
                }
            }
            return true;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RunCallbacks.class);
}
