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
package io.openmessaging.benchmark.worker;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gates callbacks by run and tracks callbacks that must finish before statistics are reset. */
final class RunCallbacks implements ConsumerCallback {
    private final WorkerStats stats;
    private final Consumer<Throwable> unexpectedFailure;
    private final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();
    private final AtomicInteger sends = new AtomicInteger();
    private final AtomicInteger receives = new AtomicInteger();
    // State is atomic or volatile; checking and signalling under this lock prevents lost wakeups.
    private final ReentrantLock progress = new ReentrantLock();
    private final Condition progressChanged = progress.newCondition();
    private volatile boolean accepting = true;
    private volatile boolean paused;

    RunCallbacks(WorkerStats stats, Consumer<Throwable> unexpectedFailure) {
        this.stats = stats;
        this.unexpectedFailure = unexpectedFailure;
    }

    boolean beginSend() {
        gate.readLock().lock();
        try {
            if (!accepting) {
                return false;
            }
            sends.incrementAndGet();
            return true;
        } finally {
            gate.readLock().unlock();
        }
    }

    void completeSend(
            long payloadLength,
            long intendedSendTime,
            long sendTime,
            LongSupplier nanoClock,
            Throwable failure) {
        try {
            gate.readLock().lock();
            try {
                if (accepting) {
                    if (failure == null) {
                        stats.recordProducerSuccess(
                                payloadLength, intendedSendTime, sendTime, nanoClock.getAsLong());
                    } else {
                        stats.recordProducerFailure();
                    }
                }
            } finally {
                gate.readLock().unlock();
            }
            if (failure != null && accepting) {
                log.warn("Write error on message", failure);
            }
        } catch (Throwable t) {
            unexpectedFailure.accept(t);
        } finally {
            abortSend();
        }
    }

    void completeProbe(Throwable failure) {
        try {
            gate.readLock().lock();
            try {
                if (accepting && failure == null) {
                    stats.recordMessageSent();
                }
            } finally {
                gate.readLock().unlock();
            }
        } catch (Throwable t) {
            unexpectedFailure.accept(t);
        } finally {
            abortSend();
        }
    }

    void abortSend() {
        sends.decrementAndGet();
        signalProgress();
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
        gate.readLock().lock();
        try {
            if (!accepting) {
                return;
            }
            receives.incrementAndGet();
            try {
                long latencyMicros =
                        TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis() - publishTimestamp);
                stats.recordMessageReceived(size, latencyMicros);
            } catch (Throwable t) {
                receives.decrementAndGet();
                signalProgress();
                unexpectedFailure.accept(t);
                throw t;
            }
        } finally {
            gate.readLock().unlock();
        }

        try {
            progress.lock();
            try {
                while (paused && accepting) {
                    progressChanged.await();
                }
            } finally {
                progress.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            receives.decrementAndGet();
            signalProgress();
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
        if (remaining <= 0 || !gate.writeLock().tryLock(remaining, TimeUnit.NANOSECONDS)) {
            return false;
        }
        try {
            return true;
        } finally {
            gate.writeLock().unlock();
        }
    }

    boolean awaitQuiescence(long deadlineNanos) throws InterruptedException {
        progress.lockInterruptibly();
        try {
            long remaining = deadlineNanos - System.nanoTime();
            while (sends.get() != 0 || receives.get() != 0) {
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

    private void signalProgress() {
        progress.lock();
        try {
            progressChanged.signalAll();
        } finally {
            progress.unlock();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RunCallbacks.class);
}
