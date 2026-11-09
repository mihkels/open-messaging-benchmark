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
package io.openmessaging.benchmark.utils;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * Provides a next operation time for rate limited operation streams.<br>
 * The rate limiter is thread safe and can be shared by all threads. Its rate can be changed with
 * {@link #setRate(double)} while other threads are in {@link #acquire()}.
 */
public final class UniformRateLimiter {

    private static final double ONE_SEC_IN_NS = (double) SECONDS.toNanos(1);
    private static final long NOT_STARTED = Long.MIN_VALUE;

    private final LongSupplier nanoClock;
    private final AtomicReference<Schedule> schedule;

    UniformRateLimiter(final double opsPerSec, LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
        this.schedule = new AtomicReference<>(new Schedule(opsPerSec, NOT_STARTED));
    }

    public UniformRateLimiter(final double opsPerSec) {
        this(opsPerSec, System::nanoTime);
    }

    public double getOpsPerSec() {
        return current().opsPerSec;
    }

    public long getIntervalNs() {
        return current().intervalNs;
    }

    /**
     * Changes the rate. The schedule is re-based so that operation {@code n} after the change is due
     * {@code n} new intervals after the change, or after the first {@link #acquire()} if none has
     * happened yet. A thread that took its slot just before the change keeps that slot.
     *
     * @param opsPerSec the new rate, which must be finite and greater than 0
     */
    public void setRate(final double opsPerSec) {
        long startNs = current().start == NOT_STARTED ? NOT_STARTED : nanoClock.getAsLong();
        schedule.set(new Schedule(opsPerSec, startNs));
    }

    public long acquire() {
        return current().acquire(nanoClock);
    }

    private Schedule current() {
        return Objects.requireNonNull(schedule.get());
    }

    public static void interruptibleSleepNs(final long intendedTime) throws InterruptedException {
        long sleepNs;
        while ((sleepNs = (intendedTime - System.nanoTime())) > 0) {
            if (Thread.interrupted()) {
                throw new InterruptedException("Interrupted while waiting for the next send");
            }
            LockSupport.parkNanos(sleepNs);
        }
        if (Thread.interrupted()) {
            throw new InterruptedException("Interrupted while waiting for the next send");
        }
    }

    /**
     * One rate and the schedule that started with it. Swapping the whole object keeps the start,
     * interval and operation index consistent with each other without a lock.
     */
    private static final class Schedule {
        private static final AtomicLongFieldUpdater<Schedule> INDEX_UPDATER =
                AtomicLongFieldUpdater.newUpdater(Schedule.class, "index");
        private static final AtomicLongFieldUpdater<Schedule> START_UPDATER =
                AtomicLongFieldUpdater.newUpdater(Schedule.class, "start");

        private final double opsPerSec;
        private final long intervalNs;
        private volatile long start;

        // Updated only through INDEX_UPDATER
        @SuppressWarnings("java:S1068")
        private volatile long index;

        private Schedule(double opsPerSec, long start) {
            if (Double.isNaN(opsPerSec) || Double.isInfinite(opsPerSec)) {
                throw new IllegalArgumentException("opsPerSec cannot be Nan or Infinite");
            }
            if (opsPerSec <= 0) {
                throw new IllegalArgumentException("opsPerSec must be greater then 0");
            }
            this.opsPerSec = opsPerSec;
            this.intervalNs = Math.round(ONE_SEC_IN_NS / opsPerSec);
            this.start = start;
        }

        private long acquire(LongSupplier nanoClock) {
            final long currOpIndex = INDEX_UPDATER.getAndIncrement(this);
            long startNs = this.start;
            if (startNs == NOT_STARTED) {
                startNs = nanoClock.getAsLong();
                if (!START_UPDATER.compareAndSet(this, NOT_STARTED, startNs)) {
                    startNs = this.start;
                }
            }
            return startNs + currOpIndex * intervalNs;
        }
    }
}
