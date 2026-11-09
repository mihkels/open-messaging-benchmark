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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class UniformRateLimiterTest {

    @Test
    void getOpsPerSec() {
        assertThat(new UniformRateLimiter(1000).getOpsPerSec()).isEqualTo(1000.0d);
    }

    @Test
    void getIntervalNs() {
        assertThat(new UniformRateLimiter(1000).getIntervalNs()).isEqualTo(SECONDS.toNanos(1) / 1000);
    }

    @Test
    void acquireSlowSingleThread() {
        LongSupplier mockClock = mock(LongSupplier.class);
        when(mockClock.getAsLong()).thenReturn(SECONDS.toNanos(2));
        UniformRateLimiter rateLimiter = new UniformRateLimiter(1000, mockClock);
        assertThat(rateLimiter.acquire()).isEqualTo(2000000000L);
        assertThat(rateLimiter.acquire()).isEqualTo(2001000000L);
        assertThat(rateLimiter.acquire()).isEqualTo(2002000000L);
    }

    @Test
    void setRateRaisesTheRateFromTheTimeOfTheChange() {
        AtomicLong clock = new AtomicLong(SECONDS.toNanos(2));
        UniformRateLimiter rateLimiter = new UniformRateLimiter(2, clock::get);
        assertThat(rateLimiter.acquire()).isEqualTo(SECONDS.toNanos(2));
        assertThat(rateLimiter.acquire()).isEqualTo(MILLISECONDS.toNanos(2500));

        clock.set(MILLISECONDS.toNanos(2100));
        rateLimiter.setRate(1000);

        assertThat(rateLimiter.getOpsPerSec()).isEqualTo(1000.0d);
        assertThat(rateLimiter.getIntervalNs()).isEqualTo(MILLISECONDS.toNanos(1));
        assertThat(rateLimiter.acquire()).isEqualTo(MILLISECONDS.toNanos(2100));
        assertThat(rateLimiter.acquire()).isEqualTo(MILLISECONDS.toNanos(2101));
        assertThat(rateLimiter.acquire()).isEqualTo(MILLISECONDS.toNanos(2102));
    }

    @Test
    void setRateLowersTheRateFromTheTimeOfTheChange() {
        AtomicLong clock = new AtomicLong(SECONDS.toNanos(2));
        UniformRateLimiter rateLimiter = new UniformRateLimiter(1000, clock::get);
        for (int i = 0; i < 100; i++) {
            rateLimiter.acquire();
        }

        clock.set(SECONDS.toNanos(3));
        rateLimiter.setRate(4);

        assertThat(rateLimiter.getIntervalNs()).isEqualTo(MILLISECONDS.toNanos(250));
        assertThat(rateLimiter.acquire()).isEqualTo(SECONDS.toNanos(3));
        assertThat(rateLimiter.acquire()).isEqualTo(MILLISECONDS.toNanos(3250));
        assertThat(rateLimiter.acquire()).isEqualTo(MILLISECONDS.toNanos(3500));
    }

    @Test
    void setRateBeforeTheFirstAcquireStartsOnTheFirstAcquire() {
        AtomicLong clock = new AtomicLong(SECONDS.toNanos(1));
        UniformRateLimiter rateLimiter = new UniformRateLimiter(1, clock::get);
        rateLimiter.setRate(10);

        clock.set(SECONDS.toNanos(5));
        assertThat(rateLimiter.acquire()).isEqualTo(SECONDS.toNanos(5));
        assertThat(rateLimiter.acquire()).isEqualTo(MILLISECONDS.toNanos(5100));
    }

    @Test
    void setRateRejectsInvalidRatesAndKeepsTheCurrentOne() {
        UniformRateLimiter rateLimiter = new UniformRateLimiter(1000);
        assertThatCode(() -> rateLimiter.setRate(0.0)).isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> rateLimiter.setRate(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(rateLimiter.getOpsPerSec()).isEqualTo(1000.0d);
    }

    @Test
    void interruptibleSleepNs() throws InterruptedException {
        long start = System.nanoTime();
        long expectedEnd = start + MILLISECONDS.toNanos(100);
        UniformRateLimiter.interruptibleSleepNs(expectedEnd);
        long end = System.nanoTime();
        assertThat(end).isGreaterThanOrEqualTo(expectedEnd);
    }

    @Test
    void rateWaitExitsPromptlyWhenInterrupted() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread waiter =
                Thread.ofVirtual()
                        .start(() -> {
                            started.countDown();
                            try {
                                UniformRateLimiter.interruptibleSleepNs(System.nanoTime() + SECONDS.toNanos(30));
                            } catch (InterruptedException _) {
                                interrupted.set(true);
                            }
                        });
        assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();
        waiter.interrupt();
        waiter.join(1000);
        assertThat(waiter.isAlive()).isFalse();
        assertThat(interrupted).isTrue();
        assertThatThrownBy(
                        () -> {
                            Thread.currentThread().interrupt();
                            UniformRateLimiter.interruptibleSleepNs(System.nanoTime());
                        })
                .isInstanceOf(InterruptedException.class);
    }

    @Test
    void cinitExceptions() {
        assertThatCode(() -> new UniformRateLimiter(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> new UniformRateLimiter(1.0d / 0.0d))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> new UniformRateLimiter(-0.1)).isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> new UniformRateLimiter(0.0)).isInstanceOf(IllegalArgumentException.class);
    }
}
