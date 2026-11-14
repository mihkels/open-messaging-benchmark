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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.WorkerHealth;
import io.openmessaging.benchmark.worker.commands.WorkerHealth.DriverState;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class WorkerWatchdogTest {
    private static final Duration FAST = Duration.ofMillis(20);
    private static final Duration NEVER = Duration.ofHours(1);

    private static Worker worker(String id, WorkerHealth health) {
        Worker worker = mock(Worker.class);
        when(worker.id()).thenReturn(id);
        when(worker.health()).thenReturn(health);
        when(worker.getCountersStats()).thenReturn(new CountersStats(1, 0, 1));
        return worker;
    }

    @Test
    void abortsTheRunWhenADriverProcessExits() throws Exception {
        Worker healthy = worker("http://w1", WorkerHealth.RUNNING);
        Worker dead =
                worker("http://w2", new WorkerHealth(true, DriverState.EXITED, 137, "OOM killed"));
        try (var ensemble = new DistributedWorkersEnsemble(List.of(healthy, dead), false)) {
            var watchdog = new WorkerWatchdog(ensemble, () -> 0, FAST, NEVER);
            InterruptedException interrupted = sleepUntilInterrupted();
            Exception failure = watchdog.explain(interrupted);
            assertThat(failure)
                    .isInstanceOfSatisfying(
                            WorkerUnhealthyException.class,
                            e -> {
                                assertThat(e.workerId()).isEqualTo("http://w2");
                                assertThat(e.reason())
                                        .isEqualTo("driver process is EXITED with exit code 137: OOM killed");
                            })
                    .hasSuppressedException(interrupted);
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
            watchdog.close();
        }
    }

    @Test
    void abortsTheRunAfterRepeatedlyFailingToReachAWorker() throws Exception {
        Worker unreachable = mock(Worker.class);
        when(unreachable.id()).thenReturn("http://gone");
        when(unreachable.health()).thenThrow(new IllegalStateException("Connection refused"));
        var watchdog = new WorkerWatchdog(unreachable, () -> 0, FAST, NEVER);
        Exception failure = watchdog.explain(sleepUntilInterrupted());
        assertThat(failure)
                .isInstanceOf(WorkerUnhealthyException.class)
                .hasMessageContaining("http://gone")
                .hasMessageContaining("unreachable after " + WorkerWatchdog.MAX_MISSES + " health checks")
                .hasMessageContaining("Connection refused");
        watchdog.close();
    }

    @Test
    void reportsAFailureThatArrivesAfterTheRunReturnedOnClose() throws Exception {
        Worker dead = worker("local", new WorkerHealth(true, DriverState.NOT_STARTED, null, null));
        var watchdog = new WorkerWatchdog(dead, () -> 0, FAST, NEVER);
        sleepUntilInterrupted();
        Thread.currentThread().interrupt();
        assertThatThrownBy(watchdog::close)
                .isInstanceOf(WorkerUnhealthyException.class)
                .hasMessageContaining("driver process is NOT_STARTED");
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    @Test
    void leavesAHealthyRunAloneAndKeepsDumpingThreadsWhileItIsStuck() throws Exception {
        Worker healthy = worker("local", WorkerHealth.RUNNING);
        AtomicLong progress = new AtomicLong();
        try (var watchdog = new WorkerWatchdog(healthy, progress::get, FAST, Duration.ofMillis(50))) {
            Thread.sleep(300);
            progress.incrementAndGet();
            Thread.sleep(100);
            IllegalStateException own = new IllegalStateException("run failed by itself");
            assertThat(watchdog.explain(own)).isSameAs(own);
        }
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    private static InterruptedException sleepUntilInterrupted() {
        try {
            Thread.sleep(Duration.ofSeconds(30));
        } catch (InterruptedException e) {
            return e;
        }
        throw new AssertionError("Watchdog did not interrupt the run");
    }
}
