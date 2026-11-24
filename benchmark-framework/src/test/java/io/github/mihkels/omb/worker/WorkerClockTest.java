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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.mihkels.omb.worker.commands.WorkerHealth;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class WorkerClockTest {
    // Request and response delays in micros per round trip; the fastest one is asymmetric
    private static final long[][] DELAYS = {
        {4_000, 900}, {150, 450}, {2_500, 2_500}, {700, 90}, {3_000, 100}
    };

    // True time in micros; the coordinator's clock reads it directly
    private final AtomicLong now = new AtomicLong(1_700_000_000_000_000L);

    private Worker remote(String id, long offsetMicros) {
        Worker worker = mock(Worker.class);
        when(worker.id()).thenReturn(id);
        AtomicInteger call = new AtomicInteger();
        when(worker.health())
                .thenAnswer(invocation -> {
                    long[] delay = DELAYS[call.getAndIncrement() % DELAYS.length];
                    long reading = now.addAndGet(delay[0]) + offsetMicros;
                    now.addAndGet(delay[1]);
                    return WorkerHealth.RUNNING.withClockMicros(reading);
                });
        return worker;
    }

    @Test
    void estimateIsWithinHalfTheRoundTripOfTheInjectedOffset() throws Exception {
        Worker ahead = remote("http://w1", 20_000);
        Worker behind = remote("http://w2", -3_000);
        try (var ensemble = new DistributedWorkersEnsemble(List.of(ahead, behind), false)) {
            List<WorkerClock> clocks = WorkerClockProbe.probe(ensemble, now::get);

            assertThat(clocks).hasSize(2);
            WorkerClock first = clocks.get(0);
            WorkerClock second = clocks.get(1);
            // The lowest round trip is 150 + 450 micros
            assertThat(first.rttMs()).isEqualTo(0.6);
            assertThat(first.offsetMs()).isCloseTo(20.0, within(first.rttMs() / 2));
            assertThat(second.offsetMs()).isCloseTo(-3.0, within(second.rttMs() / 2));
            assertThat(List.of(first.role(), second.role()))
                    .containsExactlyInAnyOrder("producer", "consumer");
            assertThat(first.driftMs()).isNull();
            assertThat(WorkerClock.spreadMs(clocks)).isCloseTo(23.0, within(first.rttMs()));
        }
    }

    @Test
    void localRunRecordsZero() {
        Worker local = mock(Worker.class);
        when(local.id()).thenReturn("local-worker");

        List<WorkerClock> clocks = WorkerClockProbe.probe(local);

        assertThat(clocks).containsExactly(new WorkerClock("local-worker", "local", 0, 0, 0.0));
        assertThat(WorkerClock.spreadMs(WorkerClock.withDrift(clocks, WorkerClockProbe.probe(local))))
                .isZero();
    }

    @Test
    void leavesOutWorkersThatCannotBeProbed() throws Exception {
        Worker old = mock(Worker.class);
        when(old.id()).thenReturn("http://old");
        when(old.health()).thenReturn(WorkerHealth.RUNNING);
        Worker down = mock(Worker.class);
        when(down.id()).thenReturn("http://down");
        when(down.health()).thenThrow(new IllegalStateException("Connection refused"));
        try (var ensemble = new DistributedWorkersEnsemble(List.of(old, down), false)) {
            assertThat(WorkerClockProbe.probe(ensemble, now::get)).isEmpty();
        }
    }

    @Test
    void driftAddsToTheSpreadAtTheEnd() {
        List<WorkerClock> start =
                List.of(
                        new WorkerClock("p", "producer", 1, 0.2, null),
                        new WorkerClock("c", "consumer", 3, 0.2, null),
                        new WorkerClock("gone", "consumer", 0, 0.2, null));
        List<WorkerClock> end =
                List.of(
                        new WorkerClock("p", "producer", 1, 0.2, null),
                        new WorkerClock("c", "consumer", 9, 0.2, null));

        List<WorkerClock> clocks = WorkerClock.withDrift(start, end);

        assertThat(clocks).extracting(WorkerClock::driftMs).containsExactly(0.0, 6.0, null);
        assertThat(WorkerClock.spreadMs(clocks)).isEqualTo(8.0);
    }
}
