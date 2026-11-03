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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.worker.commands.PeriodStats;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DistributedWorkersEnsembleTest {

    @Test
    void aggregatesSnapshotsFromConcurrentlyPolledWorkers() throws Exception {
        Worker first = mock(Worker.class);
        Worker second = mock(Worker.class);
        Histogram firstHistogram = new Histogram(5);
        firstHistogram.recordValue(100);
        Histogram secondHistogram = new Histogram(5);
        secondHistogram.recordValue(200);
        secondHistogram.recordValue(300);
        PeriodStats firstStats =
                new PeriodStats(1, 0, 10, 1, 10, 1, 0, 1, firstHistogram, firstHistogram, firstHistogram);
        PeriodStats secondStats =
                new PeriodStats(
                        2, 1, 20, 2, 20, 2, 1, 2, secondHistogram, secondHistogram, secondHistogram);
        CountDownLatch started = new CountDownLatch(2);
        when(first.getPeriodStats())
                .thenAnswer(
                        invocation -> {
                            started.countDown();
                            if (!started.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Workers were not polled concurrently");
                            }
                            return firstStats;
                        });
        when(second.getPeriodStats())
                .thenAnswer(
                        invocation -> {
                            started.countDown();
                            if (!started.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Workers were not polled concurrently");
                            }
                            return secondStats;
                        });

        try (var ensemble = new DistributedWorkersEnsemble(List.of(first, second), false)) {
            PeriodStats result = ensemble.getPeriodStats();
            assertThat(result).isEqualTo(firstStats.plus(secondStats));
            assertThat(result.publishLatency().getTotalCount()).isEqualTo(3);
            assertThat(result.publishDelayLatency().getTotalCount()).isEqualTo(3);
            assertThat(result.endToEndLatency().getTotalCount()).isEqualTo(3);
            result.publishLatency().reset();
            assertThat(firstStats.publishLatency().getTotalCount()).isEqualTo(1);
            assertThat(secondStats.publishLatency().getTotalCount()).isEqualTo(2);
            assertThat(result.publishLatency().getTotalCount()).isEqualTo(3);
        }
    }

    @Test
    void reportsEveryStatisticsPollingFailure() throws Exception {
        Worker first = mock(Worker.class);
        Worker second = mock(Worker.class);
        var firstFailure = new IllegalStateException("first statistics failure");
        var secondFailure = new IllegalStateException("second statistics failure");
        when(first.getPeriodStats()).thenThrow(firstFailure);
        when(second.getPeriodStats()).thenThrow(secondFailure);

        try (var ensemble = new DistributedWorkersEnsemble(List.of(first, second), false)) {
            Throwable failure = catchThrowable(ensemble::getPeriodStats);
            assertThat(failure).isSameAs(firstFailure);
            assertThat(failure.getSuppressed()).containsExactly(secondFailure);
            verify(first).getPeriodStats();
            verify(second).getPeriodStats();
        }
    }

    @Test
    void attemptsEveryWorkerAndReportsStopAndCloseFailures() throws Exception {
        Worker first = mock(Worker.class);
        Worker second = mock(Worker.class);
        doThrow(new IllegalStateException("first stop")).when(first).stopAll();
        doThrow(new IllegalStateException("second stop")).when(second).stopAll();
        doThrow(new IOException("first close")).when(first).close();
        doThrow(new IOException("second close")).when(second).close();
        DistributedWorkersEnsemble ensemble =
                new DistributedWorkersEnsemble(List.of(first, second), false);

        Throwable stopFailure = catchThrowable(ensemble::stopAll);
        assertThat(stopFailure.getCause().getMessage()).isEqualTo("first stop");
        assertThat(stopFailure.getCause().getSuppressed())
                .singleElement()
                .extracting(Throwable::getMessage)
                .isEqualTo("second stop");
        verify(first).stopAll();
        verify(second).stopAll();

        Throwable closeFailure = catchThrowable(ensemble::close);
        assertThat(closeFailure.getCause().getMessage()).isEqualTo("first close");
        assertThat(closeFailure.getCause().getSuppressed())
                .singleElement()
                .extracting(Throwable::getMessage)
                .isEqualTo("second close");
        verify(first).close();
        verify(second).close();
    }

    @ParameterizedTest
    @MethodSource("producerCountExpectations")
    void getNumberOfProducerWorkers(int workerCount, boolean extraConsumers, int expected) {
        List<Worker> workers = mock(List.class);
        when(workers.size()).thenReturn(workerCount);
        assertThat(DistributedWorkersEnsemble.getNumberOfProducerWorkers(workers, extraConsumers))
                .isEqualTo(expected);
    }

    private static Stream<Arguments> producerCountExpectations() {
        return Stream.of(
                Arguments.of(2, true, 1),
                Arguments.of(3, true, 1),
                Arguments.of(4, true, 2),
                Arguments.of(5, true, 2),
                Arguments.of(6, true, 2),
                Arguments.of(7, true, 3),
                Arguments.of(8, true, 3),
                Arguments.of(9, true, 3),
                Arguments.of(2, false, 1),
                Arguments.of(3, false, 1),
                Arguments.of(4, false, 2),
                Arguments.of(5, false, 2),
                Arguments.of(6, false, 3),
                Arguments.of(7, false, 3),
                Arguments.of(8, false, 4),
                Arguments.of(9, false, 4));
    }
}
