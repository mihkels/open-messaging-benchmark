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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

class DistributedWorkersEnsembleTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void distributesTopicsSubscriptionsAndControlCallsByWorkerRole(boolean extraConsumers)
            throws Exception {
        List<Worker> workers = workers(6);
        int producerCount = extraConsumers ? 2 : 3;
        int consumerCount = 6 - producerCount;
        List<String> topics = List.of("a", "b", "c", "d", "e", "f", "g");
        TopicsInfo info = new TopicsInfo(7, 3);
        when(workers.getFirst().createTopics(info)).thenReturn(topics);
        ConsumerAssignment consumers = new ConsumerAssignment();
        consumers.topicsSubscriptions =
                topics.stream().map(topic -> new TopicSubscription(topic, "subscription")).toList();
        ProducerWorkAssignment load =
                new ProducerWorkAssignment(
                        List.of(new byte[] {1, 2}, new byte[] {3}),
                        120,
                        KeyDistributorType.NO_KEY,
                        new int[] {2, 1});

        try (var ensemble = new DistributedWorkersEnsemble(workers, extraConsumers)) {
            assertThat(ensemble.createTopics(info)).containsExactlyElementsOf(topics);
            ensemble.initializeDriver(Path.of("driver.yaml"));
            ensemble.createProducers(topics);
            ensemble.createConsumers(consumers);
            ensemble.probeProducers();
            ensemble.startLoad(load);
            ensemble.adjustPublishRate(60);
            ensemble.pauseConsumers();
            ensemble.resumeConsumers();

            for (int i = 0; i < workers.size(); i++) {
                Worker worker = workers.get(i);
                verify(worker).initializeDriver(Path.of("driver.yaml"));
                if (i != 0) {
                    verify(worker, never()).createTopics(any());
                }
                if (i < producerCount) {
                    int slot = producerCount - 1 - i;
                    verify(worker).createProducers(roundRobin(topics, slot, producerCount));
                    verify(worker).probeProducers();
                    ArgumentCaptor<ProducerWorkAssignment> captured =
                            ArgumentCaptor.forClass(ProducerWorkAssignment.class);
                    verify(worker).startLoad(captured.capture());
                    assertThat(captured.getValue())
                            .usingRecursiveComparison()
                            .isEqualTo(load.withPublishRate(120.0 / producerCount));
                    verify(worker).adjustPublishRate(60.0 / producerCount);
                    verify(worker, never()).createConsumers(any());
                    verify(worker, never()).pauseConsumers();
                    verify(worker, never()).resumeConsumers();
                } else {
                    int slot = workers.size() - 1 - i;
                    ArgumentCaptor<ConsumerAssignment> captured =
                            ArgumentCaptor.forClass(ConsumerAssignment.class);
                    verify(worker).createConsumers(captured.capture());
                    assertThat(captured.getValue().topicsSubscriptions)
                            .containsExactlyElementsOf(
                                    roundRobin(consumers.topicsSubscriptions, slot, consumerCount));
                    verify(worker).pauseConsumers();
                    verify(worker).resumeConsumers();
                    verify(worker, never()).createProducers(any());
                    verify(worker, never()).probeProducers();
                    verify(worker, never()).startLoad(any());
                    verify(worker, never()).adjustPublishRate(anyDouble());
                }
            }
        }
    }

    @Test
    void dividesRateByUsedProducerWorkersAndRecalculatesForTheNextWorkload() throws Exception {
        List<Worker> workers = workers(6);
        try (var ensemble = new DistributedWorkersEnsemble(workers, false)) {
            ensemble.createProducers(List.of("only-topic"));
            verify(workers.get(2)).createProducers(List.of("only-topic"));
            verify(workers.get(1)).createProducers(List.of());
            verify(workers.get(0)).createProducers(List.of());
            ensemble.startLoad(
                    new ProducerWorkAssignment(List.of(new byte[] {1}), 90, KeyDistributorType.NO_KEY));
            ensemble.adjustPublishRate(60);
            ensemble.createProducers(List.of("a", "b", "c"));
            ensemble.adjustPublishRate(120);
            for (Worker worker : workers.subList(0, 3)) {
                ArgumentCaptor<ProducerWorkAssignment> load =
                        ArgumentCaptor.forClass(ProducerWorkAssignment.class);
                verify(worker).startLoad(load.capture());
                assertThat(load.getValue().publishRate()).isEqualTo(90);
                verify(worker).adjustPublishRate(60);
                verify(worker).adjustPublishRate(40);
            }
        }
    }

    @Test
    void aggregatesCountersAndEveryCumulativeHistogramWithoutMutatingWorkers() throws Exception {
        List<Worker> workers = workers(2);
        CumulativeLatencies first = latencies(100);
        CumulativeLatencies second = latencies(200);
        when(workers.get(0).getCountersStats()).thenReturn(new CountersStats(10, 2, 7));
        when(workers.get(1).getCountersStats()).thenReturn(new CountersStats(20, 3, 11));
        when(workers.get(0).getCumulativeLatencies()).thenReturn(first);
        when(workers.get(1).getCumulativeLatencies()).thenReturn(second);
        try (var ensemble = new DistributedWorkersEnsemble(workers, false)) {
            assertThat(ensemble.getCountersStats()).isEqualTo(new CountersStats(30, 5, 18));
            CumulativeLatencies result = ensemble.getCumulativeLatencies();
            assertThat(result.publishLatency.getCountAtValue(100)).isEqualTo(1);
            assertThat(result.publishLatency.getCountAtValue(200)).isEqualTo(1);
            assertThat(result.publishDelayLatency.getCountAtValue(101)).isEqualTo(1);
            assertThat(result.publishDelayLatency.getCountAtValue(201)).isEqualTo(1);
            assertThat(result.endToEndLatency.getCountAtValue(102)).isEqualTo(1);
            assertThat(result.endToEndLatency.getCountAtValue(202)).isEqualTo(1);
            result.publishLatency.reset();
            result.publishDelayLatency.reset();
            result.endToEndLatency.reset();
            for (CumulativeLatencies original : List.of(first, second)) {
                assertThat(original.publishLatency.getTotalCount()).isEqualTo(1);
                assertThat(original.publishDelayLatency.getTotalCount()).isEqualTo(1);
                assertThat(original.endToEndLatency.getTotalCount()).isEqualTo(1);
            }
            ensemble.resetStats();
            for (Worker worker : workers) {
                verify(worker).resetStats();
            }
        }
    }

    @Test
    void initializesAndResetsEveryWorkerDespiteFailures() throws Exception {
        List<Worker> workers = workers(3);
        Path config = Path.of("driver.yaml");
        IOException first = new IOException("first initialization");
        IOException last = new IOException("last initialization");
        doThrow(first).when(workers.get(0)).initializeDriver(config);
        doThrow(last).when(workers.get(2)).initializeDriver(config);
        IllegalStateException resetFailure = new IllegalStateException("reset failed");
        doThrow(resetFailure).when(workers.get(0)).resetStats();
        try (var ensemble = new DistributedWorkersEnsemble(workers, false)) {
            Throwable initialization = catchThrowable(() -> ensemble.initializeDriver(config));
            assertThat(initialization).isSameAs(first);
            assertThat(initialization.getSuppressed()).containsExactly(last);
            assertThat(catchThrowable(ensemble::resetStats)).isSameAs(resetFailure);
            for (Worker worker : workers) {
                verify(worker).initializeDriver(config);
                verify(worker).resetStats();
            }
        }
    }

    @Test
    void rejectsFewerThanTwoWorkers() {
        assertThatThrownBy(() -> new DistributedWorkersEnsemble(List.of(), false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("At least two workers are required");
        assertThatThrownBy(() -> new DistributedWorkersEnsemble(workers(1), true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static List<Worker> workers(int count) {
        return IntStream.range(0, count).mapToObj(i -> mock(Worker.class, "worker-" + i)).toList();
    }

    private static <T> List<T> roundRobin(List<T> values, int slot, int workerCount) {
        return IntStream.range(0, values.size())
                .filter(i -> i % workerCount == slot)
                .mapToObj(values::get)
                .toList();
    }

    private static CumulativeLatencies latencies(long value) {
        CumulativeLatencies latencies = new CumulativeLatencies();
        latencies.publishLatency.recordValue(value);
        latencies.publishDelayLatency.recordValue(value + 1);
        latencies.endToEndLatency.recordValue(value + 2);
        return latencies;
    }

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
