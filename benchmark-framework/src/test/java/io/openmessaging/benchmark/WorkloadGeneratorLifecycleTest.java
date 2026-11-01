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
package io.openmessaging.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

class WorkloadGeneratorLifecycleTest {
    @TempDir Path directory;

    @Test
    void nonemptyPeriodSnapshotsPreserveResultValuesAndOutputShape() throws Exception {
        Histogram publish = histogram(1_000, 3_000);
        Histogram delay = histogram(100, 500);
        Histogram endToEnd = histogram(4_000, 8_000);
        PeriodStats snapshot =
                spy(new PeriodStats(10, 2, 100, 8, 80, 10, 2, 8, publish, delay, endToEnd));
        CumulativeLatencies cumulative = new CumulativeLatencies();
        cumulative.publishLatency = publish.copy();
        cumulative.publishDelayLatency = delay.copy();
        cumulative.endToEndLatency = endToEnd.copy();
        publish.recordValue(9_000);
        delay.recordValue(900);
        endToEnd.recordValue(10_000);

        Worker worker = mock(Worker.class);
        when(worker.getPeriodStats()).thenReturn(snapshot);
        when(worker.getCumulativeLatencies()).thenReturn(cumulative);
        Workload workload = workload();
        workload.subscriptionsPerTopic = 2;
        TestResult result;
        try (WorkloadGenerator generator =
                new WorkloadGenerator(
                        "driver",
                        workload,
                        worker,
                        Duration.ofMillis(10),
                        Duration.ofMillis(10),
                        Duration.ofSeconds(2))) {
            result = generator.printAndCollectStats(1, TimeUnit.MILLISECONDS);
        }

        verify(worker, times(1)).getPeriodStats();
        verify(snapshot, times(1)).publishLatency();
        verify(snapshot, times(1)).publishDelayLatency();
        verify(snapshot, times(1)).endToEndLatency();
        assertThat(result.backlog).containsExactly(12L);
        assertThat(result.publishLatencyAvg).containsExactly(2.0);
        assertThat(result.publishLatency50pct).containsExactly(1.0);
        assertThat(result.publishLatency75pct).containsExactly(3.0);
        assertThat(result.publishLatency95pct).containsExactly(3.0);
        assertThat(result.publishLatency99pct).containsExactly(3.0);
        assertThat(result.publishLatency999pct).containsExactly(3.0);
        assertThat(result.publishLatency9999pct).containsExactly(3.0);
        assertThat(result.publishLatencyMax).containsExactly(3.0);
        assertThat(result.publishDelayLatencyAvg).containsExactly(300.0);
        assertThat(result.publishDelayLatency50pct).containsExactly(100L);
        assertThat(result.publishDelayLatency75pct).containsExactly(500L);
        assertThat(result.publishDelayLatency95pct).containsExactly(500L);
        assertThat(result.publishDelayLatency99pct).containsExactly(500L);
        assertThat(result.publishDelayLatency999pct).containsExactly(500L);
        assertThat(result.publishDelayLatency9999pct).containsExactly(500L);
        assertThat(result.publishDelayLatencyMax).containsExactly(500L);
        assertThat(result.endToEndLatencyAvg).containsExactly(6.0);
        assertThat(result.endToEndLatency50pct).containsExactly(4.0);
        assertThat(result.endToEndLatency75pct).containsExactly(8.0);
        assertThat(result.endToEndLatency95pct).containsExactly(8.0);
        assertThat(result.endToEndLatency99pct).containsExactly(8.0);
        assertThat(result.endToEndLatency999pct).containsExactly(8.0);
        assertThat(result.endToEndLatency9999pct).containsExactly(8.0);
        assertThat(result.endToEndLatencyMax).containsExactly(8.0);
        assertThat(result.aggregatedPublishLatencyAvg).isEqualTo(2.0);
        assertThat(result.aggregatedPublishDelayLatencyAvg).isEqualTo(300.0);
        assertThat(result.aggregatedEndToEndLatencyAvg).isEqualTo(6.0);

        Path driverConfig = directory.resolve("driver.yaml");
        Files.writeString(driverConfig, "name: driver\ndriverClass: fixture.Driver\n");
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.drivers = List.of(driverConfig.toString());
        arguments.output = directory.resolve("result.json").toString();
        WorkloadGenerator completed = mock(WorkloadGenerator.class);
        when(completed.run()).thenReturn(result);
        Benchmark.runWorkloads(
                arguments, Map.of("snapshot", workload), worker, (d, w, ignored) -> completed);

        JsonNode json = new ObjectMapper().readTree(Files.readString(Path.of(arguments.output)));
        assertThat(json.get("workload").stringValue()).isEqualTo("test");
        assertThat(json.get("driver").stringValue()).isEqualTo("driver");
        List<String> arrayFields =
                List.of(
                        "publishRate",
                        "publishErrorRate",
                        "consumeRate",
                        "backlog",
                        "publishLatencyAvg",
                        "publishLatency50pct",
                        "publishLatency75pct",
                        "publishLatency95pct",
                        "publishLatency99pct",
                        "publishLatency999pct",
                        "publishLatency9999pct",
                        "publishLatencyMax",
                        "publishDelayLatencyAvg",
                        "publishDelayLatency50pct",
                        "publishDelayLatency75pct",
                        "publishDelayLatency95pct",
                        "publishDelayLatency99pct",
                        "publishDelayLatency999pct",
                        "publishDelayLatency9999pct",
                        "publishDelayLatencyMax",
                        "endToEndLatencyAvg",
                        "endToEndLatency50pct",
                        "endToEndLatency75pct",
                        "endToEndLatency95pct",
                        "endToEndLatency99pct",
                        "endToEndLatency999pct",
                        "endToEndLatency9999pct",
                        "endToEndLatencyMax");
        for (String field : arrayFields) {
            assertThat(json.has(field)).as(field).isTrue();
            assertThat(json.get(field).isArray()).as(field).isTrue();
            assertThat(json.get(field).size()).as(field).isEqualTo(1);
            assertThat(json.get(field).get(0).isNumber()).as(field).isTrue();
        }
        assertThat(json.get("backlog").get(0).longValue()).isEqualTo(12);
        assertThat(json.get("publishLatencyAvg").get(0).doubleValue()).isEqualTo(2.0);
        assertThat(json.get("publishDelayLatency50pct").get(0).isIntegralNumber()).isTrue();
        assertThat(json.get("publishDelayLatency50pct").get(0).longValue()).isEqualTo(100);
        assertThat(json.get("endToEndLatency99pct").get(0).doubleValue()).isEqualTo(8.0);
        assertThat(json.get("aggregatedPublishLatencyQuantiles").isObject()).isTrue();
        assertThat(json.get("aggregatedPublishDelayLatencyQuantiles").isObject()).isTrue();
        assertThat(json.get("aggregatedEndToEndLatencyQuantiles").isObject()).isTrue();
        assertThat(json.get("aggregatedPublishLatencyQuantiles").size()).isPositive();
        assertThat(json.get("aggregatedPublishDelayLatencyQuantiles").size()).isPositive();
        assertThat(json.get("aggregatedEndToEndLatencyQuantiles").size()).isPositive();
    }

    private static Histogram histogram(long first, long second) {
        Histogram histogram = new Histogram(5);
        histogram.recordValue(first);
        histogram.recordValue(second);
        return histogram;
    }

    @Test
    void propagatesReadinessFailureAndStillStopsWorker() throws Exception {
        Worker worker = worker();
        doThrow(new IOException("probe rejected")).when(worker).probeProducers();
        try (WorkloadGenerator generator = generator(workload(), worker, Duration.ofSeconds(2))) {
            assertThatThrownBy(generator::run).hasMessage("probe rejected");
            verify(worker, never()).startLoad(any());
        }
        verify(worker).stopAll();
    }

    @Test
    void backlogControllerFailureWakesTheRunImmediately() throws Exception {
        Worker worker = worker();
        doThrow(new IOException("pause failed")).when(worker).pauseConsumers();
        Workload workload = workload();
        workload.consumerBacklogSizeGB = 1;
        try (WorkloadGenerator generator = generator(workload, worker, Duration.ofSeconds(2))) {
            long start = System.nanoTime();
            assertThatThrownBy(generator::run).hasRootCauseMessage("pause failed");
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
        }
        verify(worker).stopAll();
    }

    @Test
    void sustainableRateControllerFailureIsNotSwallowed() throws Exception {
        Worker worker = worker();
        when(worker.getCountersStats())
                .thenReturn(new CountersStats())
                .thenThrow(new IOException("rate controller failed"));
        Workload workload = workload();
        workload.producerRate = 0;
        try (WorkloadGenerator generator = generator(workload, worker, Duration.ofSeconds(2))) {
            assertThatThrownBy(generator::run).hasRootCauseMessage("rate controller failed");
        }
        verify(worker).stopAll();
    }

    @Test
    void propagatesProducerFailureFromTheWorkerBoundary() throws Exception {
        Worker worker = worker();
        when(worker.getPeriodStats()).thenThrow(new IllegalStateException("producer task failed"));
        WorkloadGenerator generator =
                new WorkloadGenerator(
                        "driver",
                        workload(),
                        worker,
                        Duration.ofMillis(10),
                        Duration.ofMillis(10),
                        Duration.ofSeconds(2));
        try (generator) {
            assertThatThrownBy(generator::run).hasMessage("producer task failed");
        }
        verify(worker).stopAll();
    }

    @Test
    void closeWaitsForRealControllerExitAndAttemptsWorkerStopOnTimeout() throws Exception {
        Worker worker = worker();
        CountDownLatch controllerEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<Boolean> controllerDaemon = new AtomicReference<>();
        when(worker.getCountersStats())
                .thenAnswer(
                        ignored -> {
                            if (calls.getAndIncrement() > 0) {
                                controllerDaemon.set(Thread.currentThread().isDaemon());
                                controllerEntered.countDown();
                                awaitIgnoringInterrupts(release);
                            }
                            return new CountersStats();
                        });
        doThrow(new IllegalStateException("worker stop failed")).when(worker).stopAll();
        Workload workload = workload();
        workload.producerRate = 0;
        WorkloadGenerator generator = generator(workload, worker, Duration.ofMillis(150));
        AtomicReference<Throwable> runFailure = new AtomicReference<>();
        Thread run =
                Thread.ofVirtual()
                        .start(
                                () -> {
                                    try {
                                        generator.run();
                                    } catch (Throwable t) {
                                        runFailure.set(t);
                                    }
                                });
        try {
            assertThat(controllerEntered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(controllerDaemon.get()).isTrue();
            Throwable failure = catchThrowable(generator::close);
            assertThat(messages(failure))
                    .contains("Workload controllers did not terminate", "worker stop failed");
            assertThat(generator.isQuiescentAfterClose()).isFalse();
            verify(worker, times(1)).stopAll();
            generator.close();
            verify(worker, times(1)).stopAll();
        } finally {
            release.countDown();
            run.join(2000);
        }
        assertThat(run.isAlive()).isFalse();
        assertThat(runFailure.get()).isNotNull();
    }

    private static Worker worker() throws Exception {
        Worker worker = mock(Worker.class);
        when(worker.createTopics(any())).thenReturn(List.of("topic"));
        when(worker.getCountersStats()).thenReturn(new CountersStats());
        return worker;
    }

    private static Workload workload() {
        Workload workload = new Workload();
        workload.name = "test";
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.producersPerTopic = 1;
        workload.subscriptionsPerTopic = 0;
        workload.messageSize = 8;
        workload.useRandomizedPayloads = true;
        workload.randomBytesRatio = 1;
        workload.randomizedPayloadPoolSize = 1;
        workload.producerRate = 1;
        workload.warmupDurationMinutes = 0;
        workload.testDurationMinutes = 1;
        return workload;
    }

    private static WorkloadGenerator generator(Workload workload, Worker worker, Duration timeout) {
        return new WorkloadGenerator(
                "driver", workload, worker, Duration.ofSeconds(10), Duration.ofMillis(10), timeout);
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static List<String> messages(Throwable failure) {
        List<String> result = new ArrayList<>();
        collectMessages(failure, result);
        return result;
    }

    private static void collectMessages(Throwable failure, List<String> result) {
        if (failure == null) {
            return;
        }
        result.add(failure.getMessage());
        collectMessages(failure.getCause(), result);
        for (Throwable suppressed : failure.getSuppressed()) {
            collectMessages(suppressed, result);
        }
    }
}
