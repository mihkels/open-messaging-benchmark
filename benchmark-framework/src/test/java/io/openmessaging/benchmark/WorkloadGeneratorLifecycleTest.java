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
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
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
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.RunPhase;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
        when(worker.getCountersStats()).thenReturn(new CountersStats(9, 2, 8));
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
            generator.includeHistograms(true);
            result = generator.printAndCollectStats(1, TimeUnit.MILLISECONDS);
        }

        verify(worker, times(1)).getPeriodStats();
        verify(snapshot, times(1)).publishLatency();
        verify(snapshot, times(1)).publishDelayLatency();
        verify(snapshot, times(1)).endToEndLatency();
        assertCollectedResult(result);

        Path driverConfig = directory.resolve("driver.yaml");
        Files.writeString(driverConfig, "name: driver\ndriverClass: fixture.Driver\n");
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.drivers = List.of(driverConfig.toString());
        arguments.output = directory.resolve("result.json").toString();
        WorkloadGenerator completed = mock(WorkloadGenerator.class);
        when(completed.run()).thenReturn(result);
        Benchmark.runWorkloads(
                arguments, Map.of("snapshot", workload), worker, (d, w, ignored) -> completed);

        assertSerializedResult(Path.of(arguments.output));
    }

    private static void assertCollectedResult(TestResult result) {
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
        assertThat(result.summary.totalMessagesSent()).isEqualTo(9);
        assertThat(result.summary.totalErrors()).isEqualTo(2);
        assertThat(result.summary.maxBacklog()).isEqualTo(12);
        assertThat(result.histograms).containsOnlyKeys("publish", "publishDelay", "endToEnd");
    }

    private static void assertSerializedResult(Path output) throws Exception {
        JsonNode json = new ObjectMapper().readTree(Files.readString(output));
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
        assertThat(json.get("histograms").get("publish").isTextual()).isTrue();
    }

    private static Histogram histogram(long first, long second) {
        Histogram histogram = new Histogram(5);
        histogram.recordValue(first);
        histogram.recordValue(second);
        return histogram;
    }

    @Test
    void tagsWarmupAndMeasuredPeriodsAndSummarizesOnlyTheMeasuredOnes() throws Exception {
        Histogram empty = histogram(1_000, 1_000);
        Worker worker = mock(Worker.class);
        // 10 messages per period in warm-up, then 40; both periods last at least 10 ms
        when(worker.getPeriodStats())
                .thenReturn(
                        new PeriodStats(10, 0, 10, 10, 10, 10, 0, 10, empty, empty, empty),
                        new PeriodStats(40, 0, 40, 40, 40, 50, 0, 50, empty, empty, empty));
        CumulativeLatencies cumulative = new CumulativeLatencies();
        cumulative.publishLatency = empty.copy();
        cumulative.publishDelayLatency = empty.copy();
        cumulative.publishScheduleLatency = empty.copy();
        cumulative.endToEndLatency = empty.copy();
        when(worker.getCumulativeLatencies()).thenReturn(cumulative);
        when(worker.getCountersStats()).thenReturn(new CountersStats(40, 0, 40));
        Workload workload = workload();
        workload.subscriptionsPerTopic = 1;
        TestResult result = new TestResult();
        try (WorkloadGenerator generator =
                new WorkloadGenerator(
                        "driver",
                        workload,
                        worker,
                        Duration.ofMillis(10),
                        Duration.ofMillis(10),
                        Duration.ofSeconds(2))) {
            generator.enterPhase(RunPhase.WARMUP);
            generator.collect(result, 1, TimeUnit.NANOSECONDS, false);
            generator.enterPhase(RunPhase.MEASURE);
            generator.collect(result, 1, TimeUnit.NANOSECONDS, true);
        }

        assertThat(result.phase).containsExactly(RunPhase.WARMUP, RunPhase.MEASURE);
        assertThat(result.publishRate).hasSize(2);
        assertThat(result.summary.publishRateAvg()).isEqualTo(result.publishRate.get(1));
        assertThat(TestResult.measured(result.phase, result.publishRate))
                .containsExactly(result.publishRate.get(1));
        var order = inOrder(worker);
        order.verify(worker).setPhase(RunPhase.WARMUP);
        order.verify(worker).setPhase(RunPhase.MEASURE);
    }

    @Test
    void stagesRampHoldAndStepTheRateSentToWorkers() throws Exception {
        Worker worker = worker();
        List<Double> sentRates = Collections.synchronizedList(new ArrayList<>());
        doAnswer(
                        invocation -> {
                            sentRates.add(invocation.getArgument(0));
                            return null;
                        })
                .when(worker)
                .adjustPublishRate(anyDouble());
        Workload workload = workload();
        workload.producerRate = 0;
        workload.testDurationMinutes = 0;
        workload.stages =
                List.of(
                        new Workload.Stage(2, 100),
                        new Workload.Stage(2, 300),
                        new Workload.Stage(0, 50),
                        new Workload.Stage(1, 50));
        // Every clock read is one minute later: elapsed 1..5 minutes, one tick each
        AtomicLong minutes = new AtomicLong();
        TestResult result = new TestResult();
        try (WorkloadGenerator generator =
                new WorkloadGenerator(
                        "driver",
                        workload,
                        worker,
                        Duration.ofMillis(10),
                        Duration.ofMillis(10),
                        Duration.ofSeconds(2))) {
            generator.driveStages(() -> TimeUnit.MINUTES.toNanos(minutes.getAndIncrement()), 1);
            generator.collect(result, 1, TimeUnit.NANOSECONDS, true);
        }

        // Called without run(), so no load was started: 1m sends stage 0's 100, 2m starts the ramp
        // at 100 (unchanged, not sent), 3m is half-way, 4m steps to 50 and 5m ends the profile
        assertThat(sentRates).containsExactly(100.0, 200.0, 50.0);
        assertThat(result.stage).containsExactly(3);
        assertThat(result.targetRate).containsExactly(50.0);
        assertThat(result.summary.targetRate()).isEqualTo(50.0);
    }

    @Test
    void stagedRunStartsLoadAtTheFirstStageRate() throws Exception {
        Worker worker = worker();
        AtomicReference<ProducerWorkAssignment> assignment = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            assignment.set(invocation.getArgument(0));
                            started.countDown();
                            return null;
                        })
                .when(worker)
                .startLoad(any());
        Workload workload = workload();
        workload.producerRate = 0;
        workload.testDurationMinutes = 0;
        workload.stages = List.of(new Workload.Stage(1, 700), new Workload.Stage(1, 900));
        WorkloadGenerator generator = generator(workload, worker, Duration.ofSeconds(2));
        AtomicReference<Throwable> runFailure = new AtomicReference<>();
        Thread run =
                Thread.ofVirtual()
                        .start(() -> {
                            try {
                                generator.run();
                            } catch (Throwable t) {
                                runFailure.set(t);
                            }
                        });
        try {
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
        } finally {
            generator.close();
            run.join(2000);
        }
        assertThat(run.isAlive()).isFalse();
        // close() may land before or after the stage controller starts
        assertThat(runFailure.get())
                .isInstanceOfAny(InterruptedException.class, IllegalStateException.class);
        assertThat(assignment.get().publishRate()).isEqualTo(700.0);
    }

    @Test
    void consumerPauseStopsConsumersForTheWindowAndTimesTheDrain() throws Exception {
        Worker worker = worker();
        // One subscription, so backlog = sent - received: 10 before the pause, 5000 at resume, then
        // 4000 (above the 1000 floor) and 500 (drained)
        when(worker.getCountersStats())
                .thenReturn(
                        new CountersStats(100, 0, 90),
                        new CountersStats(6_000, 0, 1_000),
                        new CountersStats(7_000, 0, 3_000),
                        new CountersStats(8_000, 0, 7_500));
        Workload workload = pausedWorkload();
        // Every clock read is one minute later
        AtomicLong reads = new AtomicLong();
        List<Long> callMinutes = Collections.synchronizedList(new ArrayList<>());
        TestResult result = new TestResult();
        try (WorkloadGenerator generator =
                new WorkloadGenerator(
                        "driver",
                        workload,
                        worker,
                        Duration.ofMillis(10),
                        Duration.ofMillis(10),
                        Duration.ofSeconds(2))) {
            // Record one period in each phase the controller passes through
            doAnswer(
                            invocation -> {
                                callMinutes.add(reads.get() - 1);
                                generator.collect(result, 1, TimeUnit.NANOSECONDS, false);
                                return null;
                            })
                    .when(worker)
                    .pauseConsumers();
            doAnswer(
                            invocation -> {
                                callMinutes.add(reads.get() - 1);
                                generator.collect(result, 1, TimeUnit.NANOSECONDS, false);
                                return null;
                            })
                    .when(worker)
                    .resumeConsumers();
            generator.collect(result, 1, TimeUnit.NANOSECONDS, false);
            generator.pauseConsumersMidRun(
                    workload.consumerPause, () -> TimeUnit.MINUTES.toNanos(reads.getAndIncrement()), 1);
            generator.collect(result, 1, TimeUnit.NANOSECONDS, true);
        }

        // Paused 1 minute in, resumed 2 minutes later
        assertThat(callMinutes).containsExactly(1L, 3L);
        var order = inOrder(worker);
        order.verify(worker).setPhase(RunPhase.BACKLOG_BUILD);
        order.verify(worker).pauseConsumers();
        order.verify(worker).setPhase(RunPhase.BACKLOG_DRAIN);
        order.verify(worker).resumeConsumers();
        order.verify(worker).setPhase(RunPhase.MEASURE);
        assertThat(result.phase)
                .containsExactly(
                        RunPhase.MEASURE, RunPhase.BACKLOG_BUILD, RunPhase.BACKLOG_DRAIN, RunPhase.MEASURE);
        // Resume read the clock at 4 minutes and the drained check at 5
        assertThat(result.consumerPause)
                .isEqualTo(new TestResult.ConsumerPauseResult(1, 1, 5_000, 60.0));
    }

    @Test
    void consumerPauseLeavesTheDrainTimeUnsetWhenTheRunEndsFirst() throws Exception {
        Worker worker = worker();
        Workload workload = pausedWorkload();
        AtomicLong reads = new AtomicLong();
        AtomicInteger counterReads = new AtomicInteger();
        TestResult result = new TestResult();
        WorkloadGenerator generator =
                new WorkloadGenerator(
                        "driver",
                        workload,
                        worker,
                        Duration.ofMillis(10),
                        Duration.ofMillis(10),
                        Duration.ofSeconds(2));
        // The backlog only grows; the run ends at the first drain check
        when(worker.getCountersStats())
                .thenAnswer(invocation -> {
                    int read = counterReads.incrementAndGet();
                    if (read == 3) {
                        generator.close();
                    }
                    return new CountersStats(read * 10_000L, 0, 0);
                });
        doAnswer(
                        invocation -> {
                            generator.collect(result, 1, TimeUnit.NANOSECONDS, false);
                            return null;
                        })
                .when(worker)
                .pauseConsumers();

        try (generator) {
            generator.pauseConsumersMidRun(
                    workload.consumerPause, () -> TimeUnit.MINUTES.toNanos(reads.getAndIncrement()), 1);
            generator.collect(result, 1, TimeUnit.NANOSECONDS, true);
        }

        verify(worker, never()).setPhase(RunPhase.MEASURE);
        assertThat(result.consumerPause)
                .isEqualTo(new TestResult.ConsumerPauseResult(0, 0, 20_000, null));
    }

    @Test
    void propagatesReadinessFailureAndStillStopsWorker() {
        Worker worker = worker();
        doThrow(new IllegalStateException("probe rejected")).when(worker).probeProducers();
        try (WorkloadGenerator generator = generator(workload(), worker, Duration.ofSeconds(2))) {
            assertThatThrownBy(generator::run).hasMessage("probe rejected");
            verify(worker, never()).startLoad(any());
        }
        verify(worker).stopAll();
    }

    @Test
    @Timeout(5)
    void backlogControllerFailureWakesTheRunImmediately() {
        Worker worker = worker();
        doThrow(new IllegalStateException("pause failed")).when(worker).pauseConsumers();
        Workload workload = workload();
        workload.consumerBacklogSizeGB = 1;
        try (WorkloadGenerator generator = generator(workload, worker, Duration.ofSeconds(2))) {
            assertThatThrownBy(generator::run).hasRootCauseMessage("pause failed");
        }
        verify(worker).stopAll();
    }

    @Test
    void sustainableRateControllerFailureIsNotSwallowed() {
        Worker worker = worker();
        when(worker.getCountersStats())
                .thenReturn(new CountersStats())
                .thenThrow(new IllegalStateException("rate controller failed"));
        Workload workload = workload();
        workload.producerRate = 0;
        try (WorkloadGenerator generator = generator(workload, worker, Duration.ofSeconds(2))) {
            assertThatThrownBy(generator::run).hasRootCauseMessage("rate controller failed");
        }
        verify(worker).stopAll();
    }

    @Test
    void sustainableRateControllerRampsUpThenBacksOffWhenBacklogGrows() throws Exception {
        Worker worker = worker();
        when(worker.getCountersStats())
                .thenReturn(new CountersStats(), new CountersStats(), new CountersStats(2_000, 0, 0));
        List<Double> adjustedRates = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch adjustments = new CountDownLatch(2);
        doAnswer(
                        invocation -> {
                            adjustedRates.add(invocation.getArgument(0));
                            adjustments.countDown();
                            return null;
                        })
                .when(worker)
                .adjustPublishRate(anyDouble());

        Workload workload = workload();
        workload.producerRate = 0;
        AtomicReference<Throwable> runFailure = new AtomicReference<>();
        WorkloadGenerator generator = generator(workload, worker, Duration.ofSeconds(2));
        Thread runner =
                Thread.ofVirtual()
                        .start(() -> {
                            try {
                                generator.run();
                            } catch (Throwable t) {
                                runFailure.set(t);
                            }
                        });
        try {
            assertThat(adjustments.await(2, TimeUnit.SECONDS)).isTrue();
            generator.close();
            runner.join(2000);
            assertThat(runner.isAlive()).isFalse();
            assertThat(adjustedRates).contains(20_000.0, 0.0);
            assertThat(runFailure.get()).isInstanceOf(InterruptedException.class);
        } finally {
            generator.close();
            runner.join(2000);
        }
        verify(worker).stopAll();
    }

    @Test
    void runsProducerOnlyAndConsumerOnlyWorkloads() throws Exception {
        Worker consumerOnlyWorker = worker();
        when(consumerOnlyWorker.getCountersStats()).thenReturn(new CountersStats(0, 0, 1));
        CountDownLatch consumerOnlyStarted = new CountDownLatch(1);
        doAnswer(
                        ignored -> {
                            consumerOnlyStarted.countDown();
                            return null;
                        })
                .when(consumerOnlyWorker)
                .startLoad(any());
        Workload consumerOnly = workload();
        consumerOnly.producersPerTopic = 0;
        consumerOnly.subscriptionsPerTopic = 1;
        consumerOnly.consumerPerSubscription = 1;
        WorkloadGenerator consumerOnlyGenerator =
                generator(consumerOnly, consumerOnlyWorker, Duration.ofSeconds(2));
        AtomicReference<Throwable> consumerOnlyFailure = new AtomicReference<>();
        Thread consumerOnlyRun =
                Thread.ofVirtual()
                        .start(() -> {
                            try {
                                consumerOnlyGenerator.run();
                            } catch (Throwable t) {
                                consumerOnlyFailure.set(t);
                            }
                        });
        try {
            assertThat(consumerOnlyStarted.await(2, TimeUnit.SECONDS)).isTrue();
        } finally {
            consumerOnlyGenerator.close();
            consumerOnlyRun.join(2000);
        }
        assertThat(consumerOnlyRun.isAlive()).isFalse();
        assertThat(consumerOnlyFailure.get()).isInstanceOf(InterruptedException.class);
        verify(consumerOnlyWorker).createProducers(List.of());
        verify(consumerOnlyWorker)
                .createConsumers(
                        org.mockito.ArgumentMatchers.argThat(
                                assignment -> assignment.topicsSubscriptions.size() == 1));

        Worker producerOnlyWorker = worker();
        CountDownLatch producerOnlyStarted = new CountDownLatch(1);
        doAnswer(
                        ignored -> {
                            producerOnlyStarted.countDown();
                            return null;
                        })
                .when(producerOnlyWorker)
                .startLoad(any());
        Workload producerOnly = workload();
        producerOnly.subscriptionsPerTopic = 0;
        WorkloadGenerator producerOnlyGenerator =
                generator(producerOnly, producerOnlyWorker, Duration.ofSeconds(2));
        AtomicReference<Throwable> producerOnlyFailure = new AtomicReference<>();
        Thread producerOnlyRun =
                Thread.ofVirtual()
                        .start(() -> {
                            try {
                                producerOnlyGenerator.run();
                            } catch (Throwable t) {
                                producerOnlyFailure.set(t);
                            }
                        });
        try {
            assertThat(producerOnlyStarted.await(2, TimeUnit.SECONDS)).isTrue();
        } finally {
            producerOnlyGenerator.close();
            producerOnlyRun.join(2000);
        }
        assertThat(producerOnlyRun.isAlive()).isFalse();
        assertThat(producerOnlyFailure.get()).isInstanceOf(InterruptedException.class);
        verify(producerOnlyWorker).createProducers(List.of("topic"));
        verify(producerOnlyWorker)
                .createConsumers(
                        org.mockito.ArgumentMatchers.argThat(
                                assignment -> assignment.topicsSubscriptions.isEmpty()));
    }

    @Test
    void buildsAndDrainsTheConfiguredBacklogBeforeReporting() throws Exception {
        Worker worker = worker();
        AtomicInteger counterReads = new AtomicInteger();
        when(worker.getCountersStats())
                .thenAnswer(ignored ->
                        switch (counterReads.getAndIncrement()) {
                            case 0 -> new CountersStats(1, 0, 1);
                            case 1 -> new CountersStats(1_025, 0, 1);
                            default -> new CountersStats(1_025, 0, 1_025);
                        });
        CountDownLatch backlogDrained = new CountDownLatch(1);
        doAnswer(
                        ignored -> {
                            backlogDrained.countDown();
                            return null;
                        })
                .when(worker)
                .resumeConsumers();
        Workload workload = workload();
        workload.messageSize = 1024 * 1024;
        workload.subscriptionsPerTopic = 1;
        workload.consumerPerSubscription = 1;
        workload.consumerBacklogSizeGB = 1;
        workload.testDurationMinutes = 1;

        WorkloadGenerator generator = generator(workload, worker, Duration.ofSeconds(2));
        AtomicReference<Throwable> runFailure = new AtomicReference<>();
        Thread run =
                Thread.ofVirtual()
                        .start(() -> {
                            try {
                                generator.run();
                            } catch (Throwable t) {
                                runFailure.set(t);
                            }
                        });
        try {
            assertThat(backlogDrained.await(3, TimeUnit.SECONDS)).isTrue();
        } finally {
            generator.close();
            run.join(2000);
        }

        assertThat(run.isAlive()).isFalse();
        assertThat(runFailure.get()).isInstanceOf(InterruptedException.class);
        verify(worker).pauseConsumers();
        verify(worker).resumeConsumers();
        verify(worker).resetStats();
        assertThat(counterReads.get()).isGreaterThanOrEqualTo(3);
    }

    @Test
    void propagatesProducerFailureFromTheWorkerBoundary() {
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
    void rejectsNonPositiveControllerIntervals() {
        Worker worker = mock(Worker.class);
        Duration ok = Duration.ofSeconds(1);
        List<Duration[]> invalid =
                List.of(
                        new Duration[] {Duration.ZERO, ok, ok},
                        new Duration[] {ok, Duration.ofNanos(1), ok},
                        new Duration[] {ok, ok, Duration.ZERO});
        Workload workload = workload();
        for (Duration[] durations : invalid) {
            assertThatThrownBy(() ->
                            new WorkloadGenerator(
                                    "driver", workload, worker, durations[0], durations[1], durations[2]))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must be positive");
        }
    }

    @Test
    void cannotRunAClosedGenerator() {
        Worker worker = mock(Worker.class);
        WorkloadGenerator generator = generator(workload(), worker, Duration.ofSeconds(2));
        generator.close();

        assertThatThrownBy(generator::run)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed or has already run");
        assertThat(generator.isQuiescentAfterClose()).isTrue();
    }

    @Test
    void closeWaitsForRealControllerExitAndAttemptsWorkerStopOnTimeout() throws Exception {
        Worker worker = worker();
        CountDownLatch controllerEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<Boolean> controllerDaemon = new AtomicReference<>();
        when(worker.getCountersStats())
                .thenAnswer(ignored -> {
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
                        .start(() -> {
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

    private static Worker worker() {
        Worker worker = mock(Worker.class);
        when(worker.createTopics(any())).thenReturn(List.of("topic"));
        when(worker.getCountersStats()).thenReturn(new CountersStats());
        when(worker.getPeriodStats()).thenReturn(new PeriodStats());
        when(worker.getCumulativeLatencies()).thenReturn(new CumulativeLatencies());
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

    private static Workload pausedWorkload() {
        Workload workload = workload();
        workload.subscriptionsPerTopic = 1;
        workload.testDurationMinutes = 5;
        workload.consumerPause = new Workload.ConsumerPause(1, 2);
        workload.validate();
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
            } catch (InterruptedException _) {
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
