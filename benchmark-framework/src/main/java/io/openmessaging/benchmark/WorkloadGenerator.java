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

import static io.openmessaging.benchmark.driver.Failures.append;
import static java.util.concurrent.TimeUnit.MINUTES;

import io.openmessaging.benchmark.driver.Failures;
import io.openmessaging.benchmark.utils.RandomGenerator;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.payload.PayloadTemplates;
import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.RunPhase;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.spi.LoggingEventBuilder;

public final class WorkloadGenerator implements WorkloadRun {

    private final String driverName;
    private final Workload workload;
    private final Worker worker;

    private final ExecutorService executor =
            Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("messaging-benchmark-", 0).factory());
    private final Object controllerLock = new Object();
    private final List<Future<?>> controllers = new ArrayList<>();
    private final CompletableFuture<Throwable> controllerFailureSignal = new CompletableFuture<>();
    private final CountDownLatch runExited = new CountDownLatch(1);
    private final long reportIntervalNanos;
    private final long controlPeriodMillis;
    private final long shutdownTimeoutNanos;

    private volatile boolean runCompleted = false;
    private volatile boolean needToWaitForBacklogDraining = false;
    private volatile boolean closed;
    private volatile boolean quiescentAfterClose;
    private volatile @Nullable Thread runThread;
    private boolean started;
    private volatile boolean includeHistograms;
    private @Nullable Throwable controllerFailure;
    private boolean controllerFailureReported;

    private volatile double targetPublishRate;
    private volatile RunPhase phase = RunPhase.MEASURE;
    private volatile int stage;
    // The creator's MDC (run id, workload, driver), repeated on phase events from controller threads
    private final Map<String, String> logContext =
            Objects.requireNonNullElse(MDC.getCopyOfContextMap(), Map.of());

    public WorkloadGenerator(String driverName, Workload workload, Worker worker) {
        this(
                driverName,
                workload,
                worker,
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ofSeconds(10));
    }

    WorkloadGenerator(
            String driverName,
            Workload workload,
            Worker worker,
            Duration reportInterval,
            Duration controlPeriod,
            Duration shutdownTimeout) {
        this.driverName = driverName;
        this.workload = workload;
        this.worker = worker;
        reportIntervalNanos = reportInterval.toNanos();
        controlPeriodMillis = controlPeriod.toMillis();
        shutdownTimeoutNanos = shutdownTimeout.toNanos();
        if (reportIntervalNanos <= 0 || controlPeriodMillis <= 0 || shutdownTimeoutNanos <= 0) {
            throw new IllegalArgumentException(
                    "Controller intervals and shutdown timeout must be positive");
        }
    }

    @Override
    public TestResult run() throws IOException, InterruptedException {
        synchronized (this) {
            if (closed || started) {
                throw new IllegalStateException("Workload generator is closed or has already run");
            }
            started = true;
            runThread = Thread.currentThread();
        }
        try {
            return runWorkload();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            runCompleted = true;
            runExited.countDown();
            runThread = null;
        }
    }

    @Override
    public synchronized void includeHistograms(boolean include) {
        if (started || closed) {
            throw new IllegalStateException("Histogram option must be set before the workload starts");
        }
        includeHistograms = include;
    }

    private TestResult runWorkload() throws IOException, InterruptedException {
        workload.validate();

        var timer = new Timer();
        List<String> topics =
                worker.createTopics(new TopicsInfo(workload.topics, workload.partitionsPerTopic));
        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());

        createConsumers(topics);
        createProducers(topics);

        ensureTopicsAreReady();

        List<Workload.Stage> stages = workload.stages;
        if (stages != null && !stages.isEmpty()) {
            targetPublishRate = stages.getFirst().targetRate();
        } else if (workload.producerRate > 0) {
            targetPublishRate = workload.producerRate;
        } else {
            // Producer rate is 0 and we need to discover the sustainable rate
            targetPublishRate = 10000;

            startController(
                    "sustainable rate", () -> findMaximumSustainableRate(targetPublishRate), () -> {});
        }

        PayloadTemplates payloads = PayloadFactory.create(workload);
        var producerWorkAssignment =
                new ProducerWorkAssignment(payloads, targetPublishRate, workload.keyDistributor);
        log.info(
                "Prepared {} payloads from {} in {} ms",
                payloads.size(),
                workload.usesMessageSizeDistribution() ? "size distribution" : workload.payloadFile,
                timer.elapsedMillis());
        worker.startLoad(producerWorkAssignment);
        checkControllerFailure();

        TestResult result = newResult();
        if (workload.warmupDurationMinutes > 0) {
            enterPhase(RunPhase.WARMUP);
            log.info("----- Starting warm-up traffic ({}m) ------", workload.warmupDurationMinutes);
            collect(result, workload.warmupDurationMinutes, TimeUnit.MINUTES, false);
        }

        // Set before the backlog controller starts, so it cannot be overwritten by a stale MEASURE
        enterPhase(workload.consumerBacklogSizeGB > 0 ? RunPhase.BACKLOG_BUILD : RunPhase.MEASURE);
        if (workload.consumerBacklogSizeGB > 0) {
            needToWaitForBacklogDraining = true;
            startController(
                    "backlog",
                    () -> buildAndDrainBacklog(workload.testDurationMinutes),
                    () -> needToWaitForBacklogDraining = false);
        }

        worker.resetStats();
        int measuredMinutes = workload.measuredMinutes();
        log.info("----- Starting benchmark traffic ({}m)------", measuredMinutes);
        if (workload.usesStages()) {
            startController("stages", () -> driveStages(System::nanoTime, 1000), () -> {});
        }

        collect(result, measuredMinutes, TimeUnit.MINUTES, true);
        checkControllerFailure();
        return result;
    }

    private void ensureTopicsAreReady() throws IOException, InterruptedException {
        log.info("Waiting for consumers to be ready");
        // This is work around the fact that there's no way to have a consumer ready in Kafka without
        // first publishing
        // some message on the topic, which will then trigger the partitions assignment to the consumers

        int expectedMessages = workload.topics * workload.subscriptionsPerTopic;

        // In this case we just publish 1 message and then wait for consumers to receive the data
        worker.probeProducers();

        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true) {
            CountersStats stats = worker.getCountersStats();

            log.info(
                    "Waiting for topics to be ready -- Sent: {}, Received: {}",
                    stats.messagesSent(),
                    stats.messagesReceived());
            if (stats.messagesReceived() >= expectedMessages) {
                log.info("All consumers are ready");
                return;
            }
            long remaining = end - System.nanoTime();
            if (remaining <= 0) {
                throw new IOException("Timed out waiting for consumers to be ready");
            }
            TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.SECONDS.toNanos(2)));
        }
    }

    /**
     * Adjust the publish rate to a level that is sustainable, meaning that we can consume all the
     * messages that are being produced.
     *
     * @param currentRate the publish rate to start the search from
     */
    private void findMaximumSustainableRate(double currentRate) throws InterruptedException {
        long lastControlTimestamp = System.nanoTime();

        RateController rateController = new RateController();

        while (!runCompleted) {
            // Check every few seconds and adjust the rate
            Thread.sleep(controlPeriodMillis);
            if (runCompleted) {
                return;
            }

            CountersStats stats = worker.getCountersStats();
            long currentTime = System.nanoTime();
            long periodNanos = currentTime - lastControlTimestamp;

            lastControlTimestamp = currentTime;

            currentRate =
                    rateController.nextRate(
                            currentRate, periodNanos, stats.messagesSent(), stats.messagesReceived());
            targetPublishRate = currentRate;
            worker.adjustPublishRate(currentRate);
        }
    }

    /**
     * Moves the publish rate through the workload stages, telling workers only when it changes.
     *
     * @param clock the nanosecond clock the stage durations are measured against
     * @param tickMillis how often the rate is recomputed
     */
    void driveStages(LongSupplier clock, long tickMillis) throws InterruptedException {
        List<Workload.Stage> stages = Objects.requireNonNull(workload.stages);
        long start = clock.getAsLong();
        long total = MINUTES.toNanos(workload.measuredMinutes());
        double sent = targetPublishRate;
        while (!runCompleted) {
            long elapsed = clock.getAsLong() - start;
            // Walk the stages: a zero-length stage is passed at once, so its target becomes a step
            int index = stages.size() - 1;
            double rate = stages.get(index).targetRate();
            double from = stages.getFirst().targetRate();
            long stageStart = 0;
            for (int i = 0; i < stages.size(); i++) {
                Workload.Stage s = stages.get(i);
                long length = MINUTES.toNanos(s.durationMinutes());
                if (elapsed < stageStart + length) {
                    index = i;
                    rate = from + (s.targetRate() - from) * (double) (elapsed - stageStart) / length;
                    break;
                }
                stageStart += length;
                from = s.targetRate();
            }
            stage = index;
            targetPublishRate = rate;
            if (Double.compare(rate, sent) != 0) {
                worker.adjustPublishRate(rate);
                sent = rate;
            }
            if (elapsed >= total) {
                return;
            }
            Thread.sleep(tickMillis);
        }
    }

    // onExit runs after any controller failure has been recorded, so the run cannot finish without it
    private void startController(String name, Controller controller, Runnable onExit) {
        synchronized (controllerLock) {
            if (closed) {
                throw new IllegalStateException("Workload generator is closed");
            }
            controllers.add(
                    executor.submit(() -> {
                        try {
                            controller.run();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            if (!runCompleted) {
                                recordControllerFailure(name, e);
                            }
                        } catch (Throwable t) {
                            recordControllerFailure(name, t);
                        } finally {
                            onExit.run();
                        }
                    }));
        }
    }

    private void recordControllerFailure(String name, Throwable cause) {
        Throwable failure = new IllegalStateException(name + " controller failed", cause);
        synchronized (controllerLock) {
            controllerFailure = append(controllerFailure, failure);
            controllerFailureSignal.complete(controllerFailure);
        }
    }

    private void checkControllerFailure() throws IOException {
        synchronized (controllerLock) {
            if (controllerFailure != null) {
                controllerFailureReported = true;
                throw new IOException("Background workload controller failed", controllerFailure);
            }
        }
    }

    private void waitForReportInterval() throws IOException, InterruptedException {
        try {
            controllerFailureSignal.get(reportIntervalNanos, TimeUnit.NANOSECONDS);
            checkControllerFailure();
        } catch (TimeoutException _) {
            checkControllerFailure();
        } catch (ExecutionException e) {
            throw new IOException("Could not monitor workload controllers", e.getCause());
        }
    }

    @FunctionalInterface
    private interface Controller {
        void run() throws InterruptedException;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        runCompleted = true;
        Thread active = runThread;
        if (active != null && active != Thread.currentThread()) {
            active.interrupt();
        }
        synchronized (controllerLock) {
            controllers.forEach(controller -> controller.cancel(true));
        }
        executor.shutdownNow();

        Failures failures = new Failures();
        failures.deferInterrupt("Interrupted before stopping workload controllers");
        boolean runQuiescent = active == null;
        boolean controllersQuiescent = false;
        if (active != null && active != Thread.currentThread()) {
            try {
                runQuiescent = runExited.await(shutdownTimeoutNanos, TimeUnit.NANOSECONDS);
                if (!runQuiescent) {
                    failures.add(new TimeoutException("Workload run did not exit"));
                }
            } catch (InterruptedException e) {
                failures.addInterruption(e);
            }
        }
        try {
            controllersQuiescent = executor.awaitTermination(shutdownTimeoutNanos, TimeUnit.NANOSECONDS);
            if (!controllersQuiescent) {
                failures.add(new TimeoutException("Workload controllers did not terminate"));
            }
        } catch (InterruptedException e) {
            failures.addInterruption(e);
        }
        synchronized (controllerLock) {
            if (controllerFailure != null && !controllerFailureReported) {
                failures.add(controllerFailure);
                controllerFailureReported = true;
            }
        }
        try {
            worker.stopAll();
        } catch (Throwable t) {
            failures.add(t);
        }
        quiescentAfterClose = runQuiescent && controllersQuiescent;
        failures.throwIfAny("Failed to close workload");
    }

    @Override
    public boolean isQuiescentAfterClose() {
        return quiescentAfterClose;
    }

    private void createConsumers(List<String> topics) {
        ConsumerAssignment consumerAssignment = new ConsumerAssignment();

        for (String topic : topics) {
            for (int i = 0; i < workload.subscriptionsPerTopic; i++) {
                String subscriptionName =
                        String.format("sub-%03d-%s", i, RandomGenerator.getRandomString());
                for (int j = 0; j < workload.consumerPerSubscription; j++) {
                    consumerAssignment.topicsSubscriptions.add(
                            new TopicSubscription(topic, subscriptionName));
                }
            }
        }

        Collections.shuffle(consumerAssignment.topicsSubscriptions);

        Timer timer = new Timer();

        worker.createConsumers(consumerAssignment);
        log.info(
                "Created {} consumers in {} ms",
                consumerAssignment.topicsSubscriptions.size(),
                timer.elapsedMillis());
    }

    private void createProducers(List<String> topics) {
        List<String> fullListOfTopics = new ArrayList<>();

        // Add the topic multiple times, one for each producer
        for (int i = 0; i < workload.producersPerTopic; i++) {
            fullListOfTopics.addAll(topics);
        }

        Collections.shuffle(fullListOfTopics);

        Timer timer = new Timer();

        worker.createProducers(fullListOfTopics);
        log.info("Created {} producers in {} ms", fullListOfTopics.size(), timer.elapsedMillis());
    }

    private void buildAndDrainBacklog(int testDurationMinutes) throws InterruptedException {
        Timer timer = new Timer();
        log.info("Stopping all consumers to build backlog");
        worker.pauseConsumers();

        int effectiveMessageSize = workload.effectiveMessageSize();
        long requestedBacklogSize = workload.consumerBacklogSizeGB * 1024 * 1024 * 1024;

        while (true) {
            CountersStats stats = worker.getCountersStats();
            long currentBacklogSize = backlog(stats) * effectiveMessageSize;

            if (currentBacklogSize >= requestedBacklogSize) {
                break;
            }

            Thread.sleep(1000);
        }

        log.info("--- Completed backlog build in {} s ---", timer.elapsedSeconds());
        timer = new Timer();
        enterPhase(RunPhase.BACKLOG_DRAIN);
        log.info("--- Start draining backlog ---");

        worker.resumeConsumers();

        long backlogMessageCapacity = requestedBacklogSize / effectiveMessageSize;
        long backlogEmptyLevel = (long) ((1.0 - workload.backlogDrainRatio) * backlogMessageCapacity);
        final long minBacklog = Math.max(1000L, backlogEmptyLevel);

        while (true) {
            CountersStats stats = worker.getCountersStats();
            if (backlog(stats) <= minBacklog) {
                log.info("--- Completed backlog draining in {} s ---", timer.elapsedSeconds());
                enterPhase(RunPhase.MEASURE);

                Thread.sleep(MINUTES.toMillis(testDurationMinutes));

                return;
            }

            Thread.sleep(100);
        }
    }

    // Messages published but not yet consumed, counting one copy per subscription
    private long backlog(CountersStats stats) {
        return workload.subscriptionsPerTopic * stats.messagesSent() - stats.messagesReceived();
    }

    // Moves the run to a phase: later periods are tagged with it, workers are told, and one
    // structured phase event is logged
    void enterPhase(RunPhase next) {
        phase = next;
        worker.setPhase(next);
        LoggingEventBuilder event = log.atInfo().setMessage("Run phase: {}").addArgument(next);
        for (Map.Entry<String, String> entry : logContext.entrySet()) {
            event = event.addKeyValue(entry.getKey(), entry.getValue());
        }
        event.addKeyValue("event", "phase").addKeyValue("phase", next.name()).log();
    }

    TestResult printAndCollectStats(long testDurations, TimeUnit unit)
            throws IOException, InterruptedException {
        TestResult result = newResult();
        collect(result, testDurations, unit, true);
        return result;
    }

    private TestResult newResult() {
        TestResult result = new TestResult();
        result.workload = workload.name;
        result.driver = driverName;
        result.topics = workload.topics;
        result.partitions = workload.partitionsPerTopic;
        result.messageSize = workload.effectiveMessageSize();
        result.producersPerTopic = workload.producersPerTopic;
        result.consumersPerTopic = workload.consumerPerSubscription;
        return result;
    }

    // Records report intervals into the result until the duration ends. The measured window also
    // waits for any backlog to drain and then adds the aggregate and summary.
    void collect(TestResult result, long duration, TimeUnit unit, boolean measured)
            throws IOException, InterruptedException {
        long startTime = System.nanoTime();
        long oldTime = startTime;
        long endTime = duration > 0 ? startTime + unit.toNanos(duration) : Long.MAX_VALUE;

        while (true) {
            waitForReportInterval();

            PeriodStats stats = worker.getPeriodStats();
            long now = System.nanoTime();
            PeriodReport period =
                    PeriodReport.of(stats, (now - oldTime) / 1e9, workload.subscriptionsPerTopic);
            RunPhase current = phase;
            try (var ignored = MDC.putCloseable("phase", current.name())) {
                ResultLog.period(period);
            }
            result.recordPeriod(period, current, targetPublishRate, workload.usesStages() ? stage : null);

            if (now >= endTime && !(measured && needToWaitForBacklogDraining)) {
                if (measured) {
                    finish(result);
                }
                return;
            }

            oldTime = now;
        }
    }

    private void finish(TestResult result) {
        CumulativeLatencies aggregate = worker.getCumulativeLatencies();
        ResultLog.aggregate(aggregate);
        result.recordAggregate(aggregate);
        var counters = worker.getCountersStats();
        result.recordSummary(
                counters,
                // A staged run has no single target; the measured average matches the achieved average
                workload.usesStages()
                        ? TestResult.average(TestResult.measured(result.phase, result.targetRate))
                        : targetPublishRate,
                TestResult.measured(result.phase, result.backlog).stream()
                        .mapToLong(Long::longValue)
                        .max()
                        .orElse(0));
        if (includeHistograms) {
            result.recordHistogramData(aggregate);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);
}
