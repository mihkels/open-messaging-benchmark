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

import static java.util.concurrent.TimeUnit.MINUTES;

import io.openmessaging.benchmark.utils.PaddingDecimalFormat;
import io.openmessaging.benchmark.utils.RandomGenerator;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.payload.FilePayloadReader;
import io.openmessaging.benchmark.utils.payload.MessageSizeDistribution;
import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.HdrHistogram.Histogram;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private volatile Thread runThread;
    private boolean started;
    private Throwable controllerFailure;
    private boolean controllerFailureReported;

    private volatile double targetPublishRate;

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
    public TestResult run() throws Exception {
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

    private TestResult runWorkload() throws Exception {
        if (workload.consumerBacklogSizeGB > 0 && workload.producerRate == 0) {
            throw new IllegalArgumentException(
                    "Cannot probe producer sustainable rate when building backlog");
        }

        var timer = new Timer();
        List<String> topics =
                worker.createTopics(new TopicsInfo(workload.topics, workload.partitionsPerTopic));
        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());

        createConsumers(topics);
        createProducers(topics);

        ensureTopicsAreReady();

        if (workload.producerRate > 0) {
            targetPublishRate = workload.producerRate;
        } else {
            // Producer rate is 0 and we need to discover the sustainable rate
            targetPublishRate = 10000;

            startController(
                    "sustainable rate", () -> findMaximumSustainableRate(targetPublishRate), false);
        }

        List<byte[]> payloads = new ArrayList<>();
        int[] payloadWeights = null;
        if (workload.usesMessageSizeDistribution()) {
            MessageSizeDistribution distribution =
                    new MessageSizeDistribution(workload.messageSizeDistribution);
            payloadWeights = distribution.getWeights();
            Random random = new Random();
            for (int size : distribution.getBucketSizes()) {
                byte[] payload = new byte[size];
                if (workload.useRandomizedPayloads) {
                    int randomBytes = (int) (size * workload.randomBytesRatio);
                    random.nextBytes(payload);
                    java.util.Arrays.fill(payload, randomBytes, size, (byte) 0);
                }
                payloads.add(payload);
            }
            log.info(
                    "Prepared {} payload templates for message-size distribution (bucket sizes: {}, "
                            + "weighted average: {} bytes)",
                    payloads.size(),
                    distribution.getBucketSizes(),
                    distribution.getAvgSize());
        } else if (workload.useRandomizedPayloads) {
            var r = new Random();
            var randomBytes = (int) (workload.messageSize * workload.randomBytesRatio);
            var zerodBytes = workload.messageSize - randomBytes;
            for (int i = 0; i < workload.randomizedPayloadPoolSize; i++) {
                byte[] randArray = new byte[randomBytes];
                r.nextBytes(randArray);
                byte[] zerodArray = new byte[zerodBytes];
                byte[] combined = ArrayUtils.addAll(randArray, zerodArray);
                payloads.add(combined);
            }
        } else {
            payloads.add(new FilePayloadReader(workload.messageSize).load(workload.payloadFile));
        }

        var producerWorkAssignment =
                new ProducerWorkAssignment(
                        payloads, targetPublishRate, workload.keyDistributor, payloadWeights);
        log.info(
                "Prepared {} payloads from {} in {} ms",
                producerWorkAssignment.payloadData().size(),
                workload.usesMessageSizeDistribution() ? "size distribution" : workload.payloadFile,
                timer.elapsedMillis());
        worker.startLoad(producerWorkAssignment);
        checkControllerFailure();

        if (workload.warmupDurationMinutes > 0) {
            log.info("----- Starting warm-up traffic ({}m) ------", workload.warmupDurationMinutes);
            printAndCollectStats(workload.warmupDurationMinutes, TimeUnit.MINUTES);
        }

        if (workload.consumerBacklogSizeGB > 0) {
            needToWaitForBacklogDraining = true;
            startController("backlog", () -> buildAndDrainBacklog(workload.testDurationMinutes), true);
        }

        worker.resetStats();
        log.info("----- Starting benchmark traffic ({}m)------", workload.testDurationMinutes);

        var result = printAndCollectStats(workload.testDurationMinutes, TimeUnit.MINUTES);
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
     * @param currentRate
     */
    private void findMaximumSustainableRate(double currentRate)
            throws IOException, InterruptedException {
        CountersStats stats = worker.getCountersStats();

        long lastControlTimestamp = System.nanoTime();

        RateController rateController = new RateController();

        while (!runCompleted) {
            // Check every few seconds and adjust the rate
            Thread.sleep(controlPeriodMillis);
            if (runCompleted) {
                return;
            }

            // Consider multiple copies when using multiple subscriptions
            stats = worker.getCountersStats();
            long currentTime = System.nanoTime();
            long periodNanos = currentTime - lastControlTimestamp;

            lastControlTimestamp = currentTime;

            currentRate =
                    rateController.nextRate(
                            currentRate, periodNanos, stats.messagesSent(), stats.messagesReceived());
            worker.adjustPublishRate(currentRate);
        }
    }

    private void startController(String name, Controller controller, boolean backlog) {
        synchronized (controllerLock) {
            if (closed) {
                throw new IllegalStateException("Workload generator is closed");
            }
            controllers.add(
                    executor.submit(
                            () -> {
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
                                    if (backlog) {
                                        needToWaitForBacklogDraining = false;
                                    }
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
        } catch (TimeoutException ignored) {
            checkControllerFailure();
        } catch (ExecutionException e) {
            throw new IOException("Could not monitor workload controllers", e.getCause());
        }
    }

    @FunctionalInterface
    private interface Controller {
        void run() throws Exception;
    }

    private static Throwable append(Throwable primary, Throwable next) {
        if (primary == null) {
            return next;
        }
        if (next != null && primary != next) {
            primary.addSuppressed(next);
        }
        return primary;
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

        Throwable failure = null;
        boolean interrupted = Thread.interrupted();
        boolean runQuiescent = active == null;
        boolean controllersQuiescent = false;
        if (interrupted) {
            failure = new InterruptedException("Interrupted before stopping workload controllers");
        }
        if (active != null && active != Thread.currentThread()) {
            try {
                runQuiescent = runExited.await(shutdownTimeoutNanos, TimeUnit.NANOSECONDS);
                if (!runQuiescent) {
                    failure = append(failure, new TimeoutException("Workload run did not exit"));
                }
            } catch (InterruptedException e) {
                interrupted = true;
                failure = append(failure, e);
            }
        }
        try {
            controllersQuiescent = executor.awaitTermination(shutdownTimeoutNanos, TimeUnit.NANOSECONDS);
            if (!controllersQuiescent) {
                failure = append(failure, new TimeoutException("Workload controllers did not terminate"));
            }
        } catch (InterruptedException e) {
            interrupted = true;
            failure = append(failure, e);
        }
        synchronized (controllerLock) {
            if (controllerFailure != null && !controllerFailureReported) {
                failure = append(failure, controllerFailure);
                controllerFailureReported = true;
            }
        }
        try {
            worker.stopAll();
        } catch (Throwable t) {
            failure = append(failure, t);
        }
        quiescentAfterClose = runQuiescent && controllersQuiescent;
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (failure != null) {
            throw new IllegalStateException("Failed to close workload", failure);
        }
    }

    @Override
    public boolean isQuiescentAfterClose() {
        return quiescentAfterClose;
    }

    private void createConsumers(List<String> topics) throws IOException {
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

    private void createProducers(List<String> topics) throws IOException {
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

    private void buildAndDrainBacklog(int testDurationMinutes)
            throws IOException, InterruptedException {
        Timer timer = new Timer();
        log.info("Stopping all consumers to build backlog");
        worker.pauseConsumers();

        int effectiveMessageSize = effectiveMessageSize();
        if (effectiveMessageSize <= 0) {
            throw new IllegalArgumentException("Message size must be positive when building a backlog");
        }
        long requestedBacklogSize = workload.consumerBacklogSizeGB * 1024 * 1024 * 1024;

        while (true) {
            CountersStats stats = worker.getCountersStats();
            long currentBacklogSize =
                    (workload.subscriptionsPerTopic * stats.messagesSent() - stats.messagesReceived())
                            * effectiveMessageSize;

            if (currentBacklogSize >= requestedBacklogSize) {
                break;
            }

            Thread.sleep(1000);
        }

        log.info("--- Completed backlog build in {} s ---", timer.elapsedSeconds());
        timer = new Timer();
        log.info("--- Start draining backlog ---");

        worker.resumeConsumers();

        long backlogMessageCapacity = requestedBacklogSize / effectiveMessageSize;
        long backlogEmptyLevel = (long) ((1.0 - workload.backlogDrainRatio) * backlogMessageCapacity);
        final long minBacklog = Math.max(1000L, backlogEmptyLevel);

        while (true) {
            CountersStats stats = worker.getCountersStats();
            long currentBacklog =
                    workload.subscriptionsPerTopic * stats.messagesSent() - stats.messagesReceived();
            if (currentBacklog <= minBacklog) {
                log.info("--- Completed backlog draining in {} s ---", timer.elapsedSeconds());

                Thread.sleep(MINUTES.toMillis(testDurationMinutes));

                return;
            }

            Thread.sleep(100);
        }
    }

    @SuppressWarnings({"checkstyle:LineLength", "checkstyle:MethodLength"})
    TestResult printAndCollectStats(long testDurations, TimeUnit unit)
            throws IOException, InterruptedException {
        long startTime = System.nanoTime();

        // Print report stats
        long oldTime = System.nanoTime();

        long testEndTime = testDurations > 0 ? startTime + unit.toNanos(testDurations) : Long.MAX_VALUE;

        TestResult result = new TestResult();
        result.workload = workload.name;
        result.driver = driverName;
        result.topics = workload.topics;
        result.partitions = workload.partitionsPerTopic;
        result.messageSize = effectiveMessageSize();
        result.producersPerTopic = workload.producersPerTopic;
        result.consumersPerTopic = workload.consumerPerSubscription;

        while (true) {
            waitForReportInterval();

            PeriodStats stats = worker.getPeriodStats();

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;

            double publishRate = stats.messagesSent() / elapsed;
            double publishThroughput = stats.bytesSent() / elapsed / 1024 / 1024;
            double errorRate = stats.messageSendErrors() / elapsed;

            double consumeRate = stats.messagesReceived() / elapsed;
            double consumeThroughput = stats.bytesReceived() / elapsed / 1024 / 1024;

            long currentBacklog =
                    Math.max(
                            0L,
                            workload.subscriptionsPerTopic * stats.totalMessagesSent()
                                    - stats.totalMessagesReceived());

            Histogram publishLatency = stats.publishLatency();
            Histogram publishDelayLatency = stats.publishDelayLatency();
            Histogram endToEndLatency = stats.endToEndLatency();

            log.info(
                    "Pub rate {} msg/s / {} MB/s | Pub err {} err/s | Cons rate {} msg/s / {} MB/s | Backlog: {} K | Pub Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {} | Pub Delay Latency (us) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}",
                    rateFormat.format(publishRate),
                    throughputFormat.format(publishThroughput),
                    rateFormat.format(errorRate),
                    rateFormat.format(consumeRate),
                    throughputFormat.format(consumeThroughput),
                    dec.format(currentBacklog / 1000.0), //
                    dec.format(microsToMillis(publishLatency.getMean())),
                    dec.format(microsToMillis(publishLatency.getValueAtPercentile(50))),
                    dec.format(microsToMillis(publishLatency.getValueAtPercentile(99))),
                    dec.format(microsToMillis(publishLatency.getValueAtPercentile(99.9))),
                    throughputFormat.format(microsToMillis(publishLatency.getMaxValue())),
                    dec.format(publishDelayLatency.getMean()),
                    dec.format(publishDelayLatency.getValueAtPercentile(50)),
                    dec.format(publishDelayLatency.getValueAtPercentile(99)),
                    dec.format(publishDelayLatency.getValueAtPercentile(99.9)),
                    throughputFormat.format(publishDelayLatency.getMaxValue()));

            result.publishRate.add(publishRate);
            result.publishErrorRate.add(errorRate);
            result.consumeRate.add(consumeRate);
            result.backlog.add(currentBacklog);
            result.publishLatencyAvg.add(microsToMillis(publishLatency.getMean()));
            result.publishLatency50pct.add(microsToMillis(publishLatency.getValueAtPercentile(50)));
            result.publishLatency75pct.add(microsToMillis(publishLatency.getValueAtPercentile(75)));
            result.publishLatency95pct.add(microsToMillis(publishLatency.getValueAtPercentile(95)));
            result.publishLatency99pct.add(microsToMillis(publishLatency.getValueAtPercentile(99)));
            result.publishLatency999pct.add(microsToMillis(publishLatency.getValueAtPercentile(99.9)));
            result.publishLatency9999pct.add(microsToMillis(publishLatency.getValueAtPercentile(99.99)));
            result.publishLatencyMax.add(microsToMillis(publishLatency.getMaxValue()));

            result.publishDelayLatencyAvg.add(publishDelayLatency.getMean());
            result.publishDelayLatency50pct.add(publishDelayLatency.getValueAtPercentile(50));
            result.publishDelayLatency75pct.add(publishDelayLatency.getValueAtPercentile(75));
            result.publishDelayLatency95pct.add(publishDelayLatency.getValueAtPercentile(95));
            result.publishDelayLatency99pct.add(publishDelayLatency.getValueAtPercentile(99));
            result.publishDelayLatency999pct.add(publishDelayLatency.getValueAtPercentile(99.9));
            result.publishDelayLatency9999pct.add(publishDelayLatency.getValueAtPercentile(99.99));
            result.publishDelayLatencyMax.add(publishDelayLatency.getMaxValue());

            result.endToEndLatencyAvg.add(microsToMillis(endToEndLatency.getMean()));
            result.endToEndLatency50pct.add(microsToMillis(endToEndLatency.getValueAtPercentile(50)));
            result.endToEndLatency75pct.add(microsToMillis(endToEndLatency.getValueAtPercentile(75)));
            result.endToEndLatency95pct.add(microsToMillis(endToEndLatency.getValueAtPercentile(95)));
            result.endToEndLatency99pct.add(microsToMillis(endToEndLatency.getValueAtPercentile(99)));
            result.endToEndLatency999pct.add(microsToMillis(endToEndLatency.getValueAtPercentile(99.9)));
            result.endToEndLatency9999pct.add(
                    microsToMillis(endToEndLatency.getValueAtPercentile(99.99)));
            result.endToEndLatencyMax.add(microsToMillis(endToEndLatency.getMaxValue()));

            if (now >= testEndTime && !needToWaitForBacklogDraining) {
                CumulativeLatencies agg = worker.getCumulativeLatencies();
                log.info(
                        "----- Aggregated Pub Latency (ms) avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {} | Pub Delay (us)  avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {}",
                        dec.format(agg.publishLatency.getMean() / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(50) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(95) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(99) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(99.9) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(99.99) / 1000.0),
                        throughputFormat.format(agg.publishLatency.getMaxValue() / 1000.0),
                        dec.format(agg.publishDelayLatency.getMean()),
                        dec.format(agg.publishDelayLatency.getValueAtPercentile(50)),
                        dec.format(agg.publishDelayLatency.getValueAtPercentile(95)),
                        dec.format(agg.publishDelayLatency.getValueAtPercentile(99)),
                        dec.format(agg.publishDelayLatency.getValueAtPercentile(99.9)),
                        dec.format(agg.publishDelayLatency.getValueAtPercentile(99.99)),
                        throughputFormat.format(agg.publishDelayLatency.getMaxValue()));

                result.aggregatedPublishLatencyAvg = agg.publishLatency.getMean() / 1000.0;
                result.aggregatedPublishLatency50pct = agg.publishLatency.getValueAtPercentile(50) / 1000.0;
                result.aggregatedPublishLatency75pct = agg.publishLatency.getValueAtPercentile(75) / 1000.0;
                result.aggregatedPublishLatency95pct = agg.publishLatency.getValueAtPercentile(95) / 1000.0;
                result.aggregatedPublishLatency99pct = agg.publishLatency.getValueAtPercentile(99) / 1000.0;
                result.aggregatedPublishLatency999pct =
                        agg.publishLatency.getValueAtPercentile(99.9) / 1000.0;
                result.aggregatedPublishLatency9999pct =
                        agg.publishLatency.getValueAtPercentile(99.99) / 1000.0;
                result.aggregatedPublishLatencyMax = agg.publishLatency.getMaxValue() / 1000.0;

                result.aggregatedPublishDelayLatencyAvg = agg.publishDelayLatency.getMean();
                result.aggregatedPublishDelayLatency50pct =
                        agg.publishDelayLatency.getValueAtPercentile(50);
                result.aggregatedPublishDelayLatency75pct =
                        agg.publishDelayLatency.getValueAtPercentile(75);
                result.aggregatedPublishDelayLatency95pct =
                        agg.publishDelayLatency.getValueAtPercentile(95);
                result.aggregatedPublishDelayLatency99pct =
                        agg.publishDelayLatency.getValueAtPercentile(99);
                result.aggregatedPublishDelayLatency999pct =
                        agg.publishDelayLatency.getValueAtPercentile(99.9);
                result.aggregatedPublishDelayLatency9999pct =
                        agg.publishDelayLatency.getValueAtPercentile(99.99);
                result.aggregatedPublishDelayLatencyMax = agg.publishDelayLatency.getMaxValue();

                result.aggregatedEndToEndLatencyAvg = agg.endToEndLatency.getMean() / 1000.0;
                result.aggregatedEndToEndLatency50pct =
                        agg.endToEndLatency.getValueAtPercentile(50) / 1000.0;
                result.aggregatedEndToEndLatency75pct =
                        agg.endToEndLatency.getValueAtPercentile(75) / 1000.0;
                result.aggregatedEndToEndLatency95pct =
                        agg.endToEndLatency.getValueAtPercentile(95) / 1000.0;
                result.aggregatedEndToEndLatency99pct =
                        agg.endToEndLatency.getValueAtPercentile(99) / 1000.0;
                result.aggregatedEndToEndLatency999pct =
                        agg.endToEndLatency.getValueAtPercentile(99.9) / 1000.0;
                result.aggregatedEndToEndLatency9999pct =
                        agg.endToEndLatency.getValueAtPercentile(99.99) / 1000.0;
                result.aggregatedEndToEndLatencyMax = agg.endToEndLatency.getMaxValue() / 1000.0;

                agg.publishLatency
                        .percentiles(100)
                        .forEach(
                                value -> {
                                    result.aggregatedPublishLatencyQuantiles.put(
                                            value.getPercentile(), value.getValueIteratedTo() / 1000.0);
                                });

                agg.publishDelayLatency
                        .percentiles(100)
                        .forEach(
                                value -> {
                                    result.aggregatedPublishDelayLatencyQuantiles.put(
                                            value.getPercentile(), value.getValueIteratedTo());
                                });

                agg.endToEndLatency
                        .percentiles(100)
                        .forEach(
                                value -> {
                                    result.aggregatedEndToEndLatencyQuantiles.put(
                                            value.getPercentile(), microsToMillis(value.getValueIteratedTo()));
                                });

                break;
            }

            oldTime = now;
        }

        return result;
    }

    private int effectiveMessageSize() {
        return workload.usesMessageSizeDistribution()
                ? new MessageSizeDistribution(workload.messageSizeDistribution).getAvgSize()
                : workload.messageSize;
    }

    private static final DecimalFormat rateFormat = new PaddingDecimalFormat("0.0", 7);
    private static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 4);
    private static final DecimalFormat dec = new PaddingDecimalFormat("0.0", 4);

    private static double microsToMillis(double timeInMicros) {
        return timeInMicros / 1000.0;
    }

    private static double microsToMillis(long timeInMicros) {
        return timeInMicros / 1000.0;
    }

    private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);
}
