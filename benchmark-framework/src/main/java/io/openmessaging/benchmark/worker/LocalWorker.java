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

import static java.util.stream.Collectors.toList;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.DriverConfiguration;
import io.openmessaging.benchmark.driver.AbandonableCreation;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkDriver.ConsumerInfo;
import io.openmessaging.benchmark.driver.BenchmarkDriver.ProducerInfo;
import io.openmessaging.benchmark.driver.BenchmarkDriver.TopicInfo;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreationCompletion;
import io.openmessaging.benchmark.utils.RandomGenerator;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.UniformRateLimiter;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;
import io.openmessaging.benchmark.utils.payload.MessageSizeDistribution;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.yaml.YAMLFactory;

public final class LocalWorker implements Worker, ConsumerCallback {

    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DEFAULT_RESOURCE_CLOSE_TIMEOUT = Duration.ofSeconds(45);
    private static final Duration DEFAULT_PROBE_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_RESET_TIMEOUT = Duration.ofSeconds(10);

    private final Object lifecycleLock = new Object();
    private final Object stopLock = new Object();
    private final ExecutorService executor =
            Executors.newCachedThreadPool(Thread.ofVirtual().name("local-worker-", 0).factory());
    private final WorkerStats stats;
    private final long shutdownTimeoutNanos;
    private final long resourceCloseTimeoutNanos;
    private final long probeTimeoutNanos;
    private final long resetTimeoutNanos;
    private volatile Run run;
    private volatile boolean closed;
    private volatile boolean unusable;

    public LocalWorker() {
        this(new PrometheusMeterRegistry(l -> null));
    }

    public LocalWorker(PrometheusMeterRegistry statsLogger) {
        this(
                new WorkerStats(statsLogger),
                DEFAULT_SHUTDOWN_TIMEOUT,
                DEFAULT_PROBE_TIMEOUT,
                DEFAULT_RESET_TIMEOUT,
                DEFAULT_RESOURCE_CLOSE_TIMEOUT);
    }

    LocalWorker(
            PrometheusMeterRegistry statsLogger, Duration shutdownTimeout, Duration probeTimeout) {
        this(new WorkerStats(statsLogger), shutdownTimeout, probeTimeout, DEFAULT_RESET_TIMEOUT);
    }

    LocalWorker(
            WorkerStats stats, Duration shutdownTimeout, Duration probeTimeout, Duration resetTimeout) {
        this(stats, shutdownTimeout, probeTimeout, resetTimeout, shutdownTimeout);
    }

    LocalWorker(
            WorkerStats stats,
            Duration shutdownTimeout,
            Duration probeTimeout,
            Duration resetTimeout,
            Duration resourceCloseTimeout) {
        this.stats = Objects.requireNonNull(stats);
        shutdownTimeoutNanos = shutdownTimeout.toNanos();
        resourceCloseTimeoutNanos = resourceCloseTimeout.toNanos();
        probeTimeoutNanos = probeTimeout.toNanos();
        resetTimeoutNanos = resetTimeout.toNanos();
        if (shutdownTimeoutNanos <= 0
                || resourceCloseTimeoutNanos <= 0
                || probeTimeoutNanos <= 0
                || resetTimeoutNanos <= 0) {
            throw new IllegalArgumentException("Lifecycle timeouts must be positive");
        }
    }

    @Override
    public void initializeDriver(Path driverConfigFile) throws Exception {
        Run next = new Run();
        synchronized (lifecycleLock) {
            if (closed || unusable) {
                throw new IllegalStateException("LocalWorker is closed or could not be safely stopped");
            }
            if (run != null) {
                throw new IllegalStateException("Stop the previous driver before initializing another");
            }
            run = next;
        }
        awaitTask(
                submit(
                        next,
                        "driver initialization",
                        () -> {
                            DriverConfiguration configuration;
                            try (var input = Files.newInputStream(driverConfigFile)) {
                                configuration = reader.readValue(input);
                            }
                            log.info("Initializing driver: {}", writer.writeValueAsString(configuration));
                            if (configuration.driverClass == null || configuration.driverClass.isEmpty()) {
                                throw new IllegalArgumentException(
                                        "Driver configuration must specify a driverClass");
                            }
                            BenchmarkDriver driver;
                            try {
                                driver =
                                        (BenchmarkDriver)
                                                Class.forName(configuration.driverClass)
                                                        .getDeclaredConstructor()
                                                        .newInstance();
                            } catch (ClassNotFoundException e) {
                                throw new IllegalArgumentException(
                                        "Driver class " + configuration.driverClass + " is not on the classpath", e);
                            }
                            boolean stopped;
                            synchronized (next.monitor) {
                                stopped = next.stopping;
                                if (!stopped) {
                                    next.driver = driver;
                                }
                            }
                            if (stopped) {
                                try {
                                    driver.close();
                                } catch (Exception e) {
                                    next.recordCleanupFailure(e);
                                    throw e;
                                }
                                throw new CancellationException("Driver initialization was stopped");
                            }
                            driver.initialize(driverConfigFile, stats.getMeterRegistry());
                            synchronized (next.monitor) {
                                next.initialized = true;
                            }
                            return null;
                        }));
    }

    /**
     * Drivers always run on this JVM's classpath. Use {@link ProcessWorker} to run a driver from its
     * own plugin bundle.
     */
    @Override
    public void initializeDriver(Path driverConfigFile, Path driverLocation) throws Exception {
        if (driverLocation != null) {
            throw new IllegalArgumentException(
                    "LocalWorker loads drivers from its own classpath; use ProcessWorker for "
                            + driverLocation);
        }
        initializeDriver(driverConfigFile);
    }

    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) {
        Run current = requireRun();
        Timer timer = new Timer();

        List<TopicInfo> topicInfos =
                IntStream.range(0, topicsInfo.numberOfTopics)
                        .mapToObj(
                                i ->
                                        new TopicInfo(
                                                generateTopicName(current, i), topicsInfo.numberOfPartitionsPerTopic))
                        .toList();

        create(
                current, "topic creation", () -> current.driver.createTopics(topicInfos), v -> {}, v -> {});

        List<String> topics = topicInfos.stream().map(TopicInfo::topic).collect(toList());

        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());
        return topics;
    }

    private String generateTopicName(Run current, int i) {
        return String.format(
                "%s-%07d-%s", current.driver.getTopicNamePrefix(), i, RandomGenerator.getRandomString());
    }

    @Override
    public void createProducers(List<String> topics) {
        Run current = requireRun();
        Timer timer = new Timer();
        AtomicInteger index = new AtomicInteger();

        create(
                current,
                "producer creation",
                () ->
                        current.driver.createProducers(
                                topics.stream().map(t -> new ProducerInfo(index.getAndIncrement(), t)).toList()),
                created -> {
                    validateResources(created, "producer");
                    current.producers.addAll(created);
                },
                LocalWorker::closeUntransferred);

        log.info("Created {} producers in {} ms", topics.size(), timer.elapsedMillis());
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) {
        Run current = requireRun();
        Timer timer = new Timer();
        AtomicInteger index = new AtomicInteger();

        create(
                current,
                "consumer creation",
                () ->
                        current.driver.createConsumers(
                                consumerAssignment.topicsSubscriptions.stream()
                                        .map(
                                                c ->
                                                        new ConsumerInfo(
                                                                index.getAndIncrement(),
                                                                c.topic,
                                                                c.subscription,
                                                                current.callbacks))
                                        .toList()),
                created -> {
                    validateResources(created, "consumer");
                    current.consumers.addAll(created);
                },
                LocalWorker::closeUntransferred);

        log.info(
                "Created {} consumers in {} ms",
                consumerAssignment.topicsSubscriptions.size(),
                timer.elapsedMillis());
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        Run current = requireRun();
        List<BenchmarkProducer> producers;
        synchronized (current.monitor) {
            current.checkRunning();
            if (current.loadStarted) {
                throw new IllegalStateException("Load has already started for this driver");
            }
            producers = List.copyOf(current.producers);
        }
        log.info(
                "Starting load with {} producers, publish rate: {}",
                producers.size(),
                producerWorkAssignment.publishRate());

        int processors = Runtime.getRuntime().availableProcessors();

        updateMessageProducer(current, producerWorkAssignment.publishRate());
        List<byte[]> payloads = producerWorkAssignment.payloadData();
        if (payloads.isEmpty()) {
            throw new IllegalArgumentException("At least one payload is required");
        }

        int[] weights = producerWorkAssignment.payloadWeights();
        int[] cumulativeWeights = null;
        int totalWeight = payloads.size();
        if (weights != null) {
            cumulativeWeights = new int[weights.length];
            totalWeight = 0;
            for (int i = 0; i < weights.length; i++) {
                if (weights[i] < 0) {
                    throw new IllegalArgumentException("Payload weights cannot be negative");
                }
                try {
                    totalWeight = Math.addExact(totalWeight, weights[i]);
                } catch (ArithmeticException e) {
                    throw new IllegalArgumentException("Payload weights exceed the supported total", e);
                }
                cumulativeWeights[i] = totalWeight;
            }
            if (totalWeight == 0) {
                throw new IllegalArgumentException("Payload weights must sum to a positive value");
            }
        }
        final int[] payloadCumulativeWeights = cumulativeWeights;
        final int payloadTotalWeight = totalWeight;

        synchronized (current.monitor) {
            current.checkRunning();
            if (current.loadStarted) {
                throw new IllegalStateException("Load has already started for this driver");
            }
            current.loadStarted = true;
        }

        Map<Integer, List<BenchmarkProducer>> processorAssignment = new TreeMap<>();

        int processorIdx = 0;
        for (BenchmarkProducer p : producers) {
            processorAssignment
                    .computeIfAbsent(processorIdx, x -> new ArrayList<BenchmarkProducer>())
                    .add(p);

            processorIdx = (processorIdx + 1) % processors;
        }

        processorAssignment
                .values()
                .forEach(
                        benchmarkProducers ->
                                submitProducersToExecutor(
                                        current,
                                        benchmarkProducers,
                                        KeyDistributor.build(producerWorkAssignment.keyDistributorType()),
                                        payloads,
                                        payloadCumulativeWeights,
                                        payloadTotalWeight));
    }

    @Override
    public void probeProducers() throws IOException {
        Run current = requireRun();
        List<BenchmarkProducer> producers;
        synchronized (current.monitor) {
            current.checkRunning();
            producers = List.copyOf(current.producers);
        }
        CompletableFuture<Void> complete = new CompletableFuture<>();
        AtomicInteger remaining = new AtomicInteger(producers.size());
        List<TrackedTask<Void>> probes = new ArrayList<>();
        if (producers.isEmpty()) {
            complete.complete(null);
        }
        long deadline = System.nanoTime() + probeTimeoutNanos;
        boolean succeeded = false;
        try {
            for (BenchmarkProducer producer : producers) {
                if (complete.isCompletedExceptionally()) {
                    break;
                }
                if (deadline - System.nanoTime() <= 0) {
                    throw new TimeoutException("Producer readiness probes timed out");
                }
                probes.add(
                        submit(
                                current,
                                "producer readiness probe",
                                () -> {
                                    if (!current.callbacks.beginSend()) {
                                        complete.completeExceptionally(new CancellationException("Worker is stopping"));
                                        return null;
                                    }
                                    try {
                                        CompletableFuture<Void> send =
                                                Objects.requireNonNull(
                                                        producer.sendAsync(Optional.of("key"), new byte[10]),
                                                        "sendAsync returned null");
                                        send.whenComplete(
                                                (ignored, failure) -> {
                                                    current.callbacks.completeProbe(failure);
                                                    if (failure != null) {
                                                        complete.completeExceptionally(unwrap(failure));
                                                    } else if (remaining.decrementAndGet() == 0) {
                                                        complete.complete(null);
                                                    }
                                                });
                                    } catch (Throwable t) {
                                        current.callbacks.abortSend();
                                        complete.completeExceptionally(t);
                                    }
                                    return null;
                                }));
            }
            long waitNanos = deadline - System.nanoTime();
            if (waitNanos <= 0) {
                throw new TimeoutException("Producer readiness probes timed out");
            }
            complete.get(waitNanos, TimeUnit.NANOSECONDS);
            succeeded = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while probing producers", e);
        } catch (Exception e) {
            throw new IOException("Producer readiness probe failed", unwrap(e));
        } finally {
            if (!succeeded) {
                probes.forEach(probe -> probe.cancel(true));
            }
        }
    }

    private void submitProducersToExecutor(
            Run current,
            List<BenchmarkProducer> producers,
            KeyDistributor keyDistributor,
            List<byte[]> payloads,
            int[] cumulativeWeights,
            int totalWeight) {
        log.info("Submitting {} producers to executor", producers.size());

        for (BenchmarkProducer p : producers) {
            submit(
                    current,
                    "producer task",
                    () -> {
                        try {
                            ThreadLocalRandom random = ThreadLocalRandom.current();
                            while (!current.stopping) {
                                int payloadIndex =
                                        cumulativeWeights == null
                                                ? random.nextInt(payloads.size())
                                                : MessageSizeDistribution.bucketIndexForTicket(
                                                        random.nextInt(totalWeight), cumulativeWeights);
                                current.messageProducer.sendMessage(
                                        p, Optional.ofNullable(keyDistributor.next()), payloads.get(payloadIndex));
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            if (!current.stopping) {
                                current.recordFailure(e);
                            }
                        } catch (Throwable t) {
                            current.recordFailure(t);
                        }
                        return null;
                    });
        }
    }

    @Override
    public void adjustPublishRate(double publishRate) {
        Run current = requireRun();
        if (publishRate < 1.0) {
            updateMessageProducer(current, 1.0);
            return;
        }
        updateMessageProducer(current, publishRate);
    }

    private void updateMessageProducer(Run current, double publishRate) {
        synchronized (current.monitor) {
            current.checkRunning();
            current.messageProducer =
                    new MessageProducer(new UniformRateLimiter(publishRate), current.callbacks);
        }
    }

    @Override
    public PeriodStats getPeriodStats() {
        requireRun().checkFailure();
        return stats.toPeriodStats();
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        requireRun().checkFailure();
        return stats.toCumulativeLatencies();
    }

    @Override
    public CountersStats getCountersStats() {
        requireRun().checkFailure();
        return stats.toCountersStats();
    }

    @Override
    public void messageReceived(byte[] data, long publishTimestamp) {
        Run current = run;
        if (current != null) {
            current.callbacks.messageReceived(data, publishTimestamp);
        }
    }

    @Override
    public void messageReceived(ByteBuffer data, long publishTimestamp) {
        Run current = run;
        if (current != null) {
            current.callbacks.messageReceived(data, publishTimestamp);
        }
    }

    public void internalMessageReceived(int size, long publishTimestamp) {
        Run current = run;
        if (current != null) {
            current.callbacks.receive(size, publishTimestamp);
        }
    }

    @Override
    public void pauseConsumers() throws IOException {
        requireRun().callbacks.pause();
        log.info("Pausing consumers");
    }

    @Override
    public void resumeConsumers() throws IOException {
        requireRun().callbacks.resume();
        log.info("Resuming consumers");
    }

    @Override
    public void resetStats() throws IOException {
        requireRun().checkFailure();
        stats.resetLatencies();
    }

    @Override
    public void stopAll() {
        synchronized (stopLock) {
            Run current = run;
            if (current == null) {
                return;
            }
            Failures cleanup = new Failures();
            boolean interrupted = Thread.interrupted();
            if (interrupted) {
                cleanup.add(new InterruptedException("Interrupted before stopping the worker"));
            }
            List<TrackedTask<?>> tasks;
            synchronized (current.monitor) {
                current.stopping = true;
                tasks = List.copyOf(current.tasks);
            }
            try {
                if (!current.callbacks.seal(deadline())) {
                    cleanup.add(new TimeoutException("Callback gate did not quiesce"));
                }
            } catch (InterruptedException e) {
                interrupted = true;
                cleanup.add(e);
            }
            tasks.forEach(task -> task.cancel(true));

            long taskDeadline = deadline();
            boolean tasksExited = true;
            for (TrackedTask<?> task : tasks) {
                try {
                    if (!task.awaitExit(taskDeadline)) {
                        cleanup.add(
                                new TimeoutException(task.description + " did not exit after cancellation"));
                        tasksExited = false;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                    cleanup.add(e);
                    tasksExited = false;
                }
            }

            List<AutoCloseable> handles = new ArrayList<>();
            BenchmarkDriver driver;
            synchronized (current.monitor) {
                handles.addAll(current.producers);
                handles.addAll(current.consumers);
                current.producers.clear();
                current.consumers.clear();
                driver = current.driver;
                current.driver = null;
            }
            interrupted |= closeResources(handles, "producer or consumer", cleanup);
            if (driver != null) {
                interrupted |= closeResources(List.of(driver), "driver", cleanup);
            }

            boolean creationsFinished = false;
            try {
                creationsFinished = current.awaitCreations(deadline());
                if (!creationsFinished) {
                    cleanup.add(new TimeoutException("Resource creation callbacks did not finish"));
                }
            } catch (InterruptedException e) {
                interrupted = true;
                cleanup.add(e);
            }
            boolean callbacksFinished = false;
            try {
                callbacksFinished = current.callbacks.awaitQuiescence(deadline());
                if (!callbacksFinished) {
                    cleanup.add(
                            new TimeoutException("Outstanding send or consumer callbacks did not finish"));
                }
            } catch (InterruptedException e) {
                interrupted = true;
                cleanup.add(e);
            }
            synchronized (current.monitor) {
                cleanup.add(current.cleanupFailure);
            }

            if (tasksExited && creationsFinished && callbacksFinished) {
                interrupted |= resetStatistics(cleanup);
            }
            if (cleanup.hasFailure()) {
                unusable = true;
            }
            synchronized (lifecycleLock) {
                if (run == current) {
                    run = null;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            Failures failures = new Failures();
            synchronized (current.monitor) {
                if (!current.failureReported) {
                    failures.add(current.failure);
                    current.failureReported = current.failure != null;
                }
            }
            failures.add(cleanup.failure);
            failures.throwIfAny("Failed to stop worker");
        }
    }

    @Override
    public String id() {
        return "local";
    }

    @Override
    public boolean canReuseAfterFailure() {
        return !closed && !unusable && run == null;
    }

    @Override
    public void close() throws Exception {
        synchronized (lifecycleLock) {
            closed = true;
        }
        Failures failures = new Failures();
        try {
            stopAll();
        } catch (Throwable t) {
            failures.add(t);
        }
        executor.shutdownNow();
        boolean interrupted = Thread.interrupted();
        if (interrupted) {
            failures.add(new InterruptedException("Interrupted before closing the worker executor"));
        }
        try {
            if (!executor.awaitTermination(shutdownTimeoutNanos, TimeUnit.NANOSECONDS)) {
                failures.add(new TimeoutException("Local worker executor did not terminate"));
            }
        } catch (InterruptedException e) {
            interrupted = true;
            failures.add(e);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        failures.throwIfAny("Failed to close worker");
    }

    private Run requireRun() {
        Run current = run;
        if (current == null) {
            throw new IllegalStateException("No driver is initialized");
        }
        synchronized (current.monitor) {
            current.checkRunning();
            if (!current.initialized) {
                throw new IllegalStateException("Driver initialization has not completed");
            }
        }
        return current;
    }

    private <T> TrackedTask<T> submit(Run current, String description, Callable<T> operation) {
        TrackedTask<T> task = new TrackedTask<>(description, operation);
        synchronized (current.monitor) {
            current.checkRunning();
            current.tasks.add(task);
            try {
                executor.execute(task);
            } catch (RuntimeException e) {
                current.tasks.remove(task);
                task.cancel(false);
                throw e;
            }
        }
        return task;
    }

    private static <T> T awaitTask(TrackedTask<T> task) throws Exception {
        try {
            return task.get();
        } catch (InterruptedException e) {
            task.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException(task.description + " failed", cause);
        }
    }

    private <T> T create(
            Run current,
            String description,
            Supplier<CompletableFuture<T>> factory,
            ResourceAction<T> transfer,
            ResourceAction<T> dispose) {
        TrackedTask<T> task =
                submit(
                        current,
                        description,
                        () -> {
                            CompletableFuture<T> created =
                                    Objects.requireNonNull(factory.get(), description + " returned null");
                            CompletableFuture<T> handedOff = new CompletableFuture<>();
                            CompletableFuture<Void> quiescence =
                                    created instanceof ResourceCreationCompletion completion
                                            ? Objects.requireNonNull(
                                                    completion.quiescence(), description + " quiescence returned null")
                                            : null;
                            synchronized (current.monitor) {
                                current.pendingCreations += quiescence == null ? 1 : 2;
                            }
                            if (quiescence != null) {
                                quiescence.whenComplete(
                                        (ignored, failure) -> {
                                            try {
                                                if (failure != null) {
                                                    current.recordCleanupFailure(unwrap(failure));
                                                }
                                            } finally {
                                                current.creationFinished();
                                            }
                                        });
                            }
                            created.whenComplete(
                                    (value, failure) -> {
                                        try {
                                            Throwable outcome = failure == null ? null : unwrap(failure);
                                            boolean untransferred = false;
                                            if (outcome == null) {
                                                synchronized (current.monitor) {
                                                    if (current.stopping) {
                                                        untransferred = true;
                                                        outcome = new CancellationException("Worker is stopping");
                                                    } else {
                                                        try {
                                                            transfer.accept(value);
                                                        } catch (Throwable t) {
                                                            untransferred = true;
                                                            outcome = t;
                                                        }
                                                    }
                                                }
                                                if (untransferred) {
                                                    try {
                                                        dispose.accept(value);
                                                    } catch (Throwable t) {
                                                        current.recordCleanupFailure(t);
                                                        if (outcome != t) {
                                                            outcome.addSuppressed(t);
                                                        }
                                                    }
                                                }
                                            } else if (quiescence == null) {
                                                for (Throwable suppressed : outcome.getSuppressed()) {
                                                    current.recordCleanupFailure(suppressed);
                                                }
                                            }
                                            if (outcome == null) {
                                                handedOff.complete(value);
                                            } else {
                                                handedOff.completeExceptionally(outcome);
                                            }
                                        } finally {
                                            current.creationFinished();
                                        }
                                    });
                            try {
                                return handedOff.get();
                            } catch (InterruptedException e) {
                                try {
                                    if (quiescence != null) {
                                        created.cancel(true);
                                    } else if (created instanceof AbandonableCreation creation) {
                                        creation.abandonCreation();
                                    }
                                } catch (Throwable failure) {
                                    current.recordCleanupFailure(failure);
                                }
                                Thread.currentThread().interrupt();
                                throw e;
                            }
                        });
        try {
            return awaitTask(task);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted during " + description, e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed during " + description, e);
        }
    }

    private static void validateResources(List<? extends AutoCloseable> resources, String kind) {
        Objects.requireNonNull(resources, kind + " creation returned null");
        if (resources.stream().anyMatch(Objects::isNull)) {
            throw new IllegalStateException(kind + " creation returned a null handle");
        }
    }

    private static void closeUntransferred(List<? extends AutoCloseable> resources) {
        if (resources == null) {
            return;
        }
        Failures failures = new Failures();
        for (AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Throwable t) {
                    failures.add(t);
                }
            }
        }
        failures.throwIfAny("Failed to close untransferred resources");
    }

    private boolean closeResources(
            List<? extends AutoCloseable> resources, String description, Failures failures) {
        List<TrackedTask<Void>> tasks = new ArrayList<>();
        for (AutoCloseable resource : resources) {
            TrackedTask<Void> task =
                    new TrackedTask<>(
                            description + " close",
                            () -> {
                                resource.close();
                                return null;
                            });
            try {
                executor.execute(task);
                tasks.add(task);
            } catch (RuntimeException e) {
                task.cancel(false);
                failures.add(e);
            }
        }
        boolean interrupted = false;
        long deadline = System.nanoTime() + resourceCloseTimeoutNanos;
        for (TrackedTask<Void> task : tasks) {
            try {
                if (!task.awaitExit(deadline)) {
                    task.cancel(true);
                    failures.add(new TimeoutException(task.description + " timed out"));
                } else {
                    awaitTask(task);
                }
            } catch (InterruptedException e) {
                interrupted = true;
                task.cancel(true);
                failures.add(e);
            } catch (Throwable t) {
                failures.add(t);
            }
        }
        return interrupted;
    }

    private boolean resetStatistics(Failures failures) {
        TrackedTask<Void> task =
                new TrackedTask<>(
                        "statistics reset",
                        () -> {
                            stats.reset();
                            return null;
                        });
        try {
            executor.execute(task);
            if (!task.awaitExit(System.nanoTime() + resetTimeoutNanos)) {
                task.cancel(true);
                failures.add(new TimeoutException("Statistics reset timed out"));
            } else {
                awaitTask(task);
            }
        } catch (InterruptedException e) {
            task.cancel(true);
            failures.add(e);
            return true;
        } catch (Throwable t) {
            task.cancel(true);
            failures.add(t);
        }
        return false;
    }

    private long deadline() {
        return System.nanoTime() + shutdownTimeoutNanos;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable result = failure;
        while ((result instanceof CompletionException || result instanceof ExecutionException)
                && result.getCause() != null) {
            result = result.getCause();
        }
        return result;
    }

    @FunctionalInterface
    private interface ResourceAction<T> {
        void accept(T value) throws Exception;
    }

    private final class Run {
        private final Object monitor = new Object();
        private final List<TrackedTask<?>> tasks = new ArrayList<>();
        private final List<BenchmarkProducer> producers = new ArrayList<>();
        private final List<BenchmarkConsumer> consumers = new ArrayList<>();
        private final RunCallbacks callbacks = new RunCallbacks(stats, this::recordFailure);
        private volatile boolean stopping;
        private volatile MessageProducer messageProducer;
        private BenchmarkDriver driver;
        private boolean initialized;
        private boolean loadStarted;
        private int pendingCreations;
        private Throwable failure;
        private boolean failureReported;
        private Throwable cleanupFailure;

        private void checkRunning() {
            if (stopping) {
                throw new IllegalStateException("Worker is stopping");
            }
            checkFailure();
        }

        private void checkFailure() {
            synchronized (monitor) {
                if (failure != null) {
                    failureReported = true;
                    throw new IllegalStateException("Producer or callback task failed", failure);
                }
            }
        }

        private void recordFailure(Throwable cause) {
            log.error("Producer or callback task failed", cause);
            synchronized (monitor) {
                failure = append(failure, cause);
            }
        }

        private void recordCleanupFailure(Throwable cause) {
            unusable = true;
            synchronized (monitor) {
                cleanupFailure = append(cleanupFailure, cause);
            }
        }

        private void creationFinished() {
            synchronized (monitor) {
                pendingCreations--;
                monitor.notifyAll();
            }
        }

        private boolean awaitCreations(long deadlineNanos) throws InterruptedException {
            synchronized (monitor) {
                while (pendingCreations != 0) {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0) {
                        return false;
                    }
                    TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
                }
                return true;
            }
        }
    }

    /**
     * Future cancellation only signals a task. This separate latch tracks when its body really exits,
     * including cancellation before the executor starts it.
     *
     * @param <T> task result type
     */
    private static final class TrackedTask<T> extends FutureTask<T> {
        private final String description;
        private final AtomicInteger state = new AtomicInteger();
        private final CountDownLatch exited = new CountDownLatch(1);

        private TrackedTask(String description, Callable<T> operation) {
            super(operation);
            this.description = description;
        }

        @Override
        public void run() {
            if (state.compareAndSet(0, 1)) {
                try {
                    super.run();
                } finally {
                    state.set(2);
                    exited.countDown();
                }
            }
        }

        @Override
        public boolean cancel(boolean interrupt) {
            boolean cancelled = super.cancel(interrupt);
            if (state.compareAndSet(0, 2)) {
                exited.countDown();
            }
            return cancelled;
        }

        private boolean awaitExit(long deadlineNanos) throws InterruptedException {
            long remaining = deadlineNanos - System.nanoTime();
            return exited.getCount() == 0
                    || (remaining > 0 && exited.await(remaining, TimeUnit.NANOSECONDS));
        }
    }

    private static Throwable append(Throwable primary, Throwable next) {
        if (next == null) {
            return primary;
        }
        if (primary == null) {
            return next;
        }
        if (primary != next) {
            primary.addSuppressed(next);
        }
        return primary;
    }

    private static final class Failures {
        private Throwable failure;

        private void add(Throwable cause) {
            failure = append(failure, cause);
        }

        private boolean hasFailure() {
            return failure != null;
        }

        private void throwIfAny(String message) {
            if (failure != null) {
                throw new IllegalStateException(message, failure);
            }
        }
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectReader reader =
            mapper
                    .readerFor(DriverConfiguration.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);
}
