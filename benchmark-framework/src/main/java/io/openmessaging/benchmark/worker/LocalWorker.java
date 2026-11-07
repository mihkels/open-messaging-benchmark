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

import static io.openmessaging.benchmark.driver.Failures.append;
import static io.openmessaging.benchmark.driver.Failures.unwrap;
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
import io.openmessaging.benchmark.driver.Failures;
import io.openmessaging.benchmark.driver.ResourceCreationCompletion;
import io.openmessaging.benchmark.utils.RandomGenerator;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.UniformRateLimiter;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

public final class LocalWorker implements Worker {

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
                            if (!next.attachDriver(driver)) {
                                try {
                                    driver.close();
                                } catch (Exception e) {
                                    next.recordCleanupFailure(e);
                                    throw e;
                                }
                                throw new CancellationException("Driver initialization was stopped");
                            }
                            driver.initialize(driverConfigFile, stats.getMeterRegistry());
                            next.markInitialized();
                            return null;
                        }));
    }

    @Override
    public void setRunId(String runId) {
        stats.setRunId(runId);
    }

    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) {
        Run current = requireRun();
        Timer timer = new Timer();

        List<TopicInfo> topicInfos =
                IntStream.range(0, topicsInfo.numberOfTopics)
                        .mapToObj(i ->
                                new TopicInfo(generateTopicName(current, i), topicsInfo.numberOfPartitionsPerTopic))
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
                    current.addProducers(created);
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
                                        .map(c -> {
                                            int consumerIndex = index.getAndIncrement();
                                            return new ConsumerInfo(
                                                    consumerIndex, c.topic, c.subscription, current.callbacks);
                                        })
                                        .toList()),
                created -> {
                    validateResources(created, "consumer");
                    current.addConsumers(created);
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
        List<BenchmarkProducer> producers = current.producersBeforeLoad();
        log.info(
                "Starting load with {} producers, publish rate: {}",
                producers.size(),
                producerWorkAssignment.publishRate());

        current.replaceMessageProducer(producerWorkAssignment.publishRate());
        PayloadSampler payloads =
                PayloadSampler.of(
                        producerWorkAssignment.payloadData(), producerWorkAssignment.payloadWeights());
        current.beginLoad();

        log.info("Submitting {} producers to executor", producers.size());
        for (BenchmarkProducer producer : producers) {
            startProducer(current, producer, producerWorkAssignment.keyDistributorType(), payloads);
        }
    }

    @Override
    public void probeProducers() {
        Run current = requireRun();
        List<BenchmarkProducer> producers = current.producers();
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
                                () -> probe(current, producer, complete, remaining)));
            }
            long waitNanos = deadline - System.nanoTime();
            if (waitNanos <= 0) {
                throw new TimeoutException("Producer readiness probes timed out");
            }
            complete.get(waitNanos, TimeUnit.NANOSECONDS);
            succeeded = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while probing producers", e);
        } catch (Exception e) {
            throw new IllegalStateException("Producer readiness probe failed", unwrap(e));
        } finally {
            if (!succeeded) {
                probes.forEach(probe -> probe.cancel(true));
            }
        }
    }

    // Sends one readiness probe; the last successful probe completes {@code complete}.
    private static Void probe(
            Run current,
            BenchmarkProducer producer,
            CompletableFuture<Void> complete,
            AtomicInteger remaining) {
        if (!current.callbacks.beginSend()) {
            complete.completeExceptionally(new CancellationException("Worker is stopping"));
            return null;
        }
        try {
            CompletableFuture<Void> send =
                    Objects.requireNonNull(
                            producer.sendAsync(Optional.of("key"), new byte[10]), "sendAsync returned null");
            send.whenComplete((ignored, failure) -> {
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
    }

    // Sends messages from one producer on its own task until the worker stops.
    private void startProducer(
            Run current,
            BenchmarkProducer producer,
            KeyDistributorType keyDistributorType,
            PayloadSampler payloads) {
        // Each producer runs on its own thread, and distributors are not thread-safe
        KeyDistributor keyDistributor = KeyDistributor.build(keyDistributorType);
        submit(
                current,
                "producer task",
                () -> {
                    try {
                        ThreadLocalRandom random = ThreadLocalRandom.current();
                        while (!current.stopping) {
                            current.messageProducer.sendMessage(
                                    producer, Optional.ofNullable(keyDistributor.next()), payloads.next(random));
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

    @Override
    public void adjustPublishRate(double publishRate) {
        requireRun().replaceMessageProducer(Math.max(publishRate, 1.0));
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
    public void pauseConsumers() {
        requireRun().callbacks.pause();
        log.info("Pausing consumers");
    }

    @Override
    public void resumeConsumers() {
        requireRun().callbacks.resume();
        log.info("Resuming consumers");
    }

    @Override
    public void resetStats() {
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
            cleanup.deferInterrupt("Interrupted before stopping the worker");
            List<TrackedTask<?>> tasks = current.beginStopping();
            sealCallbacks(current, cleanup);
            boolean tasksExited = cancelTasks(tasks, cleanup);
            Detached detached = current.detach();
            closeResources(detached.handles(), "producer or consumer", cleanup);
            if (detached.driver() != null) {
                closeResources(List.of(detached.driver()), "driver", cleanup);
            }
            boolean creationsFinished = awaitCreations(current, cleanup);
            boolean callbacksFinished = awaitCallbacks(current, cleanup);
            cleanup.add(current.cleanupFailure());

            // A worker with failed cleanup is already unusable and may still receive stats from stuck
            // resources.
            if (!cleanup.hasFailure() && tasksExited && creationsFinished && callbacksFinished) {
                resetStatistics(cleanup);
            }
            if (cleanup.hasFailure()) {
                unusable = true;
            }
            synchronized (lifecycleLock) {
                if (run == current) {
                    run = null;
                }
            }
            cleanup.restoreInterrupt();
            Failures failures = new Failures();
            failures.add(current.takeUnreportedFailure());
            failures.add(cleanup.failure());
            failures.throwIfAny("Failed to stop worker");
        }
    }

    private void sealCallbacks(Run current, Failures cleanup) {
        try {
            if (!current.callbacks.seal(deadline())) {
                cleanup.add(new TimeoutException("Callback gate did not quiesce"));
            }
        } catch (InterruptedException e) {
            cleanup.addInterruption(e);
        }
    }

    // Returns whether every task exited after its cancellation.
    private boolean cancelTasks(List<TrackedTask<?>> tasks, Failures cleanup) {
        tasks.forEach(task -> task.cancel(true));
        long taskDeadline = deadline();
        boolean tasksExited = true;
        for (TrackedTask<?> task : tasks) {
            try {
                if (!task.awaitExit(taskDeadline)) {
                    cleanup.add(
                            new TimeoutException(task.description() + " did not exit after cancellation"));
                    tasksExited = false;
                }
            } catch (InterruptedException e) {
                cleanup.addInterruption(e);
                tasksExited = false;
            }
        }
        return tasksExited;
    }

    private boolean awaitCreations(Run current, Failures cleanup) {
        try {
            if (current.awaitCreations(deadline())) {
                return true;
            }
            cleanup.add(new TimeoutException("Resource creation callbacks did not finish"));
        } catch (InterruptedException e) {
            cleanup.addInterruption(e);
        }
        return false;
    }

    private boolean awaitCallbacks(Run current, Failures cleanup) {
        try {
            if (current.callbacks.awaitQuiescence(deadline())) {
                return true;
            }
            cleanup.add(new TimeoutException("Outstanding send or consumer callbacks did not finish"));
        } catch (InterruptedException e) {
            cleanup.addInterruption(e);
        }
        return false;
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
        failures.deferInterrupt("Interrupted before closing the worker executor");
        try {
            if (!executor.awaitTermination(shutdownTimeoutNanos, TimeUnit.NANOSECONDS)) {
                failures.add(new TimeoutException("Local worker executor did not terminate"));
            }
        } catch (InterruptedException e) {
            failures.addInterruption(e);
        }
        failures.throwIfAny("Failed to close worker");
    }

    private Run requireRun() {
        Run current = run;
        if (current == null) {
            throw new IllegalStateException("No driver is initialized");
        }
        current.requireInitialized();
        return current;
    }

    private <T> TrackedTask<T> submit(Run current, String description, Callable<T> operation) {
        TrackedTask<T> task = new TrackedTask<>(description, operation);
        current.start(task, executor);
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
            throw new IllegalStateException(task.description() + " failed", cause);
        }
    }

    private <T> T create(
            Run current,
            String description,
            Supplier<CompletableFuture<T>> factory,
            ResourceAction<T> transfer,
            ResourceAction<T> dispose) {
        ResourceHandoff<T> handoff =
                new ResourceHandoff<>(current, description, factory, transfer, dispose);
        TrackedTask<T> task = submit(current, description, handoff::create);
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

    /**
     * Hands the result of one asynchronous driver creation to a run. A resource that completes after
     * stopping has begun, or that the run rejects, is disposed of instead. Every creation callback is
     * counted on the run, so that stopping can wait for late completions.
     *
     * @param <T> the created resource type
     */
    private static final class ResourceHandoff<T> {
        private final Run run;
        private final String description;
        private final Supplier<CompletableFuture<T>> factory;
        private final ResourceAction<T> transfer;
        private final ResourceAction<T> dispose;
        private final CompletableFuture<T> handedOff = new CompletableFuture<>();

        private ResourceHandoff(
                Run run,
                String description,
                Supplier<CompletableFuture<T>> factory,
                ResourceAction<T> transfer,
                ResourceAction<T> dispose) {
            this.run = run;
            this.description = description;
            this.factory = factory;
            this.transfer = transfer;
            this.dispose = dispose;
        }

        // Runs as a worker task and waits until the creation has been handed off or has failed.
        private T create() throws Exception {
            CompletableFuture<T> created =
                    Objects.requireNonNull(factory.get(), description + " returned null");
            // A creation that reports quiescence reports its own late cleanup failures through it.
            CompletableFuture<Void> quiescence =
                    created instanceof ResourceCreationCompletion completion
                            ? Objects.requireNonNull(
                                    completion.quiescence(), description + " quiescence returned null")
                            : null;
            boolean tracksQuiescence = quiescence != null;
            run.creationsStarted(tracksQuiescence ? 2 : 1);
            if (tracksQuiescence) {
                quiescence.whenComplete((ignored, failure) -> quiesced(failure));
            }
            created.whenComplete((value, failure) -> completed(value, failure, tracksQuiescence));
            try {
                return handedOff.get();
            } catch (InterruptedException e) {
                abandon(created, tracksQuiescence);
                Thread.currentThread().interrupt();
                throw e;
            }
        }

        private void quiesced(Throwable failure) {
            try {
                if (failure != null) {
                    run.recordCleanupFailure(unwrap(failure));
                }
            } finally {
                run.creationFinished();
            }
        }

        private void completed(T value, Throwable failure, boolean tracksQuiescence) {
            try {
                Throwable outcome = failure == null ? null : unwrap(failure);
                if (outcome == null) {
                    outcome = run.transfer(value, transfer);
                    if (outcome != null) {
                        disposeUntransferred(value, outcome);
                    }
                } else if (!tracksQuiescence) {
                    for (Throwable suppressed : outcome.getSuppressed()) {
                        run.recordCleanupFailure(suppressed);
                    }
                }
                if (outcome == null) {
                    handedOff.complete(value);
                } else {
                    handedOff.completeExceptionally(outcome);
                }
            } finally {
                run.creationFinished();
            }
        }

        private void disposeUntransferred(T value, Throwable outcome) {
            try {
                dispose.accept(value);
            } catch (Throwable t) {
                run.recordCleanupFailure(t);
                if (outcome != t) {
                    outcome.addSuppressed(t);
                }
            }
        }

        private void abandon(CompletableFuture<T> created, boolean tracksQuiescence) {
            try {
                if (tracksQuiescence) {
                    created.cancel(true);
                } else if (created instanceof AbandonableCreation creation) {
                    creation.abandonCreation();
                }
            } catch (Throwable failure) {
                run.recordCleanupFailure(failure);
            }
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
        resources.forEach(failures::close);
        failures.throwIfAny("Failed to close untransferred resources");
    }

    private void closeResources(
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
        long deadline = System.nanoTime() + resourceCloseTimeoutNanos;
        for (TrackedTask<Void> task : tasks) {
            try {
                if (!task.awaitExit(deadline)) {
                    task.cancel(true);
                    failures.add(new TimeoutException(task.description() + " timed out"));
                } else {
                    awaitTask(task);
                }
            } catch (InterruptedException e) {
                task.cancel(true);
                failures.addInterruption(e);
            } catch (Throwable t) {
                failures.add(t);
            }
        }
    }

    private void resetStatistics(Failures failures) {
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
            failures.addInterruption(e);
        } catch (Throwable t) {
            task.cancel(true);
            failures.add(t);
        }
    }

    private long deadline() {
        return System.nanoTime() + shutdownTimeoutNanos;
    }

    @FunctionalInterface
    private interface ResourceAction<T> {
        // Wraps driver SPI close/transfer calls declared as throws Exception
        @SuppressWarnings("java:S112")
        void accept(T value) throws Exception;
    }

    /**
     * The state of one initialized driver, from {@code initializeDriver} until {@code stopAll}. Every
     * field written by more than one thread is guarded by {@link #monitor}, and only methods of this
     * class take it.
     */
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

        // Keeps a newly constructed driver unless stopping has begun. Returns false if the caller
        // still owns the driver and must close it.
        private boolean attachDriver(BenchmarkDriver newDriver) {
            synchronized (monitor) {
                if (stopping) {
                    return false;
                }
                driver = newDriver;
                return true;
            }
        }

        private void markInitialized() {
            synchronized (monitor) {
                initialized = true;
            }
        }

        private void requireInitialized() {
            synchronized (monitor) {
                checkRunning();
                if (!initialized) {
                    throw new IllegalStateException("Driver initialization has not completed");
                }
            }
        }

        // Starts a task on the executor and tracks it so that stopping can cancel and await it.
        private void start(TrackedTask<?> task, Executor executor) {
            synchronized (monitor) {
                checkRunning();
                tasks.add(task);
                try {
                    executor.execute(task);
                } catch (RuntimeException e) {
                    tasks.remove(task);
                    task.cancel(false);
                    throw e;
                }
            }
        }

        private void creationsStarted(int count) {
            synchronized (monitor) {
                pendingCreations += count;
            }
        }

        // Hands a created resource to this run unless stopping has begun. Returns null if the
        // resource was transferred, otherwise why it was not.
        private <T> Throwable transfer(T value, ResourceAction<T> transfer) {
            synchronized (monitor) {
                if (stopping) {
                    return new CancellationException("Worker is stopping");
                }
                try {
                    transfer.accept(value);
                    return null;
                } catch (Throwable t) {
                    return t;
                }
            }
        }

        // Must only be called from a transfer action, which holds the monitor.
        private void addProducers(List<BenchmarkProducer> created) {
            producers.addAll(created);
        }

        // Must only be called from a transfer action, which holds the monitor.
        private void addConsumers(List<BenchmarkConsumer> created) {
            consumers.addAll(created);
        }

        private List<BenchmarkProducer> producers() {
            synchronized (monitor) {
                checkRunning();
                return List.copyOf(producers);
            }
        }

        private List<BenchmarkProducer> producersBeforeLoad() {
            synchronized (monitor) {
                checkRunning();
                checkLoadNotStarted();
                return List.copyOf(producers);
            }
        }

        private void beginLoad() {
            synchronized (monitor) {
                checkRunning();
                checkLoadNotStarted();
                loadStarted = true;
            }
        }

        private void checkLoadNotStarted() {
            if (loadStarted) {
                throw new IllegalStateException("Load has already started for this driver");
            }
        }

        private void replaceMessageProducer(double publishRate) {
            synchronized (monitor) {
                checkRunning();
                messageProducer = new MessageProducer(new UniformRateLimiter(publishRate), callbacks);
            }
        }

        // Rejects new tasks, transfers and calls from now on. Returns the tasks started so far.
        private List<TrackedTask<?>> beginStopping() {
            synchronized (monitor) {
                stopping = true;
                return List.copyOf(tasks);
            }
        }

        // Takes ownership of the producer and consumer handles and the driver away from this run.
        // Returns the handles, and the driver if one was attached.
        private Detached detach() {
            synchronized (monitor) {
                List<AutoCloseable> handles = new ArrayList<>(producers);
                handles.addAll(consumers);
                producers.clear();
                consumers.clear();
                BenchmarkDriver detachedDriver = driver;
                driver = null;
                return new Detached(handles, detachedDriver);
            }
        }

        private Throwable cleanupFailure() {
            synchronized (monitor) {
                return cleanupFailure;
            }
        }

        // The failure of a producer or callback task that no caller has seen yet. Returns the
        // failure, or null if there is none or it was already reported.
        private Throwable takeUnreportedFailure() {
            synchronized (monitor) {
                if (failureReported) {
                    return null;
                }
                failureReported = failure != null;
                return failure;
            }
        }

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

    private record Detached(List<AutoCloseable> handles, BenchmarkDriver driver) {}

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectReader reader =
            mapper
                    .readerFor(DriverConfiguration.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);
}
