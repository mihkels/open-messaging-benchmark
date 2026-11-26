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

import io.github.mihkels.omb.driver.Failures;
import io.github.mihkels.omb.plugin.HostRuntime;
import io.github.mihkels.omb.plugin.PluginRegistry;
import io.github.mihkels.omb.plugin.ResolvedDriver;
import io.github.mihkels.omb.plugin.RuntimeClasspath;
import io.github.mihkels.omb.worker.commands.ConsumerAssignment;
import io.github.mihkels.omb.worker.commands.CountersStats;
import io.github.mihkels.omb.worker.commands.CumulativeLatencies;
import io.github.mihkels.omb.worker.commands.PeriodStats;
import io.github.mihkels.omb.worker.commands.ProducerWorkAssignment;
import io.github.mihkels.omb.worker.commands.RunContext;
import io.github.mihkels.omb.worker.commands.RunPhase;
import io.github.mihkels.omb.worker.commands.TopicsInfo;
import io.github.mihkels.omb.worker.commands.WorkerHealth;
import io.github.mihkels.omb.worker.commands.WorkerHealth.DriverState;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLMapper;

/**
 * A {@link Worker} that runs the selected driver in a dedicated child JVM whose classpath contains
 * only the benchmark core and the driver's plugin bundle(s). Every {@link #initializeDriver(Path)}
 * starts a fresh process, so different drivers and different versions of their client libraries
 * never share a classpath.
 *
 * <p>The child JVM options are taken from the {@value #JVM_OPTS_PROPERTY} system property or the
 * {@value #JVM_OPTS_ENV} environment variable, falling back to {@link JvmOptions#DEFAULTS}.
 */
public class ProcessWorker implements Worker {

    private static final Logger log = LoggerFactory.getLogger(ProcessWorker.class);

    public static final String JVM_OPTS_PROPERTY = JvmOptions.PROPERTY;
    public static final String JVM_OPTS_ENV = JvmOptions.ENV;
    public static final String STARTUP_TIMEOUT_PROPERTY = "omb.driver.startupTimeoutSeconds";

    private static final Duration CRASH_DETECTION_TIMEOUT = Duration.ofSeconds(2);
    private static final String DRIVER_CLASS = "driverClass";

    private static final ObjectMapper YAML = YAMLMapper.builder().build();

    private final @Nullable Path driverLocation;
    private final Supplier<PluginRegistry> registry;
    private final Supplier<HostRuntime> hostRuntime;
    private final Thread shutdownHook;

    private volatile boolean closed;
    private volatile boolean unusable;
    private volatile boolean initializing;
    private boolean stopped;
    private volatile @Nullable RunContext run;
    private volatile @Nullable RunPhase phase;
    // The last driver failure, reported by health() until the next driver initializes
    private volatile @Nullable String lastError;
    // The current driver process, kept until it is fully stopped so that termination can be retried
    private volatile @Nullable DriverProcess current;

    public ProcessWorker() {
        this(null);
    }

    /**
     * Creates a worker that loads every driver from an explicit location instead of the installed
     * plugin bundles.
     *
     * @param driverLocation a plugin bundle directory, unpackaged driver module directory or jar, or
     *     {@code null} to use the installed plugin bundles
     */
    public ProcessWorker(@Nullable Path driverLocation) {
        this(driverLocation, PluginRegistry::discover, HostRuntime::current);
    }

    ProcessWorker(Supplier<PluginRegistry> registry, Supplier<HostRuntime> hostRuntime) {
        this(null, registry, hostRuntime);
    }

    ProcessWorker(
            @Nullable Path driverLocation,
            Supplier<PluginRegistry> registry,
            Supplier<HostRuntime> hostRuntime) {
        this.driverLocation = driverLocation;
        this.registry = registry;
        this.hostRuntime = hostRuntime;
        this.shutdownHook = new Thread(this::shutdownDriverProcess, "process-worker-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    @Override
    public synchronized void initializeDriver(Path driverConfigFile) throws InterruptedException {
        ensureOpen();
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Interrupted before driver initialization");
        }
        terminate();

        Map<String, Object> config = readDriverConfig(driverConfigFile);
        // readDriverConfig rejects a configuration without a driverClass
        Object driverClass = Objects.requireNonNull(config.get(DRIVER_CLASS), DRIVER_CLASS);

        PluginRegistry plugins = registry.get();
        plugins.errors().forEach(error -> log.warn("Invalid driver plugin: {}", error));
        ResolvedDriver resolved =
                driverLocation == null
                        ? plugins.resolve(driverClass.toString(), config)
                        : plugins.resolveOverride(driverClass.toString(), config, driverLocation);
        RuntimeClasspath classpath = RuntimeClasspath.compose(hostRuntime.get(), resolved);
        classpath.warnings().forEach(w -> log.warn("{}", w));

        String description =
                resolved.driverClass()
                        + " from "
                        + resolved.bundles().stream()
                                .map(b -> "'" + b.id() + "' (" + b.directory() + ")")
                                .collect(Collectors.joining(", "));
        log.info("Starting driver process for {}", description);

        // Tags fixed at launch reach every meter; a driver process serves a single run
        Map<String, String> metricTags = new LinkedHashMap<>();
        RunContext launchRun = run;
        if (launchRun != null) {
            metricTags.put("run_id", launchRun.runId());
            String workload = launchRun.workload();
            if (workload != null && !workload.isBlank()) {
                metricTags.put("workload", workload);
            }
        }
        if (config.get("name") instanceof String name && !name.isBlank()) {
            metricTags.put("driver", name);
        }

        DriverProcess starting = new DriverProcess(description);
        current = starting;
        stopped = false;
        initializing = true;
        try {
            ensureOpen();
            starting.start(classpath.entries(), metricTags, () -> closed);
            HttpWorkerClient client =
                    starting
                            .client()
                            .orElseThrow(() -> new IllegalStateException("No driver client after start"));
            RunPhase currentPhase = phase;
            if (currentPhase != null) {
                client.setPhase(currentPhase);
            }
            starting.authorizeConfig(driverConfigFile);
            client.initializeDriverFromLocalPath(driverConfigFile);
            lastError = null;
        } catch (Exception e) {
            String failure = starting.describeFailure(e);
            lastError = "Failed to initialize driver " + description + ": " + failure;
            cleanUpFailedStart(starting, e);
            if (e instanceof InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            throw new IllegalStateException(
                    "Failed to initialize driver " + description + ": " + failure, e);
        } finally {
            initializing = false;
        }
    }

    private static Map<String, Object> readDriverConfig(Path driverConfigFile) {
        Map<String, Object> config;
        try {
            config = YAML.readValue(driverConfigFile.toFile(), new TypeReference<>() {});
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Cannot read driver configuration " + driverConfigFile + ": " + e.getMessage(), e);
        }
        Object driverClass = config == null ? null : config.get(DRIVER_CLASS);
        if (driverClass == null || driverClass.toString().isBlank()) {
            throw new IllegalArgumentException(
                    "Driver configuration " + driverConfigFile + " must specify a driverClass");
        }
        return config;
    }

    // Stops and terminates a driver process that failed to start, recording cleanup failures on it.
    private void cleanUpFailedStart(DriverProcess starting, Exception failure) {
        Optional<HttpWorkerClient> client = starting.client();
        if (client.isPresent() && starting.isRunning() && !closed) {
            try {
                client.get().stopAll();
            } catch (Throwable cleanup) {
                Failures.append(failure, cleanup);
            }
        }
        try {
            terminate();
        } catch (Throwable cleanup) {
            Failures.append(failure, cleanup);
        }
    }

    @Override
    public void setRunContext(RunContext run) {
        // Reaches the meters of the next driver process as a launch tag; a running one keeps its own
        this.run = run;
        this.phase = null;
    }

    @Override
    public void setPhase(RunPhase phase) {
        DriverProcess running;
        HttpWorkerClient client;
        synchronized (this) {
            this.phase = phase;
            running = current;
            Optional<HttpWorkerClient> started = running == null ? Optional.empty() : running.client();
            if (running == null || started.isEmpty()) {
                return;
            }
            client = started.get();
            if (running.hasExited()) {
                throw running.crashed(null);
            }
        }
        invoke(
                running,
                client,
                c -> {
                    c.setPhase(phase);
                    return null;
                });
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Cannot initialize a closed ProcessWorker");
        }
        if (unusable) {
            throw new IllegalStateException("Cannot reuse a ProcessWorker whose cleanup failed");
        }
    }

    /**
     * The pid of the running driver process.
     *
     * @return the pid, or empty if no driver process is running
     */
    public synchronized OptionalLong pid() {
        DriverProcess running = current;
        return running == null ? OptionalLong.empty() : running.pid();
    }

    /**
     * Prometheus metrics of the running driver process.
     *
     * <p>Reads only volatile fields so a scrape never waits for {@link #initializeDriver(Path)}.
     *
     * @return the metrics, or an empty string if no driver is running or it is still starting
     * @throws IllegalStateException if the metrics cannot be scraped
     */
    public String scrapeMetrics() {
        DriverProcess running = current;
        // health() and the next driver call report a crash
        if (running == null || running.hasExited()) {
            return "";
        }
        return running.client().map(HttpWorkerClient::scrapeMetrics).orElse("");
    }

    // Forwards a call to the driver process and reports a crash instead of the failed call.
    private <T extends @Nullable Object> T call(Function<HttpWorkerClient, T> action) {
        DriverProcess running;
        HttpWorkerClient client;
        synchronized (this) {
            running = current;
            Optional<HttpWorkerClient> started = running == null ? Optional.empty() : running.client();
            if (running == null || started.isEmpty()) {
                throw new IllegalStateException("No driver process is running; initialize a driver first");
            }
            client = started.get();
            if (running.hasExited()) {
                throw running.crashed(null);
            }
        }
        return invoke(running, client, action);
    }

    private <T extends @Nullable Object> T invoke(
            DriverProcess running, HttpWorkerClient client, Function<HttpWorkerClient, T> action) {
        try {
            return action.apply(client);
        } catch (RuntimeException e) {
            // A failed call is often the first sign of a crashed process; give it time to be reaped
            if (running.awaitExit(CRASH_DETECTION_TIMEOUT)) {
                IllegalStateException crash = running.crashed(e);
                lastError = crash.getMessage();
                throw crash;
            }
            lastError = running.describeFailure(e);
            throw e;
        }
    }

    private void run(Consumer<HttpWorkerClient> action) {
        call(client -> {
            action.accept(client);
            return null;
        });
    }

    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) {
        return call(c -> c.createTopics(topicsInfo));
    }

    @Override
    public void createProducers(List<String> topics) {
        run(c -> c.createProducers(topics));
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) {
        run(c -> c.createConsumers(consumerAssignment));
    }

    @Override
    public void probeProducers() {
        run(HttpWorkerClient::probeProducers);
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        run(c -> c.startLoad(producerWorkAssignment));
    }

    @Override
    public void adjustPublishRate(double publishRate) {
        run(c -> c.adjustPublishRate(publishRate));
    }

    @Override
    public void pauseConsumers() {
        run(HttpWorkerClient::pauseConsumers);
    }

    @Override
    public void resumeConsumers() {
        run(HttpWorkerClient::resumeConsumers);
    }

    @Override
    public CountersStats getCountersStats() {
        return call(HttpWorkerClient::getCountersStats);
    }

    @Override
    public PeriodStats getPeriodStats() {
        return call(HttpWorkerClient::getPeriodStats);
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        return call(HttpWorkerClient::getCumulativeLatencies);
    }

    @Override
    public void resetStats() {
        run(HttpWorkerClient::resetStats);
    }

    /**
     * Stops producers, consumers and the driver in the driver process. The process itself keeps
     * running until the next driver is initialized or this worker is closed, so late calls from the
     * load generator still get an answer.
     */
    @Override
    public void stopAll() {
        DriverProcess running;
        boolean exited;
        synchronized (this) {
            phase = null;
            stopped = false;
            running = current;
            if (running == null || running.client().isEmpty()) {
                return;
            }
            exited = running.hasExited();
        }
        // A crashed driver has nothing left to stop. Its crash reaches the coordinator through
        // health(), lastError and the next driver call, so stopAll succeeds whichever saw it first.
        if (exited) {
            terminate();
            return;
        }
        try {
            run(HttpWorkerClient::stopAll);
            synchronized (this) {
                if (current == running) {
                    stopped = true;
                }
            }
        } catch (RuntimeException e) {
            IllegalStateException failure =
                    new IllegalStateException(
                            "Failed to stop driver " + running.description() + ": " + running.describeFailure(e),
                            e);
            try {
                terminate();
            } catch (Throwable cleanup) {
                Failures.append(failure, cleanup);
            }
            throw failure;
        }
    }

    // Reads only volatile fields: initializeDriver holds the monitor while the child starts
    @Override
    public WorkerHealth health() {
        boolean ready = !closed && !unusable;
        String error = lastError;
        DriverProcess running = current;
        if (running == null) {
            return new WorkerHealth(ready, DriverState.NOT_STARTED, null, error);
        }
        OptionalInt exitCode = running.exitCode();
        if (exitCode.isPresent()) {
            return new WorkerHealth(ready, DriverState.EXITED, exitCode.getAsInt(), error);
        }
        DriverState state =
                initializing || running.client().isEmpty() ? DriverState.INITIALIZING : DriverState.RUNNING;
        return new WorkerHealth(ready, state, null, error);
    }

    @Override
    public String id() {
        return "local-process";
    }

    @Override
    public synchronized boolean canReuseAfterFailure() {
        return !closed && !unusable && (current == null || stopped);
    }

    @Override
    public void close() {
        closed = true;
        Failures failures = new Failures();
        DriverProcess running = current;
        if (!initializing && running != null && running.client().isPresent()) {
            try {
                stopAll();
            } catch (Throwable t) {
                failures.add(t);
            }
        }
        running = current;
        if (running != null) {
            running.closeInput(failures);
        }
        try {
            terminate();
        } catch (Throwable t) {
            failures.add(t);
        }
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException _) {
            // JVM is already shutting down
        } catch (Throwable t) {
            failures.add(t);
        }
        failures.throwIfAny("Failed to close ProcessWorker");
    }

    private void shutdownDriverProcess() {
        // Initialization holds the worker monitor while awaiting the child, so this hook cannot use it.
        closed = true;
        DriverProcess running = current;
        if (running != null) {
            running.stopOnShutdown();
        }
    }

    private synchronized void terminate() {
        boolean wasStopped = stopped;
        stopped = false;
        Failures failures = new Failures();
        failures.deferInterrupt("Interrupted before terminating driver process");
        DriverProcess stopping = current;
        if (stopping != null) {
            if (!wasStopped) {
                phase = null;
            }
            stopping.stop(failures);
            // A child that is gone frees the worker even when stopping it failed, for example a
            // driver close that hung and was killed; only a child still running blocks the next run
            if (stopping.isStopped()) {
                current = null;
            } else {
                unusable = true;
            }
        }
        failures.throwIfAny("Failed to terminate driver process");
    }
}
