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

import io.openmessaging.benchmark.driver.Failures;
import io.openmessaging.benchmark.plugin.HostRuntime;
import io.openmessaging.benchmark.plugin.PluginRegistry;
import io.openmessaging.benchmark.plugin.ResolvedDriver;
import io.openmessaging.benchmark.plugin.RuntimeClasspath;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

    private static final ObjectMapper YAML = YAMLMapper.builder().build();

    private final Path driverLocation;
    private final Supplier<PluginRegistry> registry;
    private final Supplier<HostRuntime> hostRuntime;
    private final Thread shutdownHook;

    private volatile boolean closed;
    private volatile boolean unusable;
    private volatile boolean initializing;
    private volatile String runId;
    // The current driver process, kept until it is fully stopped so that termination can be retried
    private volatile DriverProcess current;

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
    public ProcessWorker(Path driverLocation) {
        this(driverLocation, PluginRegistry::discover, HostRuntime::current);
    }

    ProcessWorker(Supplier<PluginRegistry> registry, Supplier<HostRuntime> hostRuntime) {
        this(null, registry, hostRuntime);
    }

    ProcessWorker(
            Path driverLocation, Supplier<PluginRegistry> registry, Supplier<HostRuntime> hostRuntime) {
        this.driverLocation = driverLocation;
        this.registry = registry;
        this.hostRuntime = hostRuntime;
        this.shutdownHook = new Thread(this::shutdownDriverProcess, "process-worker-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    @Override
    public synchronized void initializeDriver(Path driverConfigFile) throws Exception {
        ensureOpen();
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Interrupted before driver initialization");
        }
        terminate();

        Map<String, Object> config;
        try {
            config = YAML.readValue(driverConfigFile.toFile(), new TypeReference<>() {});
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Cannot read driver configuration " + driverConfigFile + ": " + e.getMessage(), e);
        }
        Object driverClass = config == null ? null : config.get("driverClass");
        if (driverClass == null || driverClass.toString().isBlank()) {
            throw new IllegalArgumentException(
                    "Driver configuration " + driverConfigFile + " must specify a driverClass");
        }

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

        DriverProcess starting = new DriverProcess(description);
        current = starting;
        initializing = true;
        try {
            ensureOpen();
            starting.start(classpath.entries(), () -> closed);
            if (runId != null) {
                starting.client().setRunId(runId);
            }
            starting.client().initializeDriverFromLocalPath(driverConfigFile);
        } catch (Exception e) {
            String failure = starting.describeFailure(e);
            if (starting.isRunning() && !closed) {
                try {
                    starting.client().stopAll();
                } catch (Throwable cleanup) {
                    e.addSuppressed(cleanup);
                }
            }
            try {
                terminate();
            } catch (Throwable cleanup) {
                e.addSuppressed(cleanup);
            }
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

    @Override
    public synchronized void setRunId(String runId) {
        this.runId = runId;
        DriverProcess running = current;
        if (running != null && running.client() != null) {
            running.client().setRunId(runId);
        }
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
     * @return the pid, or {@code null} if no driver process is running
     */
    public synchronized Long pid() {
        DriverProcess running = current;
        return running == null ? null : running.pid();
    }

    /**
     * Prometheus metrics of the running driver process.
     *
     * @return the metrics, or an empty string if no driver has been started
     * @throws IllegalStateException if the driver exited or its metrics cannot be scraped
     */
    public String scrapeMetrics() {
        String url;
        synchronized (this) {
            DriverProcess running = current;
            if (running == null || running.endpoint() == null) {
                return "";
            }
            if (running.hasExited()) {
                throw running.crashed(null);
            }
            url = running.endpoint() + "/metrics";
        }
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            HttpResponse<String> response =
                    http.send(
                            HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).build(),
                            HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "Driver metrics request to " + url + " failed with HTTP " + response.statusCode());
            }
            return response.body();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot scrape driver metrics from " + url, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while scraping driver metrics from " + url, e);
        }
    }

    // Forwards a call to the driver process and reports a crash instead of the failed call.
    private <T> T call(Function<HttpWorkerClient, T> action) {
        DriverProcess running;
        HttpWorkerClient client;
        synchronized (this) {
            running = current;
            client = running == null ? null : running.client();
            if (client == null) {
                throw new IllegalStateException("No driver process is running; initialize a driver first");
            }
            if (running.hasExited()) {
                throw running.crashed(null);
            }
        }
        try {
            return action.apply(client);
        } catch (RuntimeException e) {
            // A failed call is often the first sign of a crashed process; give it time to be reaped
            if (running.awaitExit(CRASH_DETECTION_TIMEOUT)) {
                throw running.crashed(e);
            }
            throw e;
        }
    }

    private void run(Consumer<HttpWorkerClient> action) {
        call(
                client -> {
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
        boolean crashAlreadyReported;
        synchronized (this) {
            running = current;
            if (running == null || running.client() == null) {
                return;
            }
            crashAlreadyReported = running.hasExited() && running.crashReported();
        }
        if (crashAlreadyReported) {
            terminate();
            return;
        }
        try {
            run(HttpWorkerClient::stopAll);
        } catch (RuntimeException e) {
            IllegalStateException failure =
                    new IllegalStateException(
                            "Failed to stop driver " + running.description() + ": " + running.describeFailure(e),
                            e);
            try {
                terminate();
            } catch (Throwable cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    public String id() {
        return "local-process";
    }

    @Override
    public synchronized boolean canReuseAfterFailure() {
        return !closed && !unusable && current == null;
    }

    @Override
    public void close() {
        closed = true;
        Failures failures = new Failures();
        DriverProcess running = current;
        if (!initializing && running != null && running.client() != null) {
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
        } catch (IllegalStateException e) {
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
        Failures failures = new Failures();
        failures.deferInterrupt("Interrupted before terminating driver process");
        DriverProcess stopping = current;
        if (stopping != null) {
            stopping.stop(failures);
            if (stopping.isStopped()) {
                current = null;
            } else {
                unusable = true;
            }
        }
        if (failures.hasFailure()) {
            unusable = true;
        }
        failures.throwIfAny("Failed to terminate driver process");
    }
}
