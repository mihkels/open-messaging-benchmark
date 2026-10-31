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
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLMapper;

/**
 * A {@link Worker} that runs the selected driver in a dedicated child JVM whose classpath contains
 * only the benchmark core and the driver's plugin bundle(s). Every {@link #initializeDriver(Path,
 * Path)} starts a fresh process, so different drivers and different versions of their client
 * libraries never share a classpath.
 *
 * <p>The child JVM options are taken from the {@value #JVM_OPTS_PROPERTY} system property or the
 * {@value #JVM_OPTS_ENV} environment variable, falling back to {@link #DEFAULT_JVM_OPTS}.
 */
public class ProcessWorker implements Worker {

    private static final Logger log = LoggerFactory.getLogger(ProcessWorker.class);

    public static final String JVM_OPTS_PROPERTY = "omb.driver.jvmOpts";
    public static final String JVM_OPTS_ENV = "OMB_DRIVER_JVM_OPTS";
    public static final String STARTUP_TIMEOUT_PROPERTY = "omb.driver.startupTimeoutSeconds";

    static final List<String> DEFAULT_JVM_OPTS =
            List.of(
                    "-XX:+UseG1GC",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                    "--add-opens=java.base/java.util.zip=ALL-UNNAMED",
                    "-Dio.netty.tryReflectionSetAccessible=true",
                    "-Dio.netty.leakDetection.level=disabled");

    private static final List<String> FORWARDED_PROPERTIES =
            List.of("log4j.configurationFile", "log4j2.configurationFile");
    private static final String LOG_NAME_PROPERTY = "omb.log.name";
    private static final String DRIVER_LOG_NAME = "benchmark-driver";
    private static final Duration CRASH_DETECTION_TIMEOUT = Duration.ofSeconds(2);

    private static final ObjectMapper YAML = YAMLMapper.builder().build();

    private final Supplier<PluginRegistry> registry;
    private final Supplier<HostRuntime> hostRuntime;
    private final Thread shutdownHook;

    private Process process;
    private OutputStream processStdin;
    private HttpWorkerClient client;
    private Path workDir;
    private String endpoint;
    private String driverDescription;

    public ProcessWorker() {
        this(PluginRegistry::discover, HostRuntime::current);
    }

    ProcessWorker(Supplier<PluginRegistry> registry, Supplier<HostRuntime> hostRuntime) {
        this.registry = registry;
        this.hostRuntime = hostRuntime;
        this.shutdownHook = new Thread(this::terminate, "process-worker-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    @Override
    public void initializeDriver(Path driverConfigFile) throws Exception {
        initializeDriver(driverConfigFile, null);
    }

    @Override
    public synchronized void initializeDriver(Path driverConfigFile, Path driverLocation)
            throws Exception {
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
        ResolvedDriver resolved =
                driverLocation == null
                        ? plugins.resolve(driverClass.toString(), config)
                        : plugins.resolveOverride(driverClass.toString(), config, driverLocation);
        RuntimeClasspath classpath = RuntimeClasspath.compose(hostRuntime.get(), resolved);
        classpath.warnings().forEach(w -> log.warn("{}", w));

        driverDescription =
                resolved.driverClass()
                        + " from "
                        + resolved.bundles().stream()
                                .map(b -> "'" + b.id() + "' (" + b.directory() + ")")
                                .collect(Collectors.joining(", "));
        log.info("Starting driver process for {}", driverDescription);
        start(classpath.entries());

        try {
            client.initializeDriver(driverConfigFile);
        } catch (Exception e) {
            String failure = describeFailure(e, process);
            terminate();
            throw new IllegalStateException(
                    "Failed to initialize driver " + driverDescription + ": " + failure, e);
        }
    }

    private void start(List<Path> classpath) throws IOException, InterruptedException {
        workDir = Files.createTempDirectory("omb-driver-");
        Path portFile = workDir.resolve("port");
        Path argFile = workDir.resolve("java.args");

        List<String> jvmArgs = new ArrayList<>(jvmOptions());
        for (String property : FORWARDED_PROPERTIES) {
            String value = System.getProperty(property);
            if (value != null) {
                jvmArgs.add("-D" + property + "=" + value);
            }
        }
        jvmArgs.add("-D" + LOG_NAME_PROPERTY + "=" + DRIVER_LOG_NAME);
        jvmArgs.add("-cp");
        jvmArgs.add(
                classpath.stream().map(Path::toString).collect(Collectors.joining(File.pathSeparator)));
        jvmArgs.add(DriverProcessMain.class.getName());
        jvmArgs.add("--port-file");
        jvmArgs.add(portFile.toString());
        // An argument file keeps long classpaths clear of command line length limits
        Files.write(
                argFile, jvmArgs.stream().map(ProcessWorker::quote).toList(), StandardCharsets.UTF_8);

        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        ProcessBuilder builder =
                new ProcessBuilder(java.toString(), "@" + argFile)
                        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                        .redirectError(ProcessBuilder.Redirect.INHERIT)
                        .redirectInput(ProcessBuilder.Redirect.PIPE);
        builder.environment().remove("CLASSPATH");
        log.debug("Driver process classpath: {}", classpath);
        process = builder.start();
        processStdin = process.getOutputStream();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(startupTimeoutSeconds());
        while (!Files.isRegularFile(portFile)) {
            if (!process.isAlive()) {
                int exitCode = process.exitValue();
                terminate();
                throw new IllegalStateException(
                        "Driver process for "
                                + driverDescription
                                + " exited during startup with code "
                                + exitCode
                                + "; see its output above");
            }
            if (System.nanoTime() > deadline) {
                terminate();
                throw new IllegalStateException(
                        "Driver process for "
                                + driverDescription
                                + " did not start within "
                                + startupTimeoutSeconds()
                                + "s");
            }
            Thread.sleep(50);
        }
        int port = Integer.parseInt(Files.readString(portFile, StandardCharsets.US_ASCII).trim());
        endpoint = "http://127.0.0.1:" + port;
        client = new HttpWorkerClient(endpoint);
        log.info(
                "Driver process pid {} is serving {} on {}", process.pid(), driverDescription, endpoint);
    }

    static List<String> jvmOptions() {
        String configured = System.getProperty(JVM_OPTS_PROPERTY, System.getenv(JVM_OPTS_ENV));
        if (configured == null || configured.isBlank()) {
            return DEFAULT_JVM_OPTS;
        }
        return Arrays.stream(configured.trim().split("\\s+")).toList();
    }

    private static long startupTimeoutSeconds() {
        return Long.getLong(STARTUP_TIMEOUT_PROPERTY, 120);
    }

    static String quote(String argument) {
        return "\"" + argument.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /**
     * The pid of the running driver process.
     *
     * @return the pid, or {@code null} if no driver process is running
     */
    public synchronized Long pid() {
        return process == null ? null : process.pid();
    }

    /**
     * Prometheus metrics of the running driver process.
     *
     * @return the metrics, or an empty string if no driver process is running
     */
    public String scrapeMetrics() {
        String url;
        synchronized (this) {
            if (endpoint == null || process == null || !process.isAlive()) {
                return "";
            }
            url = endpoint + "/metrics";
        }
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            HttpResponse<String> response =
                    http.send(
                            HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).build(),
                            HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 ? response.body() : "";
        } catch (IOException e) {
            return "";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "";
        }
    }

    private <T> T call(ClientCall<T> action) throws IOException {
        HttpWorkerClient current;
        Process currentProcess;
        String description;
        synchronized (this) {
            current = client;
            currentProcess = process;
            description = driverDescription;
            if (current == null) {
                throw new IllegalStateException("No driver process is running; initialize a driver first");
            }
            if (!process.isAlive()) {
                throw new IllegalStateException(
                        "Driver process for " + driverDescription + " exited with code " + process.exitValue());
            }
        }
        try {
            return action.apply(current);
        } catch (IOException | RuntimeException e) {
            // A failed call is often the first sign of a crashed process; give it time to be reaped
            if (awaitExit(currentProcess, CRASH_DETECTION_TIMEOUT)) {
                throw new IllegalStateException(
                        "Driver process for " + description + " exited with code " + currentProcess.exitValue(),
                        e);
            }
            throw e;
        }
    }

    private static boolean awaitExit(Process process, Duration timeout) {
        try {
            return process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return !process.isAlive();
        }
    }

    private void run(ClientAction action) throws IOException {
        call(
                c -> {
                    action.run(c);
                    return null;
                });
    }

    @FunctionalInterface
    private interface ClientCall<T> {
        T apply(HttpWorkerClient client) throws IOException;
    }

    @FunctionalInterface
    private interface ClientAction {
        void run(HttpWorkerClient client) throws IOException;
    }

    private static String describeFailure(Throwable e, Process process) {
        Throwable cause = e;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        String message = cause.getMessage() == null ? cause.toString() : cause.getMessage();
        if (process != null && !process.isAlive()) {
            message += " (driver process exited with code " + process.exitValue() + ")";
        }
        return message;
    }

    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) throws IOException {
        return call(c -> c.createTopics(topicsInfo));
    }

    @Override
    public void createProducers(List<String> topics) throws IOException {
        run(c -> c.createProducers(topics));
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) throws IOException {
        run(c -> c.createConsumers(consumerAssignment));
    }

    @Override
    public void probeProducers() throws IOException {
        run(c -> c.probeProducers());
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) throws IOException {
        run(c -> c.startLoad(producerWorkAssignment));
    }

    @Override
    public void adjustPublishRate(double publishRate) throws IOException {
        run(c -> c.adjustPublishRate(publishRate));
    }

    @Override
    public void pauseConsumers() throws IOException {
        run(c -> c.pauseConsumers());
    }

    @Override
    public void resumeConsumers() throws IOException {
        run(c -> c.resumeConsumers());
    }

    @Override
    public CountersStats getCountersStats() throws IOException {
        return call(c -> c.getCountersStats());
    }

    @Override
    public PeriodStats getPeriodStats() throws IOException {
        return call(c -> c.getPeriodStats());
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() throws IOException {
        return call(c -> c.getCumulativeLatencies());
    }

    @Override
    public void resetStats() throws IOException {
        run(c -> c.resetStats());
    }

    /**
     * Stops producers, consumers and the driver in the driver process. The process itself keeps
     * running until the next driver is initialized or this worker is closed, so late calls from the
     * load generator still get an answer.
     */
    @Override
    public void stopAll() {
        HttpWorkerClient current;
        Process currentProcess;
        String description;
        synchronized (this) {
            current = client;
            currentProcess = process;
            description = driverDescription;
            if (current == null || !currentProcess.isAlive()) {
                return;
            }
        }
        try {
            current.stopAll();
        } catch (RuntimeException e) {
            log.warn("Failed to stop driver {}: {}", description, describeFailure(e, currentProcess));
        }
    }

    @Override
    public String id() {
        return "local";
    }

    @Override
    public void close() {
        terminate();
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException e) {
            // JVM is already shutting down
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.debug("Failed to delete {}", path, e);
        }
    }

    private synchronized void terminate() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.debug("Failed to close driver client", e);
            }
            client = null;
        }
        if (process != null) {
            try {
                // Closing stdin asks the driver process to stop its driver and exit
                processStdin.close();
            } catch (IOException e) {
                log.debug("Failed to close driver process input", e);
            }
            try {
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    process.destroy();
                    if (!process.waitFor(10, TimeUnit.SECONDS)) {
                        log.warn("Driver process {} did not stop, killing it", process.pid());
                        process.destroyForcibly();
                        process.waitFor(10, TimeUnit.SECONDS);
                    }
                }
            } catch (InterruptedException e) {
                process.destroyForcibly();
                Thread.currentThread().interrupt();
            }
            process = null;
            processStdin = null;
        }
        endpoint = null;
        if (workDir != null) {
            try (Stream<Path> files = Files.walk(workDir)) {
                files.sorted(Comparator.reverseOrder()).forEach(ProcessWorker::deleteQuietly);
            } catch (IOException e) {
                log.debug("Failed to delete {}", workDir, e);
            }
            workDir = null;
        }
    }
}
