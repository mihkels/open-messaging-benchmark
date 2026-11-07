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
package io.openmessaging.benchmark.performance;

import io.javalin.Javalin;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.WorkloadGenerator;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import io.openmessaging.benchmark.worker.DistributedWorkersEnsemble;
import io.openmessaging.benchmark.worker.HttpWorkerClient;
import io.openmessaging.benchmark.worker.JvmResourceMetrics;
import io.openmessaging.benchmark.worker.LocalWorker;
import io.openmessaging.benchmark.worker.ProcessWorker;
import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.WorkerHandler;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLMapper;

/** One fresh-JVM experiment; orchestration, RSS sampling and comparison live in the Python CLI. */
public final class ScenarioMain {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern RESOURCE =
            Pattern.compile("omb_resource_(\\w++)\\{([^}]*+)}\\s++(\\S++).*");
    private static final Pattern ROLE_LABEL = Pattern.compile("role=\"([^\"]++)\"");
    private static final Pattern RECEIVE_ERRORS =
            Pattern.compile("synthetic_receive_errors_total(?:\\{[^}]*+})?+\\s++(\\S++).*");

    private ScenarioMain() {}

    public record Scenario(
            String topology,
            String mode,
            int rate,
            int topics,
            int producers,
            int payloadBytes,
            int workers,
            int queueCapacity,
            int threads,
            int warmupSeconds,
            int measureSeconds,
            int warmupMinutes,
            int measureMinutes,
            boolean smoke,
            boolean weighted,
            String key) {
        public Scenario {
            if (!List.of("local", "process", "http", "distributed").contains(topology)
                    || !List.of("sync", "async", "send-only").contains(mode)
                    || rate < 1
                    || topics < 1
                    || producers < 1
                    || payloadBytes < 1
                    || workers < 2
                    || queueCapacity < 1
                    || threads < 1
                    || warmupSeconds < 1
                    || measureSeconds < 1
                    || warmupMinutes < 1
                    || measureMinutes < 1) {
                throw new IllegalArgumentException("Invalid scenario configuration");
            }
            KeyDistributorType.valueOf(key);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "Usage: ScenarioMain <scenario.json|serve> <output> <plugin>");
        }
        Path output = Path.of(args[1]).toAbsolutePath();
        Files.createDirectories(output);
        System.setProperty("omb.plugin.path", Path.of(args[2]).toAbsolutePath().toString());
        if ("serve".equals(args[0])) {
            serve(output);
            return;
        }
        Scenario scenario = JSON.readValue(Path.of(args[0]).toFile(), Scenario.class);
        run(scenario, output, Path.of(args[2]).toAbsolutePath());
    }

    private static void serve(Path output) throws Exception {
        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try (var worker = new ProcessWorker();
                var handler = new WorkerHandler(worker)) {
            new JvmResourceMetrics().bindTo(registry, "supervisor");
            Javalin app =
                    Javalin.create(
                                    config -> {
                                        config.startup.showJavalinBanner = false;
                                        handler.configure(config);
                                        config.routes.get(
                                                "/metrics",
                                                ctx -> {
                                                    ctx.result(registry.scrape() + worker.scrapeMetrics());
                                                });
                                    })
                            .start("127.0.0.1", 0);
            try {
                Files.writeString(output.resolve("endpoint"), "http://127.0.0.1:" + app.port());
                System.in.transferTo(OutputStream.nullOutputStream());
            } finally {
                app.stop();
            }
        } finally {
            registry.close();
        }
    }

    private static void run(Scenario scenario, Path output, Path plugin) throws Exception {
        long setupStart = System.nanoTime();
        List<Process> supervisors = new ArrayList<>();
        List<Worker> handles = new ArrayList<>();
        List<Supplier<String>> scrapes = new ArrayList<>();
        Map<String, Object> result = new LinkedHashMap<>();
        var localRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Throwable primaryFailure = null;
        try (var sampler = new Sampler(output, scrapes)) {
            Worker worker =
                    createWorker(scenario, output, plugin, supervisors, handles, scrapes, localRegistry);
            Path driver = output.resolve("driver.yaml");
            Map<String, Object> config = new LinkedHashMap<>();
            config.put("name", "framework-fixture");
            config.put("driverClass", SyntheticDriver.class.getName());
            config.put("mode", scenario.mode());
            config.put("queueCapacity", scenario.queueCapacity());
            config.put("threads", scenario.threads());
            if ("distributed".equals(scenario.topology())) {
                config.put("receiveRate", scenario.rate());
            }
            Files.writeString(driver, new YAMLMapper().writeValueAsString(config));
            worker.initializeDriver(driver);
            captureRuntime(output);
            result.put("setupSeconds", secondsSince(setupStart));
            if (scenario.smoke()) {
                shortRun(scenario, worker, sampler);
                result.putAll(sampler.finish(worker));
            } else {
                result.putAll(fullRun(scenario, worker, sampler, output));
            }
            result.put("schemaVersion", 1);
            result.put("scenario", scenario);
            result.put("status", "complete");
            result.put("jvmArguments", ManagementFactory.getRuntimeMXBean().getInputArguments());
        } catch (Exception | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            long cleanupStart = System.nanoTime();
            Exception failure = null;
            for (Worker handle : handles.reversed()) {
                try {
                    handle.close();
                } catch (Exception e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            for (Process process : supervisors) {
                process.getOutputStream().close();
                if (!process.waitFor(20, TimeUnit.SECONDS)) {
                    process.destroy();
                    if (!process.waitFor(5, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        process.waitFor(5, TimeUnit.SECONDS);
                        failure = new IllegalStateException("Supervisor exceeded shutdown deadline", failure);
                    }
                }
                if (!process.isAlive() && process.exitValue() != 0) {
                    failure =
                            new IllegalStateException(
                                    "Supervisor exited with status " + process.exitValue(), failure);
                }
            }
            result.put("cleanupSeconds", secondsSince(cleanupStart));
            localRegistry.close();
            if (failure != null) {
                if (primaryFailure == null) {
                    throw failure;
                }
                primaryFailure.addSuppressed(failure);
            }
        }
        Files.writeString(
                output.resolve("result.json"),
                JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
    }

    private static Worker createWorker(
            Scenario scenario,
            Path output,
            Path plugin,
            List<Process> processes,
            List<Worker> handles,
            List<Supplier<String>> scrapes,
            PrometheusMeterRegistry localRegistry)
            throws Exception {
        if ("local".equals(scenario.topology())) {
            Worker worker = new LocalWorker(localRegistry);
            handles.add(worker);
            scrapes.add(localRegistry::scrape);
            return worker;
        }
        if ("process".equals(scenario.topology())) {
            var worker = new ProcessWorker();
            handles.add(worker);
            scrapes.add(worker::scrapeMetrics);
            return worker;
        }
        int count = "distributed".equals(scenario.topology()) ? scenario.workers() : 1;
        List<Worker> clients = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Path directory = output.resolve("supervisor-" + i);
            Files.createDirectories(directory);
            List<String> command = new ArrayList<>();
            command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
            command.addAll(ManagementFactory.getRuntimeMXBean().getInputArguments());
            command.add("-cp");
            command.add(System.getProperty("java.class.path"));
            command.add(ScenarioMain.class.getName());
            command.addAll(List.of("serve", directory.toString(), plugin.toString()));
            Process process =
                    new ProcessBuilder(command)
                            .redirectOutput(directory.resolve("console.log").toFile())
                            .redirectErrorStream(true)
                            .start();
            processes.add(process);
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            Path endpointFile = directory.resolve("endpoint");
            while (!Files.exists(endpointFile)) {
                if (!process.isAlive() || System.nanoTime() > deadline) {
                    throw new IllegalStateException("Supervisor failed to start; see " + directory);
                }
                Thread.sleep(25);
            }
            String endpoint = Files.readString(endpointFile);
            Worker client = new HttpWorkerClient(endpoint);
            clients.add(client);
            handles.add(client);
            scrapes.add(() -> scrape(endpoint));
        }
        if (count == 1) {
            return clients.getFirst();
        }
        Worker ensemble = new DistributedWorkersEnsemble(clients, false);
        handles.add(ensemble);
        return ensemble;
    }

    private static void shortRun(Scenario s, Worker worker, Sampler sampler) throws Exception {
        List<String> topics = worker.createTopics(new TopicsInfo(s.topics(), 1));
        if (!"send-only".equals(s.mode())) {
            var assignment = new ConsumerAssignment();
            assignment.topicsSubscriptions =
                    topics.stream().map(t -> new TopicSubscription(t, "subscription")).toList();
            worker.createConsumers(assignment);
        }
        List<String> producerTopics = new ArrayList<>();
        topics.forEach(t -> {
            for (int i = 0; i < s.producers(); i++) {
                producerTopics.add(t);
            }
        });
        worker.createProducers(producerTopics);
        worker.probeProducers();
        worker.startLoad(
                new ProducerWorkAssignment(
                        s.weighted()
                                ? List.of(new byte[s.payloadBytes()], new byte[s.payloadBytes() * 2])
                                : List.of(new byte[s.payloadBytes()]),
                        s.rate(),
                        KeyDistributorType.valueOf(s.key()),
                        s.weighted() ? new int[] {3, 1} : null));
        sampler.phase = "warmup";
        long deadline = System.nanoTime() + Duration.ofSeconds(s.warmupSeconds()).toNanos();
        while (System.nanoTime() < deadline) {
            worker.getPeriodStats();
            sampler.sample();
            Thread.sleep(250);
        }
        worker.resetStats();
        worker.getPeriodStats();
        sampler.begin(worker);
        deadline = System.nanoTime() + Duration.ofSeconds(s.measureSeconds()).toNanos();
        while (System.nanoTime() < deadline) {
            long start = System.nanoTime();
            sampler.lastSnapshotStarted = System.nanoTime();
            sampler.record(worker.getPeriodStats());
            sampler.pollNanos += System.nanoTime() - start;
            sampler.sample();
            Thread.sleep(250);
        }
        sampler.lastSnapshotStarted = System.nanoTime();
        sampler.record(worker.getPeriodStats());
        sampler.latencies = worker.getCumulativeLatencies();
    }

    private static Map<String, Object> fullRun(
            Scenario s, Worker worker, Sampler sampler, Path output) throws Exception {
        if ("send-only".equals(s.mode())) {
            throw new IllegalArgumentException(
                    "Full WorkloadGenerator requires consumers; use smoke for send-only");
        }
        Workload workload = new Workload();
        workload.name = "framework-resource-efficiency";
        workload.topics = s.topics();
        workload.partitionsPerTopic = 1;
        workload.producersPerTopic = s.producers();
        workload.consumerPerSubscription = 1;
        workload.subscriptionsPerTopic = 1;
        workload.producerRate = s.rate();
        workload.messageSize = s.payloadBytes();
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 1;
        workload.randomBytesRatio = 0;
        workload.keyDistributor = KeyDistributorType.valueOf(s.key());
        workload.warmupDurationMinutes = s.warmupMinutes();
        workload.testDurationMinutes = s.measureMinutes();
        if (s.weighted()) {
            workload.messageSizeDistribution =
                    Map.of(Integer.toString(s.payloadBytes()), 3, Integer.toString(s.payloadBytes() * 2), 1);
        }
        Worker observed =
                (Worker)
                        Proxy.newProxyInstance(
                                Worker.class.getClassLoader(),
                                new Class<?>[] {Worker.class},
                                (proxy, method, args) -> {
                                    long start = System.nanoTime();
                                    Object value;
                                    try {
                                        value = method.invoke(worker, args);
                                    } catch (InvocationTargetException e) {
                                        throw e.getCause();
                                    }
                                    if ("resetStats".equals(method.getName())) {
                                        sampler.begin(worker);
                                    } else if (value instanceof PeriodStats stats && sampler.measuring) {
                                        sampler.lastSnapshotStarted = start;
                                        sampler.record(stats);
                                        sampler.pollNanos += System.nanoTime() - start;
                                    } else if (value instanceof CumulativeLatencies latencies && sampler.measuring) {
                                        sampler.latencies = latencies;
                                    }
                                    return value;
                                });
        sampler.phase = "warmup";
        try (var generator = new WorkloadGenerator("synthetic", workload, observed)) {
            Thread monitor = Thread.ofVirtual().start(() -> sampler.monitor());
            try {
                var benchmarkResult = generator.run();
                Files.writeString(
                        output.resolve("benchmark-result.json"),
                        JSON.writerWithDefaultPrettyPrinter().writeValueAsString(benchmarkResult));
                return sampler.finish(worker);
            } finally {
                monitor.interrupt();
                monitor.join();
            }
        }
    }

    private static String scrape(String endpoint) {
        try (var client = HttpClient.newHttpClient()) {
            var response =
                    client.send(
                            HttpRequest.newBuilder(URI.create(endpoint + "/metrics"))
                                    .timeout(Duration.ofSeconds(10))
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Metrics HTTP " + response.statusCode());
            }
            return response.body();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot scrape " + endpoint, e);
        }
    }

    private static void captureRuntime(Path output) throws Exception {
        List<ProcessHandle> processes = new ArrayList<>();
        processes.add(ProcessHandle.current());
        processes.addAll(ProcessHandle.current().descendants().toList());
        for (ProcessHandle process : processes) {
            if (!process.info().command().orElse("").endsWith("java")) {
                continue;
            }
            Path destination = output.resolve("jvm-" + process.pid() + ".txt");
            Process jcmd =
                    new ProcessBuilder(
                                    Path.of(System.getProperty("java.home"), "bin", "jcmd").toString(),
                                    Long.toString(process.pid()),
                                    "VM.command_line")
                            .redirectErrorStream(true)
                            .redirectOutput(destination.toFile())
                            .start();
            if (!jcmd.waitFor(10, TimeUnit.SECONDS)) {
                jcmd.destroyForcibly();
                throw new IllegalStateException("jcmd timed out for " + process.pid());
            }
            if (jcmd.exitValue() != 0) {
                throw new IllegalStateException("jcmd failed; see " + destination);
            }
        }
    }

    /**
     * One {@code omb_resource_*} Prometheus sample.
     *
     * @param name metric name after the {@code omb_resource_} prefix
     * @param role value of the {@code role} label
     * @param value sample value
     */
    record ResourceSample(String name, String role, double value) {}

    /**
     * Parses an {@code omb_resource_<name>{...role="<role>"...} <value>} line.
     *
     * @param line one line of a Prometheus scrape
     * @return the sample, or {@code null} if the line is not a resource gauge with a {@code role}
     *     label
     */
    static ResourceSample parseResource(String line) {
        Matcher matcher = RESOURCE.matcher(line);
        if (!matcher.matches()) {
            return null;
        }
        // The last role label wins, as with the former single greedy pattern.
        Matcher role = ROLE_LABEL.matcher(matcher.group(2));
        String value = null;
        while (role.find()) {
            value = role.group(1);
        }
        return value == null
                ? null
                : new ResourceSample(matcher.group(1), value, Double.parseDouble(matcher.group(3)));
    }

    /**
     * Parses a {@code synthetic_receive_errors_total[{...}] <value>} line.
     *
     * @param line one line of a Prometheus scrape
     * @return the counter value, or empty if the line is not that counter
     */
    static OptionalDouble parseReceiveErrors(String line) {
        Matcher matcher = RECEIVE_ERRORS.matcher(line);
        return matcher.matches()
                ? OptionalDouble.of(Double.parseDouble(matcher.group(1)))
                : OptionalDouble.empty();
    }

    private static double secondsSince(long start) {
        return (System.nanoTime() - start) / 1e9;
    }

    private static final class Sampler implements AutoCloseable {
        private final BufferedWriter writer;
        private final List<Supplier<String>> scrapes;
        private final JvmResourceMetrics coordinator = new JvmResourceMetrics();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final Map<String, Map<String, Double>> first = new LinkedHashMap<>();
        private final Map<String, Map<String, Double>> last = new LinkedHashMap<>();
        private final Map<String, Map<String, Double>> peaks = new LinkedHashMap<>();
        private final Map<Integer, Double> fixtureErrors = new LinkedHashMap<>();
        private double initialFixtureErrors;
        private volatile String phase = "setup";
        private volatile boolean measuring;
        private CountersStats initial;
        private long started;
        private long pollNanos;
        private long publishSamples;
        private long delaySamples;
        private volatile long lastSnapshotStarted;
        private volatile CumulativeLatencies latencies;

        Sampler(Path output, List<Supplier<String>> scrapes) throws Exception {
            this.writer = Files.newBufferedWriter(output.resolve("samples.jsonl"));
            this.scrapes = scrapes;
        }

        synchronized void begin(Worker worker) throws Exception {
            initial = worker.getCountersStats();
            phase = "measurement";
            sample();
            first.putAll(last);
            initialFixtureErrors = fixtureErrors.values().stream().mapToDouble(Double::doubleValue).sum();
            peaks.clear();
            started = System.nanoTime();
            measuring = true;
        }

        synchronized void record(PeriodStats stats) {
            publishSamples += stats.publishLatency().getTotalCount();
            delaySamples += stats.publishDelayLatency().getTotalCount();
        }

        synchronized void sample() throws Exception {
            Map<String, Map<String, Double>> resources = new LinkedHashMap<>();
            resources.put("coordinator", coordinator.snapshot());
            int index = 0;
            for (Supplier<String> scrape : scrapes) {
                for (String line : scrape.get().split("\n")) {
                    ResourceSample sample = parseResource(line);
                    if (sample != null) {
                        resources
                                .computeIfAbsent(sample.role() + "-" + index, k -> new LinkedHashMap<>())
                                .put(sample.name(), Double.isFinite(sample.value()) ? sample.value() : null);
                    }
                    OptionalDouble errors = parseReceiveErrors(line);
                    if (errors.isPresent()) {
                        fixtureErrors.put(index, errors.getAsDouble());
                    }
                }
                index++;
            }
            last.clear();
            last.putAll(resources);
            if (measuring) {
                resources.forEach((role, values) ->
                        values.forEach((name, value) -> {
                            if (value != null) {
                                peaks
                                        .computeIfAbsent(role, k -> new LinkedHashMap<>())
                                        .merge(name, value, Math::max);
                            }
                        }));
            }
            writer.write(
                    JSON.writeValueAsString(
                            Map.of("phase", phase, "nanoTime", System.nanoTime(), "resources", resources)));
            writer.newLine();
            writer.flush();
        }

        void monitor() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    sample();
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                failure.set(e);
            }
        }

        synchronized Map<String, Object> finish(Worker worker) throws Exception {
            if (failure.get() != null) {
                throw new IllegalStateException("Resource sampling failed", failure.get());
            }
            if (!measuring) {
                throw new IllegalStateException("Measurement never started");
            }
            var end = worker.getCountersStats();
            double elapsed = secondsSince(started);
            double boundarySeconds = secondsSince(lastSnapshotStarted);
            sample();
            Map<String, Object> metrics = new LinkedHashMap<>();
            long completed = end.messagesSent() - initial.messagesSent();
            metrics.put("elapsedSeconds", elapsed);
            metrics.put("snapshotBoundarySeconds", boundarySeconds);
            metrics.put(
                    "fixtureReceiveErrors",
                    fixtureErrors.values().stream().mapToDouble(Double::doubleValue).sum()
                            - initialFixtureErrors);
            metrics.put("completed", completed);
            metrics.put("received", end.messagesReceived() - initial.messagesReceived());
            metrics.put("errors", end.messageSendErrors() - initial.messageSendErrors());
            metrics.put("achievedRate", completed / elapsed);
            metrics.put("pollSeconds", pollNanos / 1e9);
            metrics.put("publishHistogramSamples", publishSamples);
            metrics.put("delayHistogramSamples", delaySamples);
            metrics.put("resourceStart", first);
            metrics.put("resourceEnd", new LinkedHashMap<>(last));
            metrics.put("resourcePeaks", peaks);
            if (latencies != null) {
                metrics.put("publishP99Micros", latencies.publishLatency.getValueAtPercentile(99));
                metrics.put("delayP99Micros", latencies.publishDelayLatency.getValueAtPercentile(99));
                metrics.put("e2eP99Micros", latencies.endToEndLatency.getValueAtPercentile(99));
            }
            measuring = false;
            phase = "cleanup";
            writer.write(
                    JSON.writeValueAsString(
                            Map.of("phase", phase, "nanoTime", System.nanoTime(), "resources", last)));
            writer.newLine();
            writer.flush();
            return metrics;
        }

        @Override
        public void close() throws Exception {
            writer.close();
        }
    }
}
