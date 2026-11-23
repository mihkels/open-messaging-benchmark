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
package io.github.mihkels.omb;

import static io.github.mihkels.omb.driver.Failures.append;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import io.github.mihkels.omb.worker.BearerTokenAuth;
import io.github.mihkels.omb.worker.DistributedWorkersEnsemble;
import io.github.mihkels.omb.worker.HttpWorkerClient;
import io.github.mihkels.omb.worker.ProcessWorker;
import io.github.mihkels.omb.worker.Worker;
import io.github.mihkels.omb.worker.WorkerClock;
import io.github.mihkels.omb.worker.WorkerWatchdog;
import io.github.mihkels.omb.worker.commands.RunContext;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.yaml.YAMLMapper;

public final class Benchmark {
    static final int THRESHOLD_FAILURE_EXIT_CODE = 2;

    /** Exit code for --fail-on-invalid and --fail-on-warning; wins over a threshold failure. */
    static final int VALIDITY_FAILURE_EXIT_CODE = 3;

    static class Arguments {

        @Parameter(
                names = {"-c", "--csv"},
                description = "Print results from this directory to a csv file")
        @Nullable String resultsDir;

        @Parameter(
                names = {"-h", "--help"},
                description = "Help message",
                help = true)
        boolean help;

        @Parameter(
                names = {"-d", "--drivers"},
                description = "Drivers list. eg.: pulsar/pulsar.yaml,kafka/kafka.yaml")
        public List<String> drivers = new ArrayList<>();

        @Parameter(
                names = {"-w", "--workers"},
                description =
                        "List of worker nodes. eg: http://1.2.3.4:8080,http://4.5.6.7:8080, or a single"
                                + " dns://<host>:<port> resolved to one worker per A/AAAA record, eg. a"
                                + " Kubernetes headless Service")
        public List<String> workers = new ArrayList<>();

        @Parameter(
                names = {"-wf", "--workers-file"},
                description = "Path to a YAML file containing the list of workers addresses")
        public @Nullable File workersFile;

        @Parameter(
                names = "--expected-workers",
                description = "With a dns:// worker address, wait until this many workers resolve")
        public int expectedWorkers = 2;

        @Parameter(
                names = "--workers-timeout",
                description =
                        "With a dns:// worker address, how long to wait for --expected-workers, eg. 30s, 5m",
                converter = DurationConverter.class)
        public Duration workersTimeout = Duration.ofMinutes(5);

        @Parameter(
                names = "--worker-token-file",
                description =
                        "File holding the bearer token sent to remote workers; overrides the "
                                + BearerTokenAuth.TOKEN_ENV
                                + " environment variable")
        public @Nullable Path workerTokenFile;

        @Parameter(
                names = "--worker-ca-file",
                description =
                        "PEM certificate that https:// workers present (their --tls-cert); overrides the "
                                + HttpWorkerClient.CA_FILE_ENV
                                + " environment variable. With a dns:// address, workers are reached over"
                                + " https:// when this is set")
        public @Nullable Path workerCaFile;

        @Parameter(
                names = {"-x", "--extra"},
                description = "Allocate extra consumer workers when your backlog builds.")
        boolean extraConsumers;

        @Parameter(
                names = {"--driver-location", "--legacy-driver-name"},
                description =
                        "Load the driver from this plugin bundle directory, unpackaged driver module"
                                + " directory or jar instead of the installed plugin bundles (local runs only)."
                                + " --legacy-driver-name is a deprecated alias")
        public @Nullable String driverLocation;

        @Parameter(description = "Workloads")
        public List<String> workloads = new ArrayList<>();

        @Parameter(
                names = {"-o", "--output"},
                description = "Output",
                required = false)
        public @Nullable String output;

        @Parameter(
                names = "--include-histograms",
                description = "Include compressed latency histograms")
        public boolean includeHistograms;

        @Parameter(
                names = "--report-html",
                description = "Also write an HTML report with charts of all runs to this file")
        public @Nullable String reportHtml;

        @Parameter(
                names = "--retries",
                description =
                        "Rerun a driver and workload combination up to this many times after a worker,"
                                + " IO or timeout failure; 0 disables retries")
        public int retries = 3;

        @Parameter(
                names = "--fail-on-invalid",
                description = "Exit with status 3 when a completed run's validity verdict is invalid")
        public boolean failOnInvalid;

        @Parameter(
                names = "--fail-on-warning",
                description =
                        "Exit with status 3 when a completed run's validity verdict is warning or invalid")
        public boolean failOnWarning;

        @Parameter(
                names = "--repeat",
                description =
                        "Run each driver and workload combination this many times and write a summary of"
                                + " the run-to-run spread; overrides the workload's repeat")
        public @Nullable Integer repeat;
    }

    // Accepts 30s, 5m, 1h30m and the like
    public static final class DurationConverter implements IStringConverter<Duration> {
        @Override
        public Duration convert(String value) {
            try {
                Duration duration = Duration.parse("PT" + value.toUpperCase(Locale.ROOT));
                if (!duration.isNegative() && !duration.isZero()) {
                    return duration;
                }
            } catch (DateTimeParseException _) {
                // reported below
            }
            throw new ParameterException(
                    "Invalid duration '" + value + "', expected a positive value like 30s, 5m or 1h30m");
        }
    }

    private Benchmark() {
        throw new UnsupportedOperationException(
                "Benchmark constructor is private and should not be called directly");
    }

    static void main(String[] args) throws Exception {
        final Arguments arguments = parseArguments(args);
        // Results are written only after the run, so a bad path must fail now, not hours later
        try {
            createResultDirectory(arguments.output, System.getenv(RESULTS_DIR_ENV));
            createReportDirectory(arguments.reportHtml);
        } catch (IOException e) {
            log.error("Cannot write results: {}", e.toString());
            System.exit(-1);
        }
        resolveWorkers(arguments);

        // Dump configuration variables
        if (log.isInfoEnabled()) {
            log.info("Starting benchmark with config: {}", writer.writeValueAsString(arguments));
        }

        Map<String, Workload> workloads = readWorkloads(arguments.workloads);
        // Fail on a broken driver file before any workload runs
        arguments.drivers.forEach(Benchmark::readDriverConfig);
        if (log.isInfoEnabled()) {
            log.info("Workloads: {}", writer.writeValueAsString(workloads));
        }

        Worker worker = createWorker(arguments);
        int exitCode = runWorkloads(arguments, workloads, worker, WorkloadGenerator::new);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    // Parses and validates the command line; exits for --help, --csv and invalid arguments
    private static Arguments parseArguments(String[] args) {
        final Arguments arguments = new Arguments();
        var jc = new JCommander(arguments);
        jc.setProgramName("messaging-benchmark");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            exitWithUsage(jc, e.getMessage());
        }

        if (arguments.help) {
            jc.usage();
            System.exit(0);
        }

        if (arguments.resultsDir != null) {
            ResultsToCsv r = new ResultsToCsv();
            r.writeAllResultFiles(arguments.resultsDir);
            System.exit(0);
        }

        String usageError = usageError(arguments);
        if (usageError != null) {
            exitWithUsage(jc, usageError);
        }
        if (!arguments.workers.isEmpty() && arguments.workersFile != null) {
            log.error("Only one between --workers and --workers-file can be specified");
            System.exit(-1);
        }
        return arguments;
    }

    // Checks JCommander cannot express; null when the arguments are usable
    static @Nullable String usageError(Arguments arguments) {
        // Not required by JCommander because --csv needs neither
        if (arguments.workloads.isEmpty()) {
            return "At least one workload file is required";
        }
        if (arguments.drivers.isEmpty()) {
            return "At least one driver must be given with --drivers";
        }
        int combinations = arguments.workloads.size() * arguments.drivers.size();
        if (arguments.output != null && !arguments.output.isEmpty() && combinations > 1) {
            return "--output can only be used with a single workload and driver, but "
                    + combinations
                    + " combinations would overwrite it; omit --output or set "
                    + RESULTS_DIR_ENV
                    + " to choose the results directory";
        }
        if (arguments.expectedWorkers < 2) {
            return "--expected-workers must be at least 2, the minimum for a distributed run";
        }
        if (arguments.retries < 0) {
            return "--retries must be 0 or more";
        }
        if (arguments.repeat != null && arguments.repeat < 1) {
            return "--repeat must be at least 1";
        }
        return null;
    }

    private static void exitWithUsage(JCommander jc, @Nullable String message) {
        log.error(message);
        jc.usage();
        System.exit(-1);
    }

    // Fills the worker list from --workers-file, or from workers.yaml when no workers are given,
    // then expands a dns:// address into one URL per resolved worker
    private static void resolveWorkers(Arguments arguments) throws InterruptedException {
        if (arguments.workers.isEmpty() && arguments.workersFile == null) {
            var defaultFile = new File("workers.yaml");
            if (defaultFile.exists()) {
                log.info("Using default worker file workers.yaml");
                arguments.workersFile = defaultFile;
            }
        }

        if (arguments.workersFile != null) {
            log.info("Reading workers list from {}", arguments.workersFile);
            Workers workers = workersReader.readValue(arguments.workersFile);
            arguments.workers = workers.workers;
        }

        List<String> workers = arguments.workers;
        if (workers.stream().anyMatch(WorkerDiscovery::isDns)) {
            if (workers.size() != 1) {
                throw new IllegalArgumentException(
                        "A dns:// worker address cannot be combined with other worker addresses: " + workers);
            }
            List<String> resolved =
                    WorkerDiscovery.resolve(
                            workers.getFirst(), arguments.expectedWorkers, arguments.workersTimeout);
            arguments.workers = discoveredUrls(resolved, workerCaFile(arguments, System.getenv()));
        }
    }

    // A driver configuration file checked against the published driver schema
    static Map<String, Object> readDriverConfig(String path) {
        JsonNode document = mapper.readTree(new File(path));
        YamlSchemas.validate(YamlSchemas.DRIVER, document, "Invalid driver configuration " + path);
        return driverConfigReader.readValue(document);
    }

    // Workloads keyed and ordered by file name without extension
    static Map<String, Workload> readWorkloads(List<String> paths) {
        Map<String, Workload> workloads = new TreeMap<>();
        for (String path : paths) {
            File file = new File(path);
            String fileName = file.getName();
            int dot = fileName.lastIndexOf('.');
            String name = dot > 0 ? fileName.substring(0, dot) : fileName;

            JsonNode document = mapper.readTree(file);
            YamlSchemas.validate(YamlSchemas.WORKLOAD, document, "Invalid workload " + path);
            Workload workload = workloadReader.readValue(document);
            try {
                if (workload.name == null || workload.name.isBlank()) {
                    throw new IllegalArgumentException("name is required");
                }
                workload.validate();
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid workload " + path + ": " + e.getMessage(), e);
            }
            if (workloads.putIfAbsent(name, workload) != null) {
                // Result files are named after the workload file, so two with the same name collide
                throw new IllegalArgumentException(
                        "Workload " + path + " has the same file name as another workload: " + name);
            }
        }
        return workloads;
    }

    // The certificate https:// workers present: --worker-ca-file, else the environment variable
    static @Nullable Path workerCaFile(Arguments arguments, Map<String, String> environment) {
        if (arguments.workerCaFile != null) {
            return arguments.workerCaFile;
        }
        String fromEnv = environment.get(HttpWorkerClient.CA_FILE_ENV);
        return fromEnv == null || fromEnv.isBlank() ? null : Path.of(fromEnv.strip());
    }

    // Discovery yields http:// URLs; a configured worker certificate means the workers use TLS
    static List<String> discoveredUrls(List<String> resolved, @Nullable Path caFile) {
        return caFile == null
                ? resolved
                : resolved.stream().map(url -> "https" + url.substring("http".length())).toList();
    }

    // Workers use self-signed certificates, so an https:// worker needs the certificate to trust
    static @Nullable String missingWorkerCa(List<String> workers, @Nullable Path caFile) {
        if (caFile == null
                && workers.stream().anyMatch(w -> w.regionMatches(true, 0, "https://", 0, 8))) {
            return "https:// workers need --worker-ca-file or "
                    + HttpWorkerClient.CA_FILE_ENV
                    + " naming the certificate they present";
        }
        return null;
    }

    private static Worker createWorker(Arguments arguments) {
        if (!arguments.workers.isEmpty()) {
            String token =
                    BearerTokenAuth.resolveToken(System.getenv(), arguments.workerTokenFile).orElse(null);
            Path caFile = workerCaFile(arguments, System.getenv());
            String tlsError = missingWorkerCa(arguments.workers, caFile);
            if (tlsError != null) {
                throw new IllegalArgumentException(tlsError);
            }
            List<Worker> workers =
                    arguments.workers.stream()
                            .map(host -> (Worker) HttpWorkerClient.forRemoteWorker(host, token, caFile))
                            .toList();
            log.info("Using distributed workers ensemble");
            if (arguments.driverLocation != null) {
                log.warn(
                        "--driver-location is ignored with remote workers; they load drivers from their"
                                + " own installed plugin bundles");
            }
            return new DistributedWorkersEnsemble(workers, arguments.extraConsumers);
        }
        if (arguments.workerTokenFile != null) {
            log.warn("--worker-token-file is ignored without remote workers");
        }
        if (arguments.workerCaFile != null) {
            log.warn("--worker-ca-file is ignored without remote workers");
        }
        // Run the driver in a child JVM with only its plugin bundle on the classpath
        log.info("Using local driver process worker");
        return new ProcessWorker(
                arguments.driverLocation == null ? null : Path.of(arguments.driverLocation));
    }

    // Throwable is caught on purpose here and in the helpers below: a failed workload, including an
    // Error, is recorded and the worker is still stopped and closed before the failures are rethrown.
    // Returns the process exit code: 3 for a validity failure, 2 for a threshold failure, else 0
    @SuppressWarnings("java:S1181")
    static int runWorkloads(
            Arguments arguments,
            Map<String, Workload> workloads,
            Worker worker,
            GeneratorFactory generatorFactory)
            throws Exception {
        Throwable failures = null;
        boolean thresholdFailed = false;
        Map<String, TestResult> written = new LinkedHashMap<>();
        Map<String, Validity> summaries = new LinkedHashMap<>();
        Runner runner = new Runner(arguments, worker, generatorFactory, written, summaries);
        try {
            Outcome initialStop = stopPreviousWorkload(worker);
            failures = initialStop.failure();
            boolean safeToContinue = initialStop.safeToContinue();

            for (Combination combination : combinations(workloads, arguments.drivers)) {
                if (!safeToContinue) {
                    log.error("Skipping remaining workloads because worker cleanup failed");
                    break;
                }
                Outcome outcome = runner.runRepeated(combination);
                failures = append(failures, outcome.failure());
                thresholdFailed |= outcome.thresholdFailed();
                safeToContinue = outcome.safeToContinue();
            }
            printVerdicts(written);
            writeHtmlReport(arguments.reportHtml, written);
        } catch (Throwable e) {
            failures = append(failures, e);
            log.error("Failed to execute benchmark workloads", e);
        } finally {
            failures = closeWorker(worker, failures);
        }
        if (failures != null) {
            throw new IllegalStateException(
                    "One or more workloads or required cleanup operations failed", failures);
        }
        return exitCode(arguments, written, summaries, thresholdFailed);
    }

    // An invalid measurement makes a threshold verdict meaningless, so the validity code wins.
    // Judges every result and every --repeat summary verdict.
    static int exitCode(
            Arguments arguments,
            Map<String, TestResult> written,
            Map<String, Validity> summaries,
            boolean thresholdFailed) {
        if (arguments.failOnInvalid || arguments.failOnWarning) {
            Validity.Status failAt =
                    arguments.failOnWarning ? Validity.Status.WARNING : Validity.Status.INVALID;
            boolean failed =
                    Stream.concat(
                                    written.values().stream().map(result -> result.validity),
                                    summaries.values().stream())
                            .anyMatch(v -> v != null && v.status().compareTo(failAt) >= 0);
            if (failed) {
                return VALIDITY_FAILURE_EXIT_CODE;
            }
        }
        return thresholdFailed ? THRESHOLD_FAILURE_EXIT_CODE : 0;
    }

    // One line per written result with its verdict and the checks behind it
    static void printVerdicts(Map<String, TestResult> written) {
        if (written.isEmpty()) {
            return;
        }
        log.info("--------------- VALIDITY ---------------");
        written.forEach((file, result) -> {
            Validity validity = Objects.requireNonNull(result.validity, "validity");
            String checks =
                    validity.findings().stream()
                            .filter(f -> f.severity() != Finding.Severity.INFO)
                            .map(Finding::check)
                            .distinct()
                            .collect(Collectors.joining(", "));
            log.info(
                    "{}: {}{}", file, validity.status().label(), checks.isEmpty() ? "" : " (" + checks + ")");
        });
    }

    // Writes the HTML report of the results written so far, when --report-html is given
    static void writeHtmlReport(@Nullable String reportHtml, Map<String, TestResult> written)
            throws IOException {
        if (reportHtml == null || reportHtml.isEmpty() || written.isEmpty()) {
            return;
        }
        log.info("Writing HTML report into {}", reportHtml);
        Path reportPath = Path.of(reportHtml);
        Path parent = reportPath.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(reportPath, HtmlReport.render(written), StandardCharsets.UTF_8);
    }

    // Stops anything a previous run left on the worker
    @SuppressWarnings("java:S1181")
    private static Outcome stopPreviousWorkload(Worker worker) {
        try {
            worker.stopAll();
            return new Outcome(null, true, false, false);
        } catch (Throwable e) {
            log.error("Failed to stop a previous workload", e);
            return new Outcome(e, canContinueAfterFailure(worker, null, e), false, false);
        }
    }

    @SuppressWarnings("java:S1181")
    private static @Nullable Throwable closeWorker(Worker worker, @Nullable Throwable failures) {
        try {
            worker.close();
            return failures;
        } catch (Throwable e) {
            log.error("Failed to close benchmark worker", e);
            return append(failures, e);
        }
    }

    /**
     * One workload run with one driver.
     *
     * @param workloadFileName the workload file name without its extension
     * @param workload the workload
     * @param driverConfig the driver configuration file
     */
    private record Combination(String workloadFileName, Workload workload, String driverConfig) {}

    /**
     * How a combination ended.
     *
     * @param failure the run or cleanup failure, or {@code null}
     * @param safeToContinue whether the worker can run the next combination
     * @param thresholdFailed whether the run completed but failed an SLO threshold
     * @param retryable whether rerunning the combination from the start could fix the failure
     * @param result the result written, or {@code null}
     * @param file the file the result was written to, or {@code null}
     */
    private record Outcome(
            @Nullable Throwable failure,
            boolean safeToContinue,
            boolean thresholdFailed,
            boolean retryable,
            @Nullable TestResult result,
            @Nullable String file) {
        Outcome(
                @Nullable Throwable failure,
                boolean safeToContinue,
                boolean thresholdFailed,
                boolean retryable) {
            this(failure, safeToContinue, thresholdFailed, retryable, null, null);
        }
    }

    // Workloads in name order, each with every driver in the order given
    private static List<Combination> combinations(
            Map<String, Workload> workloads, List<String> driverConfigs) {
        List<Combination> combinations = new ArrayList<>();
        workloads.forEach((name, workload) ->
                driverConfigs.forEach(
                        driverConfig -> combinations.add(new Combination(name, workload, driverConfig))));
        return combinations;
    }

    // The collaborators every run of a benchmark shares; written and summaries collect the files
    private record Runner(
            Arguments arguments,
            Worker worker,
            GeneratorFactory generatorFactory,
            Map<String, TestResult> written,
            Map<String, Validity> summaries) {

        // Runs a combination --repeat (or the workload's repeat) times, each repetition with its own
        // retries, and writes a summary of the run-to-run spread. Thresholds then judge the medians.
        private Outcome runRepeated(Combination combination) {
            Workload workload = combination.workload();
            int repeat =
                    Objects.requireNonNullElse(
                            arguments.repeat, Objects.requireNonNullElse(workload.repeat, 1));
            if (repeat == 1) {
                return runWithRetries(combination, null);
            }
            Throwable failures = null;
            boolean safe = true;
            List<RepeatSummary.Repetition> runs = new ArrayList<>();
            for (int i = 1; i <= repeat && safe; i++) {
                log.info("Repetition {} of {}", i, repeat);
                Outcome outcome = runWithRetries(combination, String.valueOf(i));
                failures = append(failures, outcome.failure());
                safe = outcome.safeToContinue();
                TestResult result = outcome.result();
                if (result == null) {
                    runs.add(
                            new RepeatSummary.Repetition(
                                    i, false, null, reason(Objects.requireNonNull(outcome.failure())), null, null));
                } else {
                    Validity validity = result.validity;
                    runs.add(
                            new RepeatSummary.Repetition(
                                    i,
                                    true,
                                    outcome.file(),
                                    null,
                                    validity == null ? null : validity.status(),
                                    result));
                }
            }
            if (!safe && runs.size() < repeat) {
                log.error("Skipping the remaining repetitions because worker cleanup failed");
            }
            boolean thresholdFailed = false;
            try {
                String driverName = driverName(readDriverConfig(combination.driverConfig()), combination);
                RepeatSummary summary = RepeatSummary.of(workload, driverName, repeat, runs);
                log.info("Repetitions: {} of {} succeeded", summary.succeeded(), repeat);
                thresholdFailed = printChecks(summary.findings(), summary.thresholds());
                String file =
                        writeResult(summary, "repetition summary", combination, driverName, "summary");
                summaries.put(file, summary.validity());
                String status = summary.validity().status().label();
                log.info("{}: {}", file, status);
            } catch (RuntimeException e) {
                log.error("Failed to write the repetition summary", e);
                failures = append(failures, e);
            }
            return new Outcome(failures, safe, thresholdFailed, false);
        }

        // Runs a combination, rerunning it from the start after a failure a retry can fix. Failures of
        // attempts that a later attempt made up for are recorded in the result, not rethrown.
        // repetition is the result file name suffix, or null for a single run.
        private Outcome runWithRetries(Combination combination, @Nullable String repetition) {
            List<String> failedAttempts = new ArrayList<>();
            Throwable failures = null;
            while (true) {
                Outcome outcome = runOne(combination, repetition, failedAttempts);
                Throwable failure = outcome.failure();
                if (failure == null) {
                    return outcome;
                }
                failures = append(failures, failure);
                failedAttempts.add(reason(failure));
                boolean safe = outcome.safeToContinue();
                if (outcome.retryable() && failedAttempts.size() <= arguments.retries && !safe) {
                    // Cleanup failed; a worker that stops cleanly now is back in a known state
                    Outcome reset = stopPreviousWorkload(worker);
                    safe = reset.safeToContinue();
                    if (!safe) {
                        failures = append(failures, reset.failure());
                    }
                }
                if (!outcome.retryable() || failedAttempts.size() > arguments.retries || !safe) {
                    return new Outcome(failures, safe, outcome.thresholdFailed(), false);
                }
                log.warn(
                        "Retrying the workload '{}' for driver '{}', attempt {} of {}, after: {}",
                        combination.workload().name,
                        combination.driverConfig(),
                        failedAttempts.size() + 1,
                        arguments.retries + 1,
                        failedAttempts.getLast());
            }
        }

        // Runs one attempt of a combination and always stops the worker afterwards, so the next one
        // starts clean
        @SuppressWarnings("java:S1181")
        private Outcome runOne(
                Combination combination, @Nullable String repetition, List<String> failedAttempts) {
            Workload workload = combination.workload();
            WorkloadRun generator = null;
            Throwable failure = null;
            boolean safeToContinue = true;
            boolean thresholdFailed = false;
            // A rerun would throw away a completed measurement, and a still-running controller could
            // interfere with it
            boolean measured = false;
            boolean quiescent = true;
            TestResult writtenResult = null;
            String writtenFile = null;
            try {
                Map<String, Object> driverConfig = readDriverConfig(combination.driverConfig());
                String driverName = driverName(driverConfig, combination);
                String runId = RunId.create();
                Instant startedAt = Instant.now();
                worker.setRunContext(new RunContext(runId, workload.name));
                MDC.put(MDC_RUN_ID, runId);
                MDC.put(MDC_WORKLOAD, workload.name);
                MDC.put(MDC_DRIVER, driverName);
                log.info(
                        "--------------- WORKLOAD : {} --- DRIVER : {}---------------",
                        workload.name,
                        driverName);
                worker.initializeDriver(Path.of(combination.driverConfig()));
                generator = Objects.requireNonNull(generatorFactory.create(driverName, workload, worker));
                generator.includeHistograms(arguments.includeHistograms);
                List<WorkerClock> clocksAtStart = WorkerClock.probe(worker);
                TestResult result = runWatched(generator, worker);
                measured = true;
                result.schemaVersion = 3;
                result.attempts = failedAttempts.size() + 1;
                result.failedAttempts = List.copyOf(failedAttempts);
                result.metadata =
                        ResultMetadata.create(
                                runId,
                                startedAt,
                                Instant.now(),
                                worker instanceof DistributedWorkersEnsemble ensemble ? ensemble.workerCount() : 1,
                                workload,
                                driverConfig,
                                WorkerClock.withDrift(clocksAtStart, WorkerClock.probe(worker)));
                result.findings =
                        Objects.requireNonNullElseGet(workload.checks, RunChecks::new)
                                .evaluate(workload, result);
                result.validity = Validity.of(result.findings);
                if (workload.thresholds != null) {
                    result.thresholds = workload.thresholds.evaluate(result);
                }
                thresholdFailed = printChecks(result.findings, result.thresholds);
                // Relative to the summary, which sits next to the results
                writtenFile = writeResult(result, "test result", combination, driverName, repetition);
                written.put(writtenFile, result);
                writtenResult = result;
            } catch (Throwable e) {
                failure = e;
                if (e instanceof Error) {
                    safeToContinue = false;
                }
            } finally {
                try {
                    if (generator == null) {
                        worker.stopAll();
                    } else {
                        generator.close();
                    }
                } catch (Throwable e) {
                    failure = append(failure, e);
                    safeToContinue = safeToContinue && canContinueAfterFailure(worker, generator, e);
                    quiescent = generator == null || isQuiescent(generator);
                }
            }
            boolean interrupted = Thread.currentThread().isInterrupted();
            if (interrupted) {
                safeToContinue = false;
            }
            if (failure != null) {
                log.error(
                        "Failed to run the workload '{}' for driver '{}'",
                        workload.name,
                        combination.driverConfig(),
                        failure);
            }
            MDC.remove(MDC_RUN_ID);
            MDC.remove(MDC_WORKLOAD);
            MDC.remove(MDC_DRIVER);
            boolean retryable =
                    failure != null && !measured && quiescent && !interrupted && isRetryable(failure);
            return new Outcome(
                    failure, safeToContinue, thresholdFailed, retryable, writtenResult, writtenFile);
        }

        // Writes a result file for the combination and returns its name without the directory
        private String writeResult(
                Object value,
                String description,
                Combination combination,
                String driverName,
                @Nullable String suffix) {
            String fileName =
                    resultFileName(
                            arguments.output,
                            combination.workloadFileName(),
                            driverName,
                            System.getenv(RESULTS_DIR_ENV),
                            suffix);
            log.info("Writing {} into {}", description, fileName);
            File file = new File(fileName);
            writer.writeValue(file, value);
            return file.getName();
        }

        // Prints the findings and threshold results; returns true if a threshold failed
        private static boolean printChecks(
                List<Finding> findings, Thresholds.@Nullable Result thresholds) {
            RunChecks.print(findings);
            if (thresholds == null) {
                return false;
            }
            thresholds.print();
            return !thresholds.passed();
        }

        private static String driverName(Map<String, Object> driverConfig, Combination combination) {
            Object name = driverConfig.get("name");
            if (name == null || name.toString().isBlank()) {
                throw new IllegalArgumentException(
                        "Driver configuration " + combination.driverConfig() + " must specify a name");
            }
            return name.toString();
        }

        // Runs the workload under the worker watchdog, which fails it when a worker becomes unhealthy
        private static TestResult runWatched(WorkloadRun generator, Worker worker) throws Exception {
            try (WorkerWatchdog watchdog = WorkerWatchdog.watch(worker, ResultLog::periodsLogged)) {
                try {
                    return generator.run();
                } catch (Exception e) {
                    throw watchdog.explain(e);
                }
            }
        }

        @SuppressWarnings("java:S1181")
        private static boolean isQuiescent(WorkloadRun generator) {
            try {
                return generator.isQuiescentAfterClose();
            } catch (Throwable _) {
                return false;
            }
        }
    }

    // Worker, IO, timeout, stall and unhealthy-worker (WorkerUnhealthyException) failures can pass
    // on a rerun. Configuration errors (invalid YAML,
    // a workload the driver rejects) and Errors fail the same way every time.
    static boolean isRetryable(Throwable failure) {
        if (!(failure instanceof IllegalStateException
                || failure instanceof IOException
                || failure instanceof UncheckedIOException
                || failure instanceof TimeoutException)) {
            return false;
        }
        Deque<Throwable> pending = new ArrayDeque<>(List.of(failure));
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        while (!pending.isEmpty()) {
            Throwable next = pending.pop();
            if (!seen.add(next)) {
                continue;
            }
            if (next instanceof Error
                    || next instanceof IllegalArgumentException
                    || next instanceof JacksonException) {
                return false;
            }
            if (next.getCause() != null) {
                pending.push(next.getCause());
            }
            pending.addAll(Arrays.asList(next.getSuppressed()));
        }
        return true;
    }

    // The failure and, when it wraps another, its root cause, for the result file
    static String reason(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root == failure ? failure.toString() : failure + ", caused by " + root;
    }

    // The explicit output file, or a generated name in the results directory. Without a results
    // directory the generated name is relative to the working directory. A suffix, such as the
    // repetition number, goes before the extension: result.json becomes result-2.json.
    static String resultFileName(
            @Nullable String output,
            String workloadFileName,
            String driverName,
            @Nullable String resultsDirectory,
            @Nullable String suffix) {
        String tail = suffix == null ? "" : "-" + suffix;
        if (output != null && !output.isEmpty()) {
            int dot = output.lastIndexOf('.');
            boolean hasExtension = dot > output.lastIndexOf(File.separatorChar) + 1;
            return hasExtension ? output.substring(0, dot) + tail + output.substring(dot) : output + tail;
        }
        String timestamp = LocalDateTime.now(ZoneId.systemDefault()).format(RESULT_TIMESTAMP);
        // The driver name is free text: path separators or other characters must not end the run
        String safeDriver = driverName.replaceAll("[^\\w .-]", "_");
        String fileName =
                String.format("%s-%s-%s%s.json", workloadFileName, safeDriver, timestamp, tail);
        if (resultsDirectory == null || resultsDirectory.isEmpty()) {
            return fileName;
        }
        return Path.of(resultsDirectory, fileName).toString();
    }

    // Creates the directory resultFileName writes into and checks it is writable
    static void createResultDirectory(@Nullable String output, @Nullable String resultsDirectory)
            throws IOException {
        Path directory = null;
        if (output != null && !output.isEmpty()) {
            directory = Path.of(output).toAbsolutePath().getParent();
        } else if (resultsDirectory != null && !resultsDirectory.isEmpty()) {
            directory = Path.of(resultsDirectory);
        }
        ensureWritableDirectory(directory, "Results directory");
    }

    // Creates the parent directory for --report-html up front and checks it is writable
    static void createReportDirectory(@Nullable String reportHtml) throws IOException {
        if (reportHtml == null || reportHtml.isEmpty()) {
            return;
        }
        Path reportPath = Path.of(reportHtml).toAbsolutePath();
        if (Files.isDirectory(reportPath)) {
            throw new IOException("Report path " + reportPath + " is a directory");
        }
        ensureWritableDirectory(reportPath.getParent(), "Report directory");
        // The report is written after the run; fail now instead of after hours of benchmarking
        if (Files.exists(reportPath) && !Files.isWritable(reportPath)) {
            throw new IOException("Report file " + reportPath + " is not writable");
        }
    }

    private static void ensureWritableDirectory(@Nullable Path directory, String label)
            throws IOException {
        if (directory == null) {
            return;
        }
        Files.createDirectories(directory);
        if (!Files.isWritable(directory)) {
            throw new IOException(label + " " + directory + " is not writable");
        }
    }

    @FunctionalInterface
    interface GeneratorFactory {
        WorkloadRun create(String driverName, Workload workload, Worker worker);
    }

    @SuppressWarnings("java:S1181")
    private static boolean canContinueAfterFailure(
            Worker worker, @Nullable WorkloadRun generator, Throwable failure) {
        if (failure instanceof Error) {
            return false;
        }
        try {
            return worker.canReuseAfterFailure()
                    && (generator == null || generator.isQuiescentAfterClose());
        } catch (Throwable checkFailure) {
            append(failure, checkFailure);
            return false;
        }
    }

    // Explicit YAML nulls ("stages: null") keep the field default instead of breaking non-null fields
    private static final ObjectMapper mapper =
            YAMLMapper.builder()
                    .changeDefaultNullHandling(n -> JsonSetter.Value.forValueNulls(Nulls.SKIP))
                    .build();
    private static final ObjectReader workersReader =
            mapper.readerFor(Workers.class).without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private static final ObjectReader workloadReader =
            mapper.readerFor(Workload.class).with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private static final ObjectReader driverConfigReader =
            mapper.readerFor(new TypeReference<Map<String, Object>>() {});

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final Logger log = LoggerFactory.getLogger(Benchmark.class);

    // Log context keys of a workload run, top-level fields in JSON logs
    private static final String MDC_RUN_ID = "run_id";
    private static final String MDC_WORKLOAD = "workload";
    private static final String MDC_DRIVER = "driver";

    private static final DateTimeFormatter RESULT_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    // Directory for result files written without --output, for example in a read-only container
    static final String RESULTS_DIR_ENV = "OMB_RESULTS_DIR";
}
