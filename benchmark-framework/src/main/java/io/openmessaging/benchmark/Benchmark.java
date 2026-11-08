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
import static java.util.stream.Collectors.toList;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.openmessaging.benchmark.worker.BearerTokenAuth;
import io.openmessaging.benchmark.worker.DistributedWorkersEnsemble;
import io.openmessaging.benchmark.worker.DriverConfiguration;
import io.openmessaging.benchmark.worker.HttpWorkerClient;
import io.openmessaging.benchmark.worker.ProcessWorker;
import io.openmessaging.benchmark.worker.Worker;
import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.yaml.YAMLFactory;

public final class Benchmark {
    static final int THRESHOLD_FAILURE_EXIT_CODE = 2;

    static class Arguments {

        @Parameter(
                names = {"-c", "--csv"},
                description = "Print results from this directory to a csv file")
        String resultsDir;

        @Parameter(
                names = {"-h", "--help"},
                description = "Help message",
                help = true)
        boolean help;

        @Parameter(
                names = {"-d", "--drivers"},
                description = "Drivers list. eg.: pulsar/pulsar.yaml,kafka/kafka.yaml")
        public List<String> drivers;

        @Parameter(
                names = {"-w", "--workers"},
                description = "List of worker nodes. eg: http://1.2.3.4:8080,http://4.5.6.7:8080")
        public List<String> workers;

        @Parameter(
                names = {"-wf", "--workers-file"},
                description = "Path to a YAML file containing the list of workers addresses")
        public File workersFile;

        @Parameter(
                names = "--worker-token-file",
                description =
                        "File holding the bearer token sent to remote workers; overrides the "
                                + BearerTokenAuth.TOKEN_ENV
                                + " environment variable")
        public Path workerTokenFile;

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
        public String driverLocation;

        @Parameter(description = "Workloads")
        public List<String> workloads;

        @Parameter(
                names = {"-o", "--output"},
                description = "Output",
                required = false)
        public String output;

        @Parameter(
                names = "--include-histograms",
                description = "Include compressed latency histograms")
        public boolean includeHistograms;
    }

    private Benchmark() {
        throw new UnsupportedOperationException(
                "Benchmark constructor is private and should not be called directly");
    }

    static void main(String[] args) throws Exception {
        final Arguments arguments = parseArguments(args);
        resolveWorkers(arguments);

        // Dump configuration variables
        if (log.isInfoEnabled()) {
            log.info("Starting benchmark with config: {}", writer.writeValueAsString(arguments));
        }

        Map<String, Workload> workloads = readWorkloads(arguments.workloads);
        if (log.isInfoEnabled()) {
            log.info("Workloads: {}", writer.writeValueAsString(workloads));
        }

        Worker worker = createWorker(arguments);
        try {
            runWorkloads(arguments, workloads, worker, WorkloadGenerator::new);
        } catch (ThresholdFailureException _) {
            System.exit(thresholdFailureExitCode());
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

        // Not required by JCommander because --csv needs neither
        if (arguments.workloads == null || arguments.workloads.isEmpty()) {
            exitWithUsage(jc, "At least one workload file is required");
        }
        if (arguments.drivers == null || arguments.drivers.isEmpty()) {
            exitWithUsage(jc, "At least one driver must be given with --drivers");
        }
        if (arguments.workers != null && arguments.workersFile != null) {
            log.error("Only one between --workers and --workers-file can be specified");
            System.exit(-1);
        }
        return arguments;
    }

    private static void exitWithUsage(JCommander jc, String message) {
        log.error(message);
        jc.usage();
        System.exit(-1);
    }

    // Fills the worker list from --workers-file, or from workers.yaml when no workers are given
    private static void resolveWorkers(Arguments arguments) {
        if (arguments.workers == null && arguments.workersFile == null) {
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
    }

    // Workloads keyed and ordered by file name without extension
    private static Map<String, Workload> readWorkloads(List<String> paths) {
        Map<String, Workload> workloads = new TreeMap<>();
        for (String path : paths) {
            File file = new File(path);
            String name = file.getName().substring(0, file.getName().lastIndexOf('.'));

            Workload workload = workloadReader.readValue(file);
            try {
                workload.validate();
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid workload " + path + ": " + e.getMessage(), e);
            }
            workloads.put(name, workload);
        }
        return workloads;
    }

    private static Worker createWorker(Arguments arguments) {
        if (arguments.workers != null && !arguments.workers.isEmpty()) {
            String token =
                    BearerTokenAuth.resolveToken(System.getenv(), arguments.workerTokenFile).orElse(null);
            List<Worker> workers =
                    arguments.workers.stream()
                            .map(host -> (Worker) new HttpWorkerClient(host, token))
                            .collect(toList());
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
        // Run the driver in a child JVM with only its plugin bundle on the classpath
        log.info("Using local driver process worker");
        return new ProcessWorker(
                arguments.driverLocation == null ? null : Path.of(arguments.driverLocation));
    }

    // Throwable is caught on purpose here and in the helpers below: a failed workload, including an
    // Error, is recorded and the worker is still stopped and closed before the failures are rethrown
    @SuppressWarnings("java:S1181")
    static void runWorkloads(
            Arguments arguments,
            Map<String, Workload> workloads,
            Worker worker,
            GeneratorFactory generatorFactory)
            throws Exception {
        Throwable failures = null;
        boolean thresholdFailed = false;
        try {
            Outcome initialStop = stopPreviousWorkload(worker);
            failures = initialStop.failure();
            boolean safeToContinue = initialStop.safeToContinue();

            for (Combination combination : combinations(workloads, arguments.drivers)) {
                if (!safeToContinue) {
                    log.error("Skipping remaining workloads because worker cleanup failed");
                    break;
                }
                Outcome outcome =
                        runOne(
                                combination,
                                arguments.output,
                                arguments.includeHistograms,
                                worker,
                                generatorFactory);
                failures = append(failures, outcome.failure());
                thresholdFailed |= outcome.thresholdFailed();
                safeToContinue = outcome.safeToContinue();
            }
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
        if (thresholdFailed) {
            throw new ThresholdFailureException();
        }
    }

    // Stops anything a previous run left on the worker
    @SuppressWarnings("java:S1181")
    private static Outcome stopPreviousWorkload(Worker worker) {
        try {
            worker.stopAll();
            return new Outcome(null, true, false);
        } catch (Throwable e) {
            log.error("Failed to stop a previous workload", e);
            return new Outcome(e, canContinueAfterFailure(worker, null, e), false);
        }
    }

    @SuppressWarnings("java:S1181")
    private static Throwable closeWorker(Worker worker, Throwable failures) {
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
     */
    private record Outcome(Throwable failure, boolean safeToContinue, boolean thresholdFailed) {}

    // Workloads in name order, each with every driver in the order given
    private static List<Combination> combinations(
            Map<String, Workload> workloads, List<String> driverConfigs) {
        List<Combination> combinations = new ArrayList<>();
        workloads.forEach((name, workload) ->
                driverConfigs.forEach(
                        driverConfig -> combinations.add(new Combination(name, workload, driverConfig))));
        return combinations;
    }

    // Runs one combination and always stops the worker afterwards, so the next one starts clean
    @SuppressWarnings("java:S1181")
    private static Outcome runOne(
            Combination combination,
            String output,
            boolean includeHistograms,
            Worker worker,
            GeneratorFactory generatorFactory) {
        Workload workload = combination.workload();
        WorkloadRun generator = null;
        Throwable failure = null;
        boolean safeToContinue = true;
        boolean thresholdFailed = false;
        try {
            Map<String, Object> driverConfig =
                    driverConfigReader.readValue(new File(combination.driverConfig()));
            DriverConfiguration driverConfiguration =
                    driverConfigurationReader.readValue(new File(combination.driverConfig()));
            String runId = RunId.create();
            Instant startedAt = Instant.now();
            worker.setRunId(runId);
            log.info(
                    "--------------- WORKLOAD : {} --- DRIVER : {}---------------",
                    workload.name,
                    driverConfiguration.name);
            worker.initializeDriver(Path.of(combination.driverConfig()));
            generator =
                    Objects.requireNonNull(
                            generatorFactory.create(driverConfiguration.name, workload, worker));
            generator.includeHistograms(includeHistograms);
            TestResult result = generator.run();
            result.schemaVersion = 2;
            result.metadata =
                    ResultMetadata.create(
                            runId,
                            startedAt,
                            Instant.now(),
                            worker instanceof DistributedWorkersEnsemble ensemble ? ensemble.workerCount() : 1,
                            workload,
                            driverConfig);
            if (workload.thresholds != null) {
                result.thresholds = workload.thresholds.evaluate(result);
                result.thresholds.print();
                thresholdFailed = !result.thresholds.passed();
            }
            String fileName =
                    resultFileName(
                            output,
                            combination.workloadFileName(),
                            driverConfiguration.name,
                            System.getenv(RESULTS_DIR_ENV));
            log.info("Writing test result into {}", fileName);
            writer.writeValue(new File(fileName), result);
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
            }
        }
        if (Thread.currentThread().isInterrupted()) {
            safeToContinue = false;
        }
        if (failure != null) {
            log.error(
                    "Failed to run the workload '{}' for driver '{}'",
                    workload.name,
                    combination.driverConfig(),
                    failure);
        }
        return new Outcome(failure, safeToContinue, thresholdFailed);
    }

    // The explicit output file, or a generated name in the results directory. Without a results
    // directory the generated name is relative to the working directory.
    static String resultFileName(
            String output, String workloadFileName, String driverName, String resultsDirectory) {
        if (output != null && !output.isEmpty()) {
            return output;
        }
        String timestamp = LocalDateTime.now(ZoneId.systemDefault()).format(RESULT_TIMESTAMP);
        String fileName = String.format("%s-%s-%s.json", workloadFileName, driverName, timestamp);
        if (resultsDirectory == null || resultsDirectory.isEmpty()) {
            return fileName;
        }
        return Path.of(resultsDirectory, fileName).toString();
    }

    @FunctionalInterface
    interface GeneratorFactory {
        WorkloadRun create(String driverName, Workload workload, Worker worker);
    }

    @SuppressWarnings("java:S1181")
    private static boolean canContinueAfterFailure(
            Worker worker, WorkloadRun generator, Throwable failure) {
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

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectReader workersReader =
            mapper.readerFor(Workers.class).without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private static final ObjectReader workloadReader =
            mapper.readerFor(Workload.class).without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private static final ObjectReader driverConfigurationReader =
            mapper
                    .readerFor(DriverConfiguration.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private static final ObjectReader driverConfigReader =
            mapper.readerFor(new TypeReference<Map<String, Object>>() {});

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final Logger log = LoggerFactory.getLogger(Benchmark.class);

    private static final DateTimeFormatter RESULT_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    // Directory for result files written without --output, for example in a read-only container
    static final String RESULTS_DIR_ENV = "OMB_RESULTS_DIR";

    static final class ThresholdFailureException extends RuntimeException {
        ThresholdFailureException() {
            super("One or more workload threshold checks failed");
        }
    }

    static int thresholdFailureExitCode() {
        return THRESHOLD_FAILURE_EXIT_CODE;
    }
}
