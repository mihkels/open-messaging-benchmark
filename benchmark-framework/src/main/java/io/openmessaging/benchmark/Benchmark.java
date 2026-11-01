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

import static java.util.stream.Collectors.toList;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.openmessaging.benchmark.worker.DistributedWorkersEnsemble;
import io.openmessaging.benchmark.worker.HttpWorkerClient;
import io.openmessaging.benchmark.worker.ProcessWorker;
import io.openmessaging.benchmark.worker.Worker;
import java.io.File;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.yaml.YAMLFactory;

public final class Benchmark {

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
                description =
                        "Drivers list. eg.: pulsar/pulsar.yaml,kafka/kafka.yaml") // , required = true)
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
                names = {"-x", "--extra"},
                description = "Allocate extra consumer workers when your backlog builds.")
        boolean extraConsumers;

        @Parameter(
                names = {"--legacy-driver-name"},
                description =
                        "Load the driver from this plugin bundle directory, unpackaged driver module"
                                + " directory or jar instead of the installed plugin bundles (local runs only)")
        public String legacyDriverName;

        @Parameter(description = "Workloads") // , required = true)
        public List<String> workloads;

        @Parameter(
                names = {"-o", "--output"},
                description = "Output",
                required = false)
        public String output;
    }

    private Benchmark() {
        throw new UnsupportedOperationException(
                "Benchmark constructor is private and should not be called directly");
    }

    static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        var jc = new JCommander(arguments);
        jc.setProgramName("messaging-benchmark");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(-1);
        }

        if (arguments.resultsDir != null) {
            ResultsToCsv r = new ResultsToCsv();
            r.writeAllResultFiles(arguments.resultsDir);
            System.exit(0);
        }

        if (arguments.workers != null && arguments.workersFile != null) {
            System.err.println("Only one between --workers and --workers-file can be specified");
            System.exit(-1);
        }

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

        // Dump configuration variables
        log.info("Starting benchmark with config: {}", writer.writeValueAsString(arguments));

        Map<String, Workload> workloads = new TreeMap<>();
        for (String path : arguments.workloads) {
            File file = new File(path);
            String name = file.getName().substring(0, file.getName().lastIndexOf('.'));

            workloads.put(name, workloadReader.readValue(file));
        }

        log.info("Workloads: {}", writer.writeValueAsString(workloads));

        Worker worker;

        if (arguments.workers != null && !arguments.workers.isEmpty()) {
            List<Worker> workers =
                    arguments.workers.stream().map(HttpWorkerClient::new).collect(toList());
            worker = new DistributedWorkersEnsemble(workers, arguments.extraConsumers);
            log.info("Using distributed workers ensemble");
            if (arguments.legacyDriverName != null) {
                log.warn(
                        "--legacy-driver-name is ignored with remote workers; they load drivers from their"
                                + " own installed plugin bundles");
            }
        } else {
            // Run the driver in a child JVM with only its plugin bundle on the classpath
            worker = new ProcessWorker();
            log.info("Using local driver process worker");
        }

        runWorkloads(arguments, workloads, worker, WorkloadGenerator::new);
    }

    static void runWorkloads(
            Arguments arguments,
            Map<String, Workload> workloads,
            Worker worker,
            GeneratorFactory generatorFactory)
            throws Exception {
        Throwable failures = null;
        boolean safeToContinue = true;
        try {
            try {
                worker.stopAll();
            } catch (Throwable e) {
                failures = append(failures, e);
                safeToContinue = canContinueAfterFailure(worker, null, e);
                log.error("Failed to stop a previous workload", e);
            }

            combinations:
            for (Map.Entry<String, Workload> entry : workloads.entrySet()) {
                for (String driverConfig : arguments.drivers) {
                    if (!safeToContinue) {
                        log.error("Skipping remaining workloads because worker cleanup failed");
                        break combinations;
                    }

                    Workload workload = entry.getValue();
                    WorkloadRun generator = null;
                    Throwable runFailure = null;
                    try {
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
                        DriverConfiguration driverConfiguration =
                                driverConfigurationReader.readValue(new File(driverConfig));
                        log.info(
                                "--------------- WORKLOAD : {} --- DRIVER : {}---------------",
                                workload.name,
                                driverConfiguration.name);
                        Path driverLocation =
                                arguments.legacyDriverName != null ? Path.of(arguments.legacyDriverName) : null;
                        worker.initializeDriver(Path.of(driverConfig), driverLocation);
                        generator =
                                Objects.requireNonNull(
                                        generatorFactory.create(driverConfiguration.name, workload, worker));
                        TestResult result = generator.run();
                        boolean useOutput = arguments.output != null && !arguments.output.isEmpty();
                        String timestamp = dateFormat.format(new Date());
                        String fileName =
                                useOutput
                                        ? arguments.output
                                        : String.format(
                                                "%s-%s-%s.json", entry.getKey(), driverConfiguration.name, timestamp);
                        log.info("Writing test result into {}", fileName);
                        writer.writeValue(new File(fileName), result);
                    } catch (Throwable e) {
                        runFailure = e;
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
                            runFailure = append(runFailure, e);
                            safeToContinue = safeToContinue && canContinueAfterFailure(worker, generator, e);
                        }
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        safeToContinue = false;
                    }
                    if (runFailure != null) {
                        log.error(
                                "Failed to run the workload '{}' for driver '{}'",
                                workload.name,
                                driverConfig,
                                runFailure);
                        failures = append(failures, runFailure);
                    }
                }
            }
        } catch (Throwable e) {
            failures = append(failures, e);
            log.error("Failed to execute benchmark workloads", e);
        } finally {
            try {
                worker.close();
            } catch (Throwable e) {
                log.error("Failed to close benchmark worker", e);
                failures = append(failures, e);
            }
        }
        if (failures != null) {
            throw new IllegalStateException(
                    "One or more workloads or required cleanup operations failed", failures);
        }
    }

    @FunctionalInterface
    interface GeneratorFactory {
        WorkloadRun create(String driverName, Workload workload, Worker worker);
    }

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

    private static Throwable append(Throwable primary, Throwable next) {
        if (primary == null) {
            return next;
        }
        if (next != null && primary != next) {
            primary.addSuppressed(next);
        }
        return primary;
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

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final Logger log = LoggerFactory.getLogger(Benchmark.class);
}
