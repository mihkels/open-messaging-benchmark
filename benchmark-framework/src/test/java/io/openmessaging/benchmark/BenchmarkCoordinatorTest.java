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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.worker.DistributedWorkersEnsemble;
import io.openmessaging.benchmark.worker.HttpWorkerClient;
import io.openmessaging.benchmark.worker.ProcessWorker;
import io.openmessaging.benchmark.worker.Worker;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

class BenchmarkCoordinatorTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(strings = {"local", "workers", "workers-file"})
    void mainSelectsWorkerTopologyAndWritesTheCompletedResult(String topology) throws Exception {
        Path driver = driver("fixture");
        Path workload = directory.resolve("workload.yaml");
        Files.writeString(workload, "name: workload\nunknownFutureOption: ignored\n");
        Path output = directory.resolve("result.json");
        List<String> arguments =
                new ArrayList<>(
                        List.of(
                                "--drivers",
                                driver.toString(),
                                "--output",
                                output.toString(),
                                "--include-histograms",
                                "--driver-location",
                                directory.toString()));
        List<String> addresses = List.of("http://worker-1:8080", "http://worker-2:8080");
        if (topology.equals("workers")) {
            arguments.addAll(List.of("--workers", String.join(",", addresses), "--extra"));
        } else if (topology.equals("workers-file")) {
            Path workers = directory.resolve("workers.yaml");
            Files.writeString(workers, "workers:\n  - " + String.join("\n  - ", addresses) + "\n");
            arguments.addAll(List.of("--workers-file", workers.toString()));
        }
        arguments.add(workload.toString());
        List<List<?>> processArguments = new ArrayList<>();
        List<String> remoteAddresses = new ArrayList<>();
        List<Worker> selectedWorkers = new ArrayList<>();
        try (var processes =
                        mockConstruction(
                                ProcessWorker.class, (mock, context) -> processArguments.add(context.arguments()));
                var clients =
                        mockConstruction(
                                HttpWorkerClient.class,
                                (mock, context) -> remoteAddresses.add((String) context.arguments().getFirst()));
                var generators =
                        mockConstruction(
                                WorkloadGenerator.class,
                                (mock, context) -> {
                                    selectedWorkers.add((Worker) context.arguments().get(2));
                                    TestResult result = new TestResult();
                                    result.driver = (String) context.arguments().get(0);
                                    result.workload = ((Workload) context.arguments().get(1)).name;
                                    result.publishRate = List.of(42.5);
                                    when(mock.run()).thenReturn(result);
                                })) {
            Benchmark.main(arguments.toArray(String[]::new));

            assertThat(generators.constructed()).hasSize(1);
            verify(generators.constructed().getFirst()).run();
            verify(generators.constructed().getFirst()).includeHistograms(true);
            verify(generators.constructed().getFirst()).close();
            if (topology.equals("local")) {
                assertThat(processes.constructed()).hasSize(1);
                assertThat(processArguments).containsExactly(List.of(directory));
                assertThat(clients.constructed()).isEmpty();
                assertThat(selectedWorkers).containsExactly(processes.constructed().getFirst());
                verify(processes.constructed().getFirst()).initializeDriver(driver);
                verify(processes.constructed().getFirst()).setRunId(anyString());
                verify(processes.constructed().getFirst()).stopAll();
                verify(processes.constructed().getFirst()).close();
            } else {
                assertThat(processes.constructed()).isEmpty();
                assertThat(remoteAddresses).containsExactlyElementsOf(addresses);
                assertThat(selectedWorkers).singleElement().isInstanceOf(DistributedWorkersEnsemble.class);
                for (HttpWorkerClient client : clients.constructed()) {
                    verify(client).initializeDriver(driver);
                    verify(client).setRunId(anyString());
                    verify(client).stopAll();
                    verify(client).close();
                }
            }
        }
        JsonNode json = new ObjectMapper().readTree(Files.readString(output));
        assertThat(json.get("workload").stringValue()).isEqualTo("workload");
        assertThat(json.get("driver").stringValue()).isEqualTo("fixture");
        assertThat(json.get("publishRate").get(0).doubleValue()).isEqualTo(42.5);
        assertThat(json.get("schemaVersion").intValue()).isEqualTo(2);
        assertThat(json.get("metadata").get("runId").stringValue())
                .matches("[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}");
        assertThat(json.get("metadata").get("driverConfig").get("password").stringValue())
                .isEqualTo("[REDACTED]");
        assertThat(json.get("metadata").get("driverConfig").get("authToken").stringValue())
                .isEqualTo("[REDACTED]");
    }

    @Test
    void validatesWorkloadsBeforeConstructingWorkers() throws Exception {
        Path invalid = directory.resolve("invalid.yaml");
        Files.writeString(
                invalid, "name: invalid\nuseRandomizedPayloads: true\nrandomizedPayloadPoolSize: 0\n");
        String[] arguments = {"--drivers", driver("fixture").toString(), invalid.toString()};
        try (var processes = mockConstruction(ProcessWorker.class);
                var clients = mockConstruction(HttpWorkerClient.class)) {
            assertThatThrownBy(() -> Benchmark.main(arguments))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(
                            "Invalid workload " + invalid + ": Randomized payload pool size must be positive");
            assertThat(processes.constructed()).isEmpty();
            assertThat(clients.constructed()).isEmpty();
        }
    }

    @Test
    void runsEveryWorkloadDriverCombinationAndExplicitOutputContainsTheLastResult() throws Exception {
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        Path firstDriver = driver("first");
        Path secondDriver = driver("second");
        arguments.drivers = List.of(firstDriver.toString(), secondDriver.toString());
        arguments.output = directory.resolve("result.json").toString();
        Worker worker = mock(Worker.class);
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("alpha-file", workload("alpha"));
        workloads.put("beta-file", workload("beta"));
        List<String> events = new ArrayList<>();
        Map<String, WorkloadRun> runs = new LinkedHashMap<>();
        for (String combination : List.of("alpha/first", "alpha/second", "beta/first", "beta/second")) {
            WorkloadRun run = mock(WorkloadRun.class);
            TestResult result = new TestResult();
            result.workload = combination.split("/")[0];
            result.driver = combination.split("/")[1];
            result.publishRate = List.of((double) runs.size() + 1);
            when(run.run()).thenReturn(result);
            runs.put(combination, run);
        }

        Benchmark.runWorkloads(
                arguments,
                workloads,
                worker,
                (name, workload, selectedWorker) -> {
                    assertThat(selectedWorker).isSameAs(worker);
                    String combination = workload.name + "/" + name;
                    events.add(combination);
                    return runs.get(combination);
                });

        assertThat(events).containsExactly("alpha/first", "alpha/second", "beta/first", "beta/second");
        verify(worker, times(2)).initializeDriver(firstDriver);
        verify(worker, times(2)).initializeDriver(secondDriver);
        verify(worker).stopAll();
        verify(worker).close();
        for (WorkloadRun run : runs.values()) {
            verify(run).run();
            verify(run).close();
        }
        JsonNode json = new ObjectMapper().readTree(Files.readString(Path.of(arguments.output)));
        assertThat(json.get("workload").stringValue()).isEqualTo("beta");
        assertThat(json.get("driver").stringValue()).isEqualTo("second");
        assertThat(json.get("publishRate").get(0).doubleValue()).isEqualTo(4);
    }

    @Test
    void outputWriteFailureIsReportedAndBothRunAndWorkerAreClosed() throws Exception {
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.drivers = List.of(driver("fixture").toString());
        arguments.output = directory.resolve("missing-parent/result.json").toString();
        Worker worker = mock(Worker.class);
        WorkloadRun run = mock(WorkloadRun.class);
        when(run.run()).thenReturn(new TestResult());
        assertThatThrownBy(
                        () ->
                                Benchmark.runWorkloads(
                                        arguments,
                                        Map.of("workload", workload("workload")),
                                        worker,
                                        (d, w, selected) -> run))
                .isInstanceOf(IllegalStateException.class)
                .hasStackTraceContaining("missing-parent")
                .hasMessage("One or more workloads or required cleanup operations failed");
        assertThat(Path.of(arguments.output)).doesNotExist();
        verify(run).close();
        verify(worker).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void initialStopFailureOnlyAllowsRunsWhenWorkerCanBeSafelyReused(boolean reusable)
            throws Exception {
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.drivers = List.of(driver("fixture").toString());
        arguments.output = directory.resolve("result.json").toString();
        Worker worker = mock(Worker.class);
        when(worker.canReuseAfterFailure()).thenReturn(reusable);
        doThrow(new IllegalStateException("initial stop failed")).when(worker).stopAll();
        WorkloadRun run = mock(WorkloadRun.class);
        when(run.run()).thenReturn(new TestResult());
        assertThatThrownBy(
                        () ->
                                Benchmark.runWorkloads(
                                        arguments,
                                        Map.of("workload", workload("workload")),
                                        worker,
                                        (d, w, selected) -> run))
                .hasRootCauseMessage("initial stop failed");
        if (reusable) {
            verify(run).run();
            verify(run).close();
            assertThat(Path.of(arguments.output)).exists();
        } else {
            verify(worker, never()).initializeDriver(any());
            verify(run, never()).run();
            assertThat(Path.of(arguments.output)).doesNotExist();
        }
        verify(worker).close();
    }

    @ParameterizedTest
    @MethodSource("invalidArguments")
    void mainRejectsInvalidArgumentsBeforeRunningWorkloads(List<String> arguments, String message)
            throws Exception {
        ProcessResult result = invokeMain(arguments);
        assertThat(result.exitCode()).as(result.output()).isNotZero();
        assertThat(result.output()).contains(message);
        assertThat(directory.resolve("result.json")).doesNotExist();
    }

    private static Stream<Arguments> invalidArguments() {
        return Stream.of(
                Arguments.of(List.of(), "At least one workload file is required"),
                Arguments.of(List.of("workload.yaml"), "At least one driver must be given with --drivers"),
                Arguments.of(
                        List.of(
                                "--drivers",
                                "driver.yaml",
                                "--workers",
                                "http://worker",
                                "--workers-file",
                                "workers.yaml",
                                "workload.yaml"),
                        "Only one between --workers and --workers-file can be specified"),
                Arguments.of(List.of("--output"), "Expected a value after parameter"));
    }

    @Test
    void helpDoesNotRequireDriversOrWorkloads() throws Exception {
        ProcessResult result = invokeMain(List.of("--help"));
        assertThat(result.exitCode()).as(result.output()).isZero();
        assertThat(result.output()).contains("messaging-benchmark", "--workers-file", "--drivers");
    }

    private ProcessResult invokeMain(List<String> arguments) throws Exception {
        String classpath =
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        String absoluteClasspath =
                Arrays.stream(classpath.split(Pattern.quote(File.pathSeparator)))
                        .map(entry -> Path.of(entry).toAbsolutePath().normalize().toString())
                        .collect(Collectors.joining(File.pathSeparator));
        List<String> command =
                new ArrayList<>(
                        List.of(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "-cp",
                                absoluteClasspath,
                                Benchmark.class.getName()));
        command.addAll(arguments);
        Path log = directory.resolve("coordinator.log");
        Process process =
                new ProcessBuilder(command)
                        .directory(directory.toFile())
                        .redirectErrorStream(true)
                        .redirectOutput(log.toFile())
                        .start();
        try {
            assertThat(process.waitFor(15, TimeUnit.SECONDS)).as("coordinator exited").isTrue();
            return new ProcessResult(process.exitValue(), Files.readString(log));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private record ProcessResult(int exitCode, String output) {}

    private Path driver(String name) throws IOException {
        Path config = directory.resolve(name + ".yaml");
        Files.writeString(
                config,
                "name: "
                        + name
                        + "\ndriverClass: fixture.Driver\npassword: driver-password\n"
                        + "authToken: driver-token\n");
        return config;
    }

    private static Workload workload(String name) {
        Workload workload = new Workload();
        workload.name = name;
        return workload;
    }
}
