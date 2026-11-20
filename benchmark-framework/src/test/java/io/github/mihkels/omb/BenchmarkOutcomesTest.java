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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.mihkels.omb.worker.Worker;
import io.github.mihkels.omb.worker.WorkerUnhealthyException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

class BenchmarkOutcomesTest {
    @TempDir Path directory;

    @Test
    void continuesAfterFailuresAndKeepsSuccessfulResults() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        WorkloadRun first = mock(WorkloadRun.class);
        WorkloadRun success = mock(WorkloadRun.class);
        WorkloadRun last = mock(WorkloadRun.class);
        when(first.run()).thenThrow(new IOException("first failed"));
        when(success.run()).thenReturn(result("successful"));
        when(last.run()).thenThrow(new IOException("last failed"));
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("first", workload("first"));
        workloads.put("successful", workload("successful"));
        workloads.put("last", workload("last"));

        Throwable failure =
                catchThrowable(() ->
                        Benchmark.runWorkloads(
                                arguments,
                                workloads,
                                worker,
                                (driver, workload, ignored) ->
                                        switch (workload.name) {
                                            case "first" -> first;
                                            case "successful" -> success;
                                            default -> last;
                                        }));
        assertThat(messages(failure)).contains("first failed", "last failed");
        assertThat(Files.readString(Path.of(arguments.output))).contains("successful");
        verify(worker, times(3)).initializeDriver(any());
        verify(first).close();
        verify(success).close();
        verify(last).close();
        verify(worker).close();
    }

    @Test
    void reportsCleanupFailuresAndDoesNotReuseAnUnsafeWorker() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        doThrow(new IOException("worker close failed")).when(worker).close();
        WorkloadRun generator = mock(WorkloadRun.class);
        when(generator.run()).thenReturn(result("completed"));
        doThrow(new IllegalStateException("run cleanup failed")).when(generator).close();
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("first", workload("first"));
        workloads.put("never", workload("never"));

        Throwable failure =
                catchThrowable(() ->
                        Benchmark.runWorkloads(arguments, workloads, worker, (d, w, ignored) -> generator));
        assertThat(messages(failure)).contains("run cleanup failed", "worker close failed");
        assertThat(Files.readString(Path.of(arguments.output))).contains("completed");
        verify(worker, times(1)).initializeDriver(any());
        verify(generator, times(1)).close();
        verify(worker).close();
    }

    @Test
    void continuesAfterCleanupFailureWhenTheWorkerWasSafelyReplaced() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        when(worker.canReuseAfterFailure()).thenReturn(true);
        WorkloadRun failedCleanup = mock(WorkloadRun.class);
        WorkloadRun next = mock(WorkloadRun.class);
        when(failedCleanup.run()).thenReturn(result("first"));
        when(failedCleanup.isQuiescentAfterClose()).thenReturn(true);
        doThrow(new IllegalStateException("driver cleanup failed")).when(failedCleanup).close();
        when(next.run()).thenReturn(result("second"));
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("first", workload("first"));
        workloads.put("second", workload("second"));

        assertThatThrownBy(() ->
                        Benchmark.runWorkloads(
                                arguments,
                                workloads,
                                worker,
                                (driver, workload, ignored) ->
                                        workload.name.equals("first") ? failedCleanup : next))
                .hasRootCauseMessage("driver cleanup failed");
        assertThat(Files.readString(Path.of(arguments.output))).contains("second");
        verify(worker, times(2)).initializeDriver(any());
        verify(next).close();
        verify(worker).close();
    }

    @Test
    void doesNotReuseWorkerWhenAControllerCouldNotBeJoined() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        when(worker.canReuseAfterFailure()).thenReturn(true);
        WorkloadRun generator = mock(WorkloadRun.class);
        when(generator.run()).thenReturn(result("first"));
        doThrow(new IllegalStateException("controller did not exit")).when(generator).close();
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("first", workload("first"));
        workloads.put("never", workload("never"));

        assertThatThrownBy(() ->
                        Benchmark.runWorkloads(arguments, workloads, worker, (d, w, ignored) -> generator))
                .hasRootCauseMessage("controller did not exit");
        verify(worker, times(1)).initializeDriver(any());
        verify(worker).close();
    }

    @Test
    void retainsFatalRunAndCleanupFailuresAndSkipsLaterWorkloads() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        doThrow(new IOException("worker close failed")).when(worker).close();
        WorkloadRun generator = mock(WorkloadRun.class);
        when(generator.run()).thenThrow(new AssertionError("fatal run failure"));
        doThrow(new IllegalStateException("run close failed")).when(generator).close();
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("first", workload("first"));
        workloads.put("never", workload("never"));

        Throwable failure =
                catchThrowable(() ->
                        Benchmark.runWorkloads(arguments, workloads, worker, (d, w, ignored) -> generator));
        assertThat(messages(failure))
                .contains("fatal run failure", "run close failed", "worker close failed");
        verify(worker, times(1)).initializeDriver(any());
        verify(worker).close();
    }

    @Test
    void preservesCleanupFailureAfterAnUnexpectedCoordinatorFailure() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        doThrow(new IOException("worker close failed")).when(worker).close();

        Throwable failure =
                catchThrowable(
                        () -> Benchmark.runWorkloads(arguments, null, worker, (d, w, ignored) -> null));
        assertThat(messages(failure)).contains("worker close failed");
        assertThat(failure).hasCauseInstanceOf(NullPointerException.class);
        verify(worker).close();
    }

    @Test
    void continuesAfterFailedInitializationWhenCleanupSucceeds() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        doThrow(new IOException("initialization failed"))
                .doNothing()
                .when(worker)
                .initializeDriver(any());
        WorkloadRun generator = mock(WorkloadRun.class);
        when(generator.run()).thenReturn(result("second"));
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("first", workload("first"));
        workloads.put("second", workload("second"));

        assertThatThrownBy(() ->
                        Benchmark.runWorkloads(arguments, workloads, worker, (d, w, ignored) -> generator))
                .hasRootCauseMessage("initialization failed");
        assertThat(Files.readString(Path.of(arguments.output))).contains("second");
        verify(worker, times(2)).initializeDriver(any());
        verify(worker, times(2)).stopAll();
        verify(worker).close();
    }

    @Test
    void doesNotReuseWorkerAfterAFatalCleanupFailure() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        when(worker.canReuseAfterFailure()).thenReturn(true);
        WorkloadRun generator = mock(WorkloadRun.class);
        when(generator.run()).thenReturn(result("first"));
        when(generator.isQuiescentAfterClose()).thenReturn(true);
        doThrow(new AssertionError("fatal cleanup failure")).when(generator).close();
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("first", workload("first"));
        workloads.put("never", workload("never"));

        assertThatThrownBy(() ->
                        Benchmark.runWorkloads(arguments, workloads, worker, (d, w, ignored) -> generator))
                .hasRootCauseMessage("fatal cleanup failure");
        verify(worker, times(1)).initializeDriver(any());
        verify(worker).close();
    }

    @Test
    void stopsAfterAWorkloadInterruptsTheCoordinator() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        WorkloadRun generator = mock(WorkloadRun.class);
        when(generator.run())
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return result("interrupted");
                });
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("first", workload("first"));
        workloads.put("never", workload("never"));

        try {
            Benchmark.runWorkloads(arguments, workloads, worker, (d, w, ignored) -> generator);
        } finally {
            assertThat(Thread.interrupted()).isTrue();
        }
        assertThat(Files.readString(Path.of(arguments.output))).contains("interrupted");
        verify(worker, times(1)).initializeDriver(any());
        verify(worker).close();
    }

    @Test
    void retriesATransientFailureAndRecordsTheFailedAttempt() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.retries = 3;
        Worker worker = mock(Worker.class);
        WorkloadRun timedOut = mock(WorkloadRun.class);
        WorkloadRun success = mock(WorkloadRun.class);
        when(timedOut.run())
                .thenThrow(
                        new IllegalStateException(
                                "HTTP post http://w1/create-producers timed out after 200 ms",
                                new TimeoutException("Request timeout")));
        when(success.run()).thenReturn(result("retried"));
        List<WorkloadRun> attempts = new ArrayList<>(List.of(timedOut, success));

        assertThat(
                        Benchmark.runWorkloads(
                                arguments,
                                Map.of("retried", workload("retried")),
                                worker,
                                (d, w, ignored) -> attempts.removeFirst()))
                .isZero();
        JsonNode json = new ObjectMapper().readTree(Path.of(arguments.output).toFile());
        assertThat(json.get("attempts").asInt()).isEqualTo(2);
        assertThat(json.get("failedAttempts")).hasSize(1);
        assertThat(json.get("failedAttempts").get(0).asString())
                .contains("timed out after 200 ms", "caused by java.util.concurrent.TimeoutException");
        assertThat(json.get("validity").get("status").asString()).isEqualTo("warning");
        assertThat(json.get("validity").get("findings").findValuesAsString("check"))
                .contains("retried");
        verify(worker, times(2)).initializeDriver(any());
        verify(timedOut).close();
        verify(success).close();
        verify(worker).close();
    }

    @Test
    void givesUpAfterTheRetryLimitAndRunsTheNextWorkload() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.retries = 2;
        Worker worker = mock(Worker.class);
        WorkloadRun flaky = mock(WorkloadRun.class);
        WorkloadRun next = mock(WorkloadRun.class);
        when(flaky.run())
                .thenThrow(
                        new IOException("attempt 1"),
                        new IOException("attempt 2"),
                        new IOException("attempt 3"));
        when(next.run()).thenReturn(result("next"));
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("flaky", workload("flaky"));
        workloads.put("next", workload("next"));

        Throwable failure =
                catchThrowable(() ->
                        Benchmark.runWorkloads(
                                arguments,
                                workloads,
                                worker,
                                (d, workload, ignored) -> workload.name.equals("flaky") ? flaky : next));
        assertThat(messages(failure)).contains("attempt 1", "attempt 2", "attempt 3");
        JsonNode json = new ObjectMapper().readTree(Path.of(arguments.output).toFile());
        assertThat(json.get("workload").asString()).isEqualTo("next");
        assertThat(json.get("attempts").asInt()).isEqualTo(1);
        assertThat(json.has("failedAttempts")).isFalse();
        verify(flaky, times(3)).run();
        verify(worker, times(4)).initializeDriver(any());
        verify(worker).close();
    }

    @Test
    void retriesAfterAFailedCleanupWhenTheWorkerStopsCleanly() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.retries = 1;
        Worker worker = mock(Worker.class);
        // Unknown state after the failed stop, then known after the coordinator's own stopAll
        when(worker.canReuseAfterFailure()).thenReturn(false, true);
        WorkloadRun stalled = mock(WorkloadRun.class);
        WorkloadRun success = mock(WorkloadRun.class);
        when(stalled.run()).thenThrow(new IllegalStateException("workload stalled"));
        when(stalled.isQuiescentAfterClose()).thenReturn(true);
        doThrow(new IllegalStateException("stop timed out")).when(stalled).close();
        when(success.run()).thenReturn(result("recovered"));
        List<WorkloadRun> attempts = new ArrayList<>(List.of(stalled, success));

        assertThat(
                        Benchmark.runWorkloads(
                                arguments,
                                Map.of("recovered", workload("recovered")),
                                worker,
                                (d, w, ignored) -> attempts.removeFirst()))
                .isZero();
        assertThat(Files.readString(Path.of(arguments.output)))
                .contains("recovered", "workload stalled");
        verify(worker, times(2)).stopAll();
        verify(worker, times(2)).initializeDriver(any());
    }

    @Test
    void doesNotRetryAWorkloadTheDriverRejects() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.retries = 3;
        Worker worker = mock(Worker.class);
        WorkloadRun generator = mock(WorkloadRun.class);
        when(generator.run()).thenThrow(new IllegalArgumentException("invalid workload"));

        assertThatThrownBy(() ->
                        Benchmark.runWorkloads(
                                arguments,
                                Map.of("first", workload("first")),
                                worker,
                                (d, w, ignored) -> generator))
                .hasRootCauseMessage("invalid workload");
        verify(worker, times(1)).initializeDriver(any());
    }

    @Test
    void doesNotRetryAnInvalidDriverConfiguration() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.retries = 3;
        Files.writeString(Path.of(arguments.drivers.getFirst()), "driverClass: fixture.Driver\n");
        Worker worker = mock(Worker.class);

        assertThatThrownBy(() ->
                        Benchmark.runWorkloads(
                                arguments, Map.of("first", workload("first")), worker, (d, w, ignored) -> null))
                .hasRootCauseMessage(
                        "Invalid driver configuration "
                                + arguments.drivers.getFirst()
                                + ": required property 'name' not found");
        verify(worker, never()).initializeDriver(any());
    }

    @Test
    void doesNotRetryWithRetriesZero() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        WorkloadRun generator = mock(WorkloadRun.class);
        when(generator.run()).thenThrow(new IOException("transient"));

        assertThatThrownBy(() ->
                        Benchmark.runWorkloads(
                                arguments,
                                Map.of("first", workload("first")),
                                worker,
                                (d, w, ignored) -> generator))
                .hasRootCauseMessage("transient");
        verify(worker, times(1)).initializeDriver(any());
    }

    @Test
    void retriesOnlyFailuresARerunCanFix() {
        assertThat(Benchmark.isRetryable(new IOException("connection refused"))).isTrue();
        assertThat(Benchmark.isRetryable(new IllegalStateException("stalled"))).isTrue();
        assertThat(Benchmark.isRetryable(new WorkerUnhealthyException("w1", "driver FAILED"))).isTrue();
        assertThat(Benchmark.isRetryable(new IllegalArgumentException("bad workload"))).isFalse();
        assertThat(Benchmark.isRetryable(new NullPointerException())).isFalse();
        assertThat(Benchmark.isRetryable(new IllegalStateException(new OutOfMemoryError()))).isFalse();
        IOException withConfigError = new IOException("worker call failed");
        withConfigError.addSuppressed(new IllegalArgumentException("bad config"));
        assertThat(Benchmark.isRetryable(withConfigError)).isFalse();
    }

    @Test
    void processExitStatusReflectsWorkloadOutcome() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Path driver = Path.of(arguments.drivers.getFirst());
        Path output = directory.resolve("child-result.json");
        ProbeResult success = exitProbe(driver, output, false, false, false);
        assertThat(success.exitCode()).as(success.output()).isZero();
        assertThat(Files.exists(output)).isTrue();

        ProbeResult failure = exitProbe(driver, output, true, false, false);
        assertThat(failure.exitCode()).as(failure.output()).isNotZero();
        assertThat(failure.output()).contains("fixture workload failure");
        assertThat(Files.exists(output)).isTrue();
    }

    @Test
    void thresholdFailureContinuesOtherWorkloadsAndClosesWorker() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.reportHtml = directory.resolve("report.html").toString();
        Worker worker = mock(Worker.class);
        WorkloadRun first = mock(WorkloadRun.class);
        WorkloadRun second = mock(WorkloadRun.class);
        TestResult failed = result("first");
        failed.aggregatedPublishLatency99pct = 11;
        when(first.run()).thenReturn(failed);
        TestResult passed = result("second");
        passed.summary = new ResultSummary(0, 0, 0, 1, 0, 0, 0);
        when(second.run()).thenReturn(passed);
        Workload firstWorkload = workload("first");
        firstWorkload.thresholds = new Thresholds();
        firstWorkload.thresholds.maxPublishLatency99Ms = 10.0;
        Workload secondWorkload = workload("second");
        secondWorkload.thresholds = new Thresholds();
        secondWorkload.thresholds.maxPublishLatency99Ms = 10.0;
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("first", firstWorkload);
        workloads.put("second", secondWorkload);

        assertThat(
                        Benchmark.runWorkloads(
                                arguments,
                                workloads,
                                worker,
                                (driver, workload, ignored) -> workload.name.equals("first") ? first : second))
                .isEqualTo(Benchmark.THRESHOLD_FAILURE_EXIT_CODE);
        assertThat(Files.readString(Path.of(arguments.output))).contains("second", "PASS");
        // Both runs went to the same --output file, so the report keeps the last under its name
        assertThat(Files.readString(Path.of(arguments.reportHtml)))
                .contains("\"result.json\"", "second");
        verify(worker, times(2)).initializeDriver(any());
        verify(first).close();
        verify(second).close();
        verify(worker).close();
    }

    @Test
    void thresholdFailureHasDedicatedProcessExitCodeAndKeepsTheResult() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Path driver = Path.of(arguments.drivers.getFirst());
        Path output = directory.resolve("threshold-result.json");

        ProbeResult result = exitProbe(driver, output, false, true, false);

        assertThat(result.exitCode())
                .as(result.output())
                .isEqualTo(Benchmark.THRESHOLD_FAILURE_EXIT_CODE);
        JsonNode thresholds = new ObjectMapper().readTree(Files.readString(output)).get("thresholds");
        assertThat(thresholds.get("status").stringValue()).isEqualTo("FAIL");
        assertThat(thresholds.get("checks").get("minThroughputRatio").get("limit").doubleValue())
                .isEqualTo(0.9);
        assertThat(thresholds.get("checks").get("minThroughputRatio").get("observed").doubleValue())
                .isEqualTo(0.8);
        assertThat(thresholds.get("checks").get("minThroughputRatio").get("status").stringValue())
                .isEqualTo("FAIL");
        assertThat(result.output()).contains("Thresholds: FAIL", "minThroughputRatio");
    }

    @Test
    void forcedRateShortfallIsInvalidAndFailsWithValidityExitCode() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Path driver = Path.of(arguments.drivers.getFirst());
        Path output = directory.resolve("invalid-result.json");

        // The fixture publishes 80 msg/s against a target of 100 and also fails its threshold
        ProbeResult result = exitProbe(driver, output, false, true, true);

        assertThat(result.exitCode())
                .as(result.output())
                .isEqualTo(Benchmark.VALIDITY_FAILURE_EXIT_CODE);
        JsonNode validity = new ObjectMapper().readTree(Files.readString(output)).get("validity");
        assertThat(validity.get("status").stringValue()).isEqualTo("invalid");
        assertThat(validity.get("findings").findValuesAsString("check")).contains("rateShortfall");
        assertThat(result.output()).contains("VALIDITY", "invalid (rateShortfall");
    }

    @Test
    void coordinatorMainExitsNonzeroAndKeepsExistingOutputWhenDriverFails() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Path workload = directory.resolve("workload.yaml");
        Files.writeString(workload, WorkloadYaml.minimal("workload"));
        Path output = Path.of(arguments.output);
        Files.writeString(output, "previous successful result");
        String classpath =
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        String absoluteClasspath =
                Arrays.stream(classpath.split(Pattern.quote(File.pathSeparator)))
                        .map(entry -> Path.of(entry).toAbsolutePath().normalize().toString())
                        .collect(Collectors.joining(File.pathSeparator));
        ProcessBuilder builder =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "-Domb.home=" + directory,
                                "-Domb.plugin.path=",
                                "-cp",
                                absoluteClasspath,
                                Benchmark.class.getName(),
                                "--drivers",
                                arguments.drivers.getFirst(),
                                "--output",
                                output.toString(),
                                workload.toString())
                        .directory(directory.toFile());
        ProbeResult failure = waitForProbe(builder, directory.resolve("coordinator.log"));
        assertThat(failure.exitCode()).as(failure.output()).isNotZero();
        assertThat(failure.output())
                .contains("fixture.Driver", "One or more workloads or required cleanup operations failed");
        assertThat(Files.readString(output)).isEqualTo("previous successful result");
    }

    private ProbeResult exitProbe(
            Path driver, Path output, boolean fail, boolean thresholdFailure, boolean failOnInvalid)
            throws Exception {
        String classpath =
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        ProcessBuilder builder =
                new ProcessBuilder(
                        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-cp",
                        classpath,
                        ExitProbe.class.getName(),
                        driver.toString(),
                        output.toString(),
                        Boolean.toString(fail),
                        Boolean.toString(thresholdFailure),
                        Boolean.toString(failOnInvalid));
        return waitForProbe(builder, directory.resolve("child-" + fail + ".log"));
    }

    private ProbeResult waitForProbe(ProcessBuilder builder, Path log) throws Exception {
        Process process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertThat(process.waitFor(15, TimeUnit.SECONDS)).as("child process exited").isTrue();
            return new ProbeResult(process.exitValue(), Files.readString(log));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private record ProbeResult(int exitCode, String output) {}

    @Test
    void repeatWritesEachRepetitionAndASummary() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.repeat = 3;
        Worker worker = mock(Worker.class);
        List<WorkloadRun> runs = new ArrayList<>();
        for (double rate : new double[] {1000, 1010, 990}) {
            WorkloadRun run = mock(WorkloadRun.class);
            TestResult result = result("repeated");
            result.summary = new ResultSummary(rate, rate, 1, 1, 0, 0, 1000);
            result.aggregatedPublishLatency99pct = 5;
            when(run.run()).thenReturn(result);
            runs.add(run);
        }
        List<WorkloadRun> pending = new ArrayList<>(runs);

        assertThat(
                        Benchmark.runWorkloads(
                                arguments,
                                Map.of("repeated", workload("repeated")),
                                worker,
                                (d, w, ignored) -> pending.removeFirst()))
                .isZero();

        try (var files = Files.list(directory)) {
            assertThat(files.map(f -> f.getFileName().toString()))
                    .containsExactlyInAnyOrder(
                            "driver.yaml",
                            "result-1.json",
                            "result-2.json",
                            "result-3.json",
                            "result-summary.json");
        }
        assertThat(Files.readString(directory.resolve("result-2.json"))).contains("1010");
        JsonNode summary =
                new ObjectMapper().readTree(directory.resolve("result-summary.json").toFile());
        assertThat(summary.get("workload").asString()).isEqualTo("repeated");
        assertThat(summary.get("driver").asString()).isEqualTo("fixture");
        assertThat(summary.get("repetitions").asInt()).isEqualTo(3);
        assertThat(summary.get("succeeded").asInt()).isEqualTo(3);
        assertThat(summary.get("results").get(0).get("file").asString()).isEqualTo("result-1.json");
        assertThat(summary.get("results").get(0).get("succeeded").asBoolean()).isTrue();
        // The in-memory result of a repetition stays out of the summary
        assertThat(summary.get("results").get(0).has("result")).isFalse();
        JsonNode rate = summary.get("metrics").get("publishRateAvg");
        assertThat(rate.get("median").asDouble()).isEqualTo(1000);
        assertThat(rate.get("min").asDouble()).isEqualTo(990);
        assertThat(rate.get("max").asDouble()).isEqualTo(1010);
        assertThat(rate.get("cv").asDouble()).isEqualTo(0.01, within(1e-9));
        assertThat(summary.get("metrics").get("aggregatedPublishLatency99pct").get("cv").asDouble())
                .isZero();
        // Zero end-to-end latency has no meaningful spread
        assertThat(summary.get("metrics").get("aggregatedEndToEndLatency99pct").has("cv")).isFalse();
        assertThat(summary.has("findings")).isFalse();
        // Each repetition has its own verdict; the summary's is the worst of them, tagged by number
        JsonNode second = new ObjectMapper().readTree(directory.resolve("result-2.json").toFile());
        String secondStatus = second.get("validity").get("status").asString();
        assertThat(summary.get("results").get(1).get("validity").asString()).isEqualTo(secondStatus);
        assertThat(summary.get("validity").get("status").asString()).isEqualTo(secondStatus);
        assertThat(summary.get("validity").get("findings"))
                .extracting(f -> f.get("message").asString())
                .anyMatch(m -> m.startsWith("repetition 2: "));
        verify(worker, times(3)).initializeDriver(any());
        for (WorkloadRun run : runs) {
            verify(run).close();
        }
    }

    @Test
    void failedRepetitionIsRecordedAndLeftOutOfTheSpread() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.repeat = 3;
        Worker worker = mock(Worker.class);
        WorkloadRun first = mock(WorkloadRun.class);
        WorkloadRun failed = mock(WorkloadRun.class);
        WorkloadRun third = mock(WorkloadRun.class);
        TestResult firstResult = result("flaky");
        firstResult.summary = new ResultSummary(100, 100, 1, 1, 0, 0, 0);
        TestResult thirdResult = result("flaky");
        thirdResult.summary = new ResultSummary(100, 100, 1, 1, 0, 0, 0);
        when(first.run()).thenReturn(firstResult);
        when(failed.run()).thenThrow(new IOException("repetition failed"));
        when(third.run()).thenReturn(thirdResult);
        List<WorkloadRun> pending = new ArrayList<>(List.of(first, failed, third));

        Throwable failure =
                catchThrowable(() ->
                        Benchmark.runWorkloads(
                                arguments,
                                Map.of("flaky", workload("flaky")),
                                worker,
                                (d, w, ignored) -> pending.removeFirst()));

        assertThat(messages(failure)).contains("repetition failed");
        assertThat(directory.resolve("result-2.json")).doesNotExist();
        JsonNode summary =
                new ObjectMapper().readTree(directory.resolve("result-summary.json").toFile());
        assertThat(summary.get("succeeded").asInt()).isEqualTo(2);
        JsonNode second = summary.get("results").get(1);
        assertThat(second.get("succeeded").asBoolean()).isFalse();
        assertThat(second.has("file")).isFalse();
        assertThat(second.get("failure").asString()).contains("repetition failed");
        JsonNode rate = summary.get("metrics").get("publishRateAvg");
        assertThat(rate.get("median").asDouble()).isEqualTo(100);
        assertThat(rate.get("cv").asDouble()).isZero();
        assertThat(summary.get("findings"))
                .singleElement()
                .satisfies(f -> {
                    assertThat(f.get("check").asString()).isEqualTo("failedRepetitions");
                    assertThat(f.get("severity").asString()).isEqualTo("warning");
                    assertThat(f.get("value").asDouble()).isEqualTo(1);
                });
        assertThat(summary.get("validity").get("status").asString()).isIn("warning", "invalid");
    }

    @Test
    void noSuccessfulRepetitionMakesTheSummaryInvalid() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.repeat = 2;
        WorkloadRun failed = mock(WorkloadRun.class);
        when(failed.run()).thenThrow(new IOException("repetition failed"));

        assertThatThrownBy(() ->
                        Benchmark.runWorkloads(
                                arguments,
                                Map.of("broken", workload("broken")),
                                mock(Worker.class),
                                (d, w, ignored) -> failed))
                .hasRootCauseMessage("repetition failed");

        JsonNode summary =
                new ObjectMapper().readTree(directory.resolve("result-summary.json").toFile());
        assertThat(summary.get("succeeded").asInt()).isZero();
        assertThat(summary.has("metrics")).isFalse();
        assertThat(summary.get("findings").get(0).get("severity").asString()).isEqualTo("invalid");
        assertThat(summary.get("validity").get("status").asString()).isEqualTo("invalid");
    }

    @Test
    void failOnWarningJudgesTheRepetitionSummary() throws Exception {
        for (boolean failOnWarning : new boolean[] {false, true}) {
            Benchmark.Arguments arguments = arguments();
            arguments.repeat = 3;
            arguments.failOnInvalid = true;
            arguments.failOnWarning = failOnWarning;
            List<WorkloadRun> pending = new ArrayList<>();
            for (double rate : new double[] {100, 1000, 100}) {
                WorkloadRun run = mock(WorkloadRun.class);
                TestResult result = result("noisy");
                result.summary = new ResultSummary(rate, rate, 1, 1, 0, 0, 0);
                when(run.run()).thenReturn(result);
                pending.add(run);
            }
            // Every repetition alone is valid, so only the summary's variance warning can fail the run
            Workload workload = workload("noisy");
            workload.checks = new RunChecks();
            workload.checks.disabled =
                    RunChecks.NAMES.stream().filter(n -> !n.equals("runToRunVariance")).toList();

            int exitCode =
                    Benchmark.runWorkloads(
                            arguments,
                            Map.of("noisy", workload),
                            mock(Worker.class),
                            (d, w, ignored) -> pending.removeFirst());

            assertThat(
                            new ObjectMapper()
                                    .readTree(directory.resolve("result-1.json").toFile())
                                    .get("validity")
                                    .get("status")
                                    .asString())
                    .isEqualTo("valid");
            assertThat(exitCode).isEqualTo(failOnWarning ? Benchmark.VALIDITY_FAILURE_EXIT_CODE : 0);
        }
    }

    @Test
    void singleSuccessfulRepetitionReportsNoVariance() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.repeat = 2;
        WorkloadRun success = mock(WorkloadRun.class);
        WorkloadRun failed = mock(WorkloadRun.class);
        TestResult result = result("once");
        result.summary = new ResultSummary(100, 100, 1, 1, 0, 0, 0);
        when(success.run()).thenReturn(result);
        when(failed.run()).thenThrow(new IOException("repetition failed"));
        List<WorkloadRun> pending = new ArrayList<>(List.of(success, failed));

        assertThatThrownBy(() ->
                        Benchmark.runWorkloads(
                                arguments,
                                Map.of("once", workload("once")),
                                mock(Worker.class),
                                (d, w, ignored) -> pending.removeFirst()))
                .hasRootCauseMessage("repetition failed");

        JsonNode summary =
                new ObjectMapper().readTree(directory.resolve("result-summary.json").toFile());
        assertThat(summary.get("succeeded").asInt()).isEqualTo(1);
        assertThat(summary.get("metrics").get("publishRateAvg").get("median").asDouble())
                .isEqualTo(100);
        assertThat(summary.get("metrics").get("publishRateAvg").has("cv")).isFalse();
    }

    @Test
    void highRunToRunVarianceAddsAWarning() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Worker worker = mock(Worker.class);
        List<WorkloadRun> pending = new ArrayList<>();
        for (double rate : new double[] {100, 1000, 100}) {
            WorkloadRun run = mock(WorkloadRun.class);
            TestResult result = result("noisy");
            result.summary = new ResultSummary(rate, rate, 1, 1, 0, 0, 0);
            when(run.run()).thenReturn(result);
            pending.add(run);
        }
        // The workload's repeat applies without --repeat
        Workload workload = workload("noisy");
        workload.repeat = 3;

        Benchmark.runWorkloads(
                arguments, Map.of("noisy", workload), worker, (d, w, ignored) -> pending.removeFirst());

        JsonNode summary =
                new ObjectMapper().readTree(directory.resolve("result-summary.json").toFile());
        assertThat(summary.get("findings"))
                .anySatisfy(finding -> {
                    assertThat(finding.get("check").asString()).isEqualTo("runToRunVariance");
                    assertThat(finding.get("severity").asString()).isEqualTo("warning");
                    assertThat(finding.get("message").asString()).startsWith("publishRateAvg varies");
                    assertThat(finding.get("limit").asDouble()).isEqualTo(0.1);
                });
    }

    @Test
    void thresholdsJudgeTheMedianWhenRepeating() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.repeat = 3;
        List<WorkloadRun> pending = new ArrayList<>();
        for (double p99 : new double[] {5, 20, 8}) {
            WorkloadRun run = mock(WorkloadRun.class);
            TestResult result = result("slo");
            result.summary = new ResultSummary(100, 100, 1, 1, 0, 0, 0);
            result.aggregatedPublishLatency99pct = p99;
            when(run.run()).thenReturn(result);
            pending.add(run);
        }
        Workload workload = workload("slo");
        workload.thresholds = new Thresholds();
        workload.thresholds.maxPublishLatency99Ms = 10.0;

        // The second repetition alone fails, but the median of 8 ms passes
        assertThat(
                        Benchmark.runWorkloads(
                                arguments,
                                Map.of("slo", workload),
                                mock(Worker.class),
                                (d, w, ignored) -> pending.removeFirst()))
                .isZero();
        assertThat(Files.readString(directory.resolve("result-2.json"))).contains("FAIL");
        JsonNode thresholds =
                new ObjectMapper()
                        .readTree(directory.resolve("result-summary.json").toFile())
                        .get("thresholds");
        assertThat(thresholds.get("status").asString()).isEqualTo("PASS");
        assertThat(thresholds.get("checks").get("maxPublishLatency99Ms").get("observed").asDouble())
                .isEqualTo(8);
    }

    @Test
    void lostMessagesInAnyRepetitionFailTheRepeatedRun() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.repeat = 3;
        List<WorkloadRun> pending = new ArrayList<>();
        for (long lost : new long[] {0, 500, 0}) {
            WorkloadRun run = mock(WorkloadRun.class);
            TestResult result = result("lossy");
            result.summary = new ResultSummary(100, 100, 1, 1, 0, 0, 0);
            result.delivery = new Delivery(1000, 1000 - lost, lost, 0, 0);
            when(run.run()).thenReturn(result);
            pending.add(run);
        }
        Workload workload = workload("lossy");
        workload.verifyDelivery = true;
        workload.thresholds = new Thresholds();
        workload.thresholds.maxLostMessages = 0L;

        // The median loss is 0, but the worst repetition's 500 must fail the run
        assertThat(
                        Benchmark.runWorkloads(
                                arguments,
                                Map.of("lossy", workload),
                                mock(Worker.class),
                                (d, w, ignored) -> pending.removeFirst()))
                .isEqualTo(Benchmark.THRESHOLD_FAILURE_EXIT_CODE);
        JsonNode lostCheck =
                new ObjectMapper()
                        .readTree(directory.resolve("result-summary.json").toFile())
                        .get("thresholds")
                        .get("checks")
                        .get("maxLostMessages");
        assertThat(lostCheck.get("status").asString()).isEqualTo("FAIL");
        assertThat(lostCheck.get("observed").asDouble()).isEqualTo(500);
    }

    @Test
    void repeatOneKeepsTheSingleResultFile() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.repeat = 1;
        WorkloadRun run = mock(WorkloadRun.class);
        when(run.run()).thenReturn(result("single"));
        Workload workload = workload("single");
        workload.repeat = 3;

        assertThat(
                        Benchmark.runWorkloads(
                                arguments, Map.of("single", workload), mock(Worker.class), (d, w, i) -> run))
                .isZero();
        try (var files = Files.list(directory)) {
            assertThat(files.map(f -> f.getFileName().toString()))
                    .containsExactlyInAnyOrder("driver.yaml", "result.json");
        }
    }

    private Benchmark.Arguments arguments() throws IOException {
        Path config = directory.resolve("driver.yaml");
        Files.writeString(config, "name: fixture\ndriverClass: fixture.Driver\n");
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.drivers = List.of(config.toString());
        arguments.output = directory.resolve("result.json").toString();
        // Tests above check a single attempt, the behaviour of --retries 0
        arguments.retries = 0;
        return arguments;
    }

    private static Workload workload(String name) {
        Workload workload = new Workload();
        workload.name = name;
        return workload;
    }

    private static TestResult result(String name) {
        TestResult result = new TestResult();
        result.workload = name;
        result.driver = "fixture";
        return result;
    }

    private static List<String> messages(Throwable failure) {
        List<String> result = new ArrayList<>();
        collectMessages(failure, result);
        return result;
    }

    private static void collectMessages(Throwable failure, List<String> result) {
        if (failure == null) {
            return;
        }
        result.add(failure.getMessage());
        collectMessages(failure.getCause(), result);
        for (Throwable suppressed : failure.getSuppressed()) {
            collectMessages(suppressed, result);
        }
    }

    public static final class ExitProbe {
        public static void main(String[] args) throws Exception {
            Worker worker =
                    (Worker)
                            Proxy.newProxyInstance(
                                    Worker.class.getClassLoader(),
                                    new Class<?>[] {Worker.class},
                                    (proxy, method, ignored) -> method.getName().equals("id") ? "fixture" : null);
            Benchmark.Arguments arguments = new Benchmark.Arguments();
            arguments.drivers = List.of(args[0]);
            arguments.output = args[1];
            boolean fail = Boolean.parseBoolean(args[2]);
            boolean thresholdFailure = Boolean.parseBoolean(args[3]);
            arguments.failOnInvalid = Boolean.parseBoolean(args[4]);
            Workload workload = workload("fixture");
            if (thresholdFailure) {
                workload.producerRate = 100;
                workload.thresholds = new Thresholds();
                workload.thresholds.minThroughputRatio = 0.9;
            }
            int exitCode =
                    Benchmark.runWorkloads(
                            arguments,
                            Map.of("fixture", workload),
                            worker,
                            (driver, selectedWorkload, ownedWorker) ->
                                    new WorkloadRun() {
                                        @Override
                                        public TestResult run() throws Exception {
                                            if (fail) {
                                                throw new IOException("fixture workload failure");
                                            }
                                            TestResult result = result("successful");
                                            if (thresholdFailure) {
                                                result.summary = new ResultSummary(80, 80, 0, 0, 0, 0, 100);
                                            }
                                            return result;
                                        }

                                        @Override
                                        public boolean isQuiescentAfterClose() {
                                            return true;
                                        }

                                        @Override
                                        public void close() {
                                            // The fixture run holds no resources
                                        }
                                    });
            if (exitCode != 0) {
                System.exit(exitCode);
            }
        }
    }
}
