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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.worker.Worker;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
                catchThrowable(
                        () ->
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
        verify(worker, times(3)).initializeDriver(any(), any());
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
                catchThrowable(
                        () ->
                                Benchmark.runWorkloads(arguments, workloads, worker, (d, w, ignored) -> generator));
        assertThat(messages(failure)).contains("run cleanup failed", "worker close failed");
        assertThat(Files.readString(Path.of(arguments.output))).contains("completed");
        verify(worker, times(1)).initializeDriver(any(), any());
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

        assertThatThrownBy(
                        () ->
                                Benchmark.runWorkloads(
                                        arguments,
                                        workloads,
                                        worker,
                                        (driver, workload, ignored) ->
                                                workload.name.equals("first") ? failedCleanup : next))
                .hasRootCauseMessage("driver cleanup failed");
        assertThat(Files.readString(Path.of(arguments.output))).contains("second");
        verify(worker, times(2)).initializeDriver(any(), any());
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

        assertThatThrownBy(
                        () ->
                                Benchmark.runWorkloads(arguments, workloads, worker, (d, w, ignored) -> generator))
                .hasRootCauseMessage("controller did not exit");
        verify(worker, times(1)).initializeDriver(any(), any());
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
                catchThrowable(
                        () ->
                                Benchmark.runWorkloads(arguments, workloads, worker, (d, w, ignored) -> generator));
        assertThat(messages(failure))
                .contains("fatal run failure", "run close failed", "worker close failed");
        verify(worker, times(1)).initializeDriver(any(), any());
        verify(worker).close();
    }

    @Test
    void preservesCleanupFailureAfterAnUnexpectedCoordinatorFailure() throws Exception {
        Benchmark.Arguments arguments = arguments();
        arguments.drivers = null;
        Worker worker = mock(Worker.class);
        doThrow(new IOException("worker close failed")).when(worker).close();

        Throwable failure =
                catchThrowable(
                        () ->
                                Benchmark.runWorkloads(
                                        arguments,
                                        Map.of("first", workload("first")),
                                        worker,
                                        (d, w, ignored) -> null));
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
                .initializeDriver(any(), any());
        WorkloadRun generator = mock(WorkloadRun.class);
        when(generator.run()).thenReturn(result("second"));
        Map<String, Workload> workloads = new LinkedHashMap<>();
        workloads.put("first", workload("first"));
        workloads.put("second", workload("second"));

        assertThatThrownBy(
                        () ->
                                Benchmark.runWorkloads(arguments, workloads, worker, (d, w, ignored) -> generator))
                .hasRootCauseMessage("initialization failed");
        assertThat(Files.readString(Path.of(arguments.output))).contains("second");
        verify(worker, times(2)).initializeDriver(any(), any());
        verify(worker, times(2)).stopAll();
        verify(worker).close();
    }

    @Test
    void processExitStatusReflectsWorkloadOutcome() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Path driver = Path.of(arguments.drivers.getFirst());
        Path output = directory.resolve("child-result.json");
        ProbeResult success = exitProbe(driver, output, false);
        assertThat(success.exitCode()).as(success.output()).isZero();
        assertThat(Files.exists(output)).isTrue();

        ProbeResult failure = exitProbe(driver, output, true);
        assertThat(failure.exitCode()).as(failure.output()).isNotZero();
        assertThat(failure.output()).contains("fixture workload failure");
        assertThat(Files.exists(output)).isTrue();
    }

    @Test
    void coordinatorMainExitsNonzeroAndKeepsExistingOutputWhenDriverFails() throws Exception {
        Benchmark.Arguments arguments = arguments();
        Path workload = directory.resolve("workload.yaml");
        Files.writeString(workload, "name: workload\n");
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

    private ProbeResult exitProbe(Path driver, Path output, boolean fail) throws Exception {
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
                        Boolean.toString(fail));
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

    private Benchmark.Arguments arguments() throws IOException {
        Path config = directory.resolve("driver.yaml");
        Files.writeString(config, "name: fixture\ndriverClass: fixture.Driver\n");
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.drivers = List.of(config.toString());
        arguments.output = directory.resolve("result.json").toString();
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
            Benchmark.runWorkloads(
                    arguments,
                    Map.of("fixture", workload("fixture")),
                    worker,
                    (driver, workload, ownedWorker) ->
                            new WorkloadRun() {
                                @Override
                                public TestResult run() throws Exception {
                                    if (fail) {
                                        throw new IOException("fixture workload failure");
                                    }
                                    return result("successful");
                                }

                                @Override
                                public boolean isQuiescentAfterClose() {
                                    return true;
                                }

                                @Override
                                public void close() {}
                            });
        }
    }
}
