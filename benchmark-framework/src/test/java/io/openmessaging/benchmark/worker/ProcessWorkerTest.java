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

import static io.openmessaging.benchmark.plugin.PluginFixtures.compileJar;
import static io.openmessaging.benchmark.plugin.PluginFixtures.descriptor;
import static io.openmessaging.benchmark.plugin.PluginFixtures.driverSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.javalin.Javalin;
import io.openmessaging.benchmark.plugin.HostRuntime;
import io.openmessaging.benchmark.plugin.PluginRegistry;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs drivers in real child JVMs built from plugin bundles with conflicting library versions. */
class ProcessWorkerTest {

    private static final String LIB = "fixture.lib.Lib";

    @TempDir static Path dir;
    private static Path plugins;

    @BeforeAll
    static void buildPlugins() throws IOException {
        plugins = dir.resolve("plugins with spaces");
        Path lib1 = compileJar(dir.resolve("v1/shared-lib.jar"), Map.of(LIB, lib("1")), List.of());
        Path lib2 = compileJar(dir.resolve("v2/shared-lib.jar"), Map.of(LIB, lib("2")), List.of());

        bundle("alpha", "fixture.alpha.AlphaDriver", "\"alpha-v\" + " + isolation(), "", lib1);
        bundle("beta", "fixture.beta.BetaDriver", "\"beta-v\" + " + isolation(), "", lib2);
        bundle("crash", "fixture.crash.CrashDriver", "\"crash\"", "", lib1);
        bundle(
                "options",
                "fixture.options.OptionsDriver",
                "System.getProperty(\"fixture.option\")",
                "",
                lib1);
        bundle(
                "zone", "fixture.zone.ZoneDriver", "System.getProperty(\"zone.id\", \"unset\")", "", lib1);
        bundle(
                "relative",
                "fixture.relative.RelativeDriver",
                "\"relative\"",
                "if (!java.nio.file.Files.exists(config.toAbsolutePath().resolveSibling(\"driver-sibling\")))"
                        + " throw new java.io.IOException(\"missing sibling of driver configuration\");",
                lib1);
        bundle(
                "hung",
                "fixture.hung.HungDriver",
                "\"hung\"",
                "java.nio.file.Files.writeString(java.nio.file.Path.of(System.getProperty(\"fixture.marker\")),"
                        + " Long.toString(ProcessHandle.current().pid()));"
                        + " while (true) { java.util.concurrent.locks.LockSupport.parkNanos(1000000000L); }",
                lib1);
        bundle(
                "hung-close",
                "fixture.hungclose.HungCloseDriver",
                "\"hung-close\"",
                "java.nio.file.Files.writeString(java.nio.file.Path.of(System.getProperty(\"fixture.marker\")),"
                        + " Long.toString(ProcessHandle.current().pid()));",
                "while (true) { java.util.concurrent.locks.LockSupport.parkNanos(1000000000L); }",
                lib1);
        bundle(
                "broken",
                "fixture.broken.BrokenDriver",
                "\"broken\"",
                "throw new java.io.IOException(\"broken driver configuration\");",
                lib1);
    }

    @Test
    void runsEachDriverInItsOwnProcessWithItsOwnLibraries() throws Exception {
        try (ProcessWorker worker = newWorker()) {
            worker.initializeDriver(config("fixture.alpha.AlphaDriver"));
            long alphaPid = worker.pid();
            assertThat(worker.createTopics(new TopicsInfo(1, 1)))
                    .singleElement()
                    .asString()
                    .startsWith("alpha-v1-isolated-");
            assertThat(worker.scrapeMetrics()).contains("messages_sent_total");
            worker.stopAll();
            assertThat(worker.pid()).isEqualTo(alphaPid);

            worker.initializeDriver(config("fixture.beta.BetaDriver"));
            long betaPid = worker.pid();
            assertThat(betaPid).isNotEqualTo(alphaPid);
            assertThat(isAlive(alphaPid)).isFalse();
            assertThat(worker.createTopics(new TopicsInfo(1, 1)))
                    .singleElement()
                    .asString()
                    .startsWith("beta-v2-isolated-");

            worker.close();
            assertThat(worker.pid()).isNull();
            assertThat(isAlive(betaPid)).isFalse();
        }
        assertThatThrownBy(() -> Class.forName(LIB)).isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void reportsDriverInitializationFailures() throws Exception {
        try (ProcessWorker worker = newWorker()) {
            assertThatThrownBy(() -> worker.initializeDriver(config("fixture.broken.BrokenDriver")))
                    .hasMessageContaining("fixture.broken.BrokenDriver")
                    .hasMessageContaining("broken driver configuration");
            assertThat(worker.pid()).isNull();

            assertThatThrownBy(() -> worker.initializeDriver(config("fixture.missing.MissingDriver")))
                    .hasMessageContaining("fixture.missing.MissingDriver")
                    .hasMessageContaining("fixture.alpha.AlphaDriver");
        }
    }

    @Test
    void reportsCrashedDriverProcesses() throws Exception {
        try (ProcessWorker worker = newWorker()) {
            worker.initializeDriver(config("fixture.crash.CrashDriver"));

            assertThatThrownBy(() -> worker.createTopics(new TopicsInfo(1, 1)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("exited with code 42");
            assertThatThrownBy(() -> worker.getCountersStats())
                    .hasMessageContaining("exited with code 42");
            assertThatThrownBy(worker::scrapeMetrics).hasMessageContaining("exited with code 42");

            worker.initializeDriver(config("fixture.alpha.AlphaDriver"));
            assertThat(worker.createTopics(new TopicsInfo(1, 1))).hasSize(1);
        }
    }

    @Test
    void cleansUpWhenDriverStartupIsInterrupted() throws Exception {
        Path driverConfig = config("fixture.alpha.AlphaDriver");
        try (ProcessWorker worker = newWorker()) {
            try {
                Thread.currentThread().interrupt();
                assertThatThrownBy(() -> worker.initializeDriver(driverConfig))
                        .isInstanceOf(InterruptedException.class);
                assertThat(worker.pid()).isNull();
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void cannotReinitializeClosedWorker() throws Exception {
        ProcessWorker worker = newWorker();
        worker.close();
        assertThatThrownBy(() -> worker.initializeDriver(config("fixture.alpha.AlphaDriver")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed ProcessWorker");
    }

    @Test
    void passesQuotedJvmOptionsToDriverProcess() throws Exception {
        String oldOptions = System.getProperty(ProcessWorker.JVM_OPTS_PROPERTY);
        try {
            System.setProperty(
                    ProcessWorker.JVM_OPTS_PROPERTY, "-Xmx256m -Dfixture.option=\"hello world\"");
            try (ProcessWorker worker = newWorker()) {
                worker.initializeDriver(config("fixture.options.OptionsDriver"));
                assertThat(worker.createTopics(new TopicsInfo(1, 1)))
                        .singleElement()
                        .asString()
                        .startsWith("hello world-");
            }
        } finally {
            if (oldOptions == null) {
                System.clearProperty(ProcessWorker.JVM_OPTS_PROPERTY);
            } else {
                System.setProperty(ProcessWorker.JVM_OPTS_PROPERTY, oldOptions);
            }
        }
    }

    @Test
    void parsesQuotedAndEscapedJvmOptions() {
        String oldOptions = System.getProperty(ProcessWorker.JVM_OPTS_PROPERTY);
        try {
            System.setProperty(
                    ProcessWorker.JVM_OPTS_PROPERTY,
                    "-Xmx256m -Da='two words' -Db=three\\ words -Dc=\"say \\\"hi\\\"\""
                            + " -Dd=C:\\data\\file");
            assertThat(ProcessWorker.jvmOptions())
                    .containsExactly(
                            "-Xmx256m",
                            "-Da=two words",
                            "-Db=three words",
                            "-Dc=say \"hi\"",
                            "-Dd=C:\\data\\file");

            System.setProperty(ProcessWorker.JVM_OPTS_PROPERTY, "-Da=\"unterminated");
            assertThatThrownBy(ProcessWorker::jvmOptions)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unterminated quote");
        } finally {
            if (oldOptions == null) {
                System.clearProperty(ProcessWorker.JVM_OPTS_PROPERTY);
            } else {
                System.setProperty(ProcessWorker.JVM_OPTS_PROPERTY, oldOptions);
            }
        }
    }

    @Test
    void forwardsZoneIdAndAllowsDriverJvmOptionsToOverrideIt() throws Exception {
        String oldZone = System.getProperty("zone.id");
        String oldOptions = System.getProperty(ProcessWorker.JVM_OPTS_PROPERTY);
        try {
            System.setProperty("zone.id", "supervisor-zone");
            System.clearProperty(ProcessWorker.JVM_OPTS_PROPERTY);
            try (ProcessWorker worker = newWorker()) {
                worker.initializeDriver(config("fixture.zone.ZoneDriver"));
                assertThat(worker.createTopics(new TopicsInfo(1, 1)))
                        .singleElement()
                        .asString()
                        .startsWith("supervisor-zone-");

                System.setProperty(ProcessWorker.JVM_OPTS_PROPERTY, "-Xmx256m -Dzone.id=driver-zone");
                worker.initializeDriver(config("fixture.zone.ZoneDriver"));
                assertThat(worker.createTopics(new TopicsInfo(1, 1)))
                        .singleElement()
                        .asString()
                        .startsWith("driver-zone-");
            }
        } finally {
            if (oldZone == null) {
                System.clearProperty("zone.id");
            } else {
                System.setProperty("zone.id", oldZone);
            }
            if (oldOptions == null) {
                System.clearProperty(ProcessWorker.JVM_OPTS_PROPERTY);
            } else {
                System.setProperty(ProcessWorker.JVM_OPTS_PROPERTY, oldOptions);
            }
        }
    }

    @Test
    void preservesLocalDriverConfigurationPath() throws Exception {
        Files.writeString(dir.resolve("driver-sibling"), "fixture");
        try (ProcessWorker worker = newWorker()) {
            worker.initializeDriver(config("fixture.relative.RelativeDriver"));
            assertThat(worker.createTopics(new TopicsInfo(1, 1))).hasSize(1);
        }
    }

    @Test
    void remoteSupervisorPropagatesResultsAndReportsDriverCrashes() throws Exception {
        Javalin app = Javalin.create(config -> config.showJavalinBanner = false);
        ProcessWorker worker = newWorker();
        try (worker;
                WorkerHandler handler = new WorkerHandler(app, worker)) {
            app.get("/metrics", ctx -> ctx.result(worker.scrapeMetrics()));
            app.start("127.0.0.1", 0);
            String host = "http://127.0.0.1:" + app.port();
            try (HttpWorkerClient remote = new HttpWorkerClient(host);
                    HttpClient metrics = HttpClient.newHttpClient()) {
                remote.initializeDriver(config("fixture.alpha.AlphaDriver"));
                assertThat(remote.createTopics(new TopicsInfo(1, 1)))
                        .singleElement()
                        .asString()
                        .startsWith("alpha-v1-isolated-");
                HttpRequest metricsRequest = HttpRequest.newBuilder(URI.create(host + "/metrics")).build();
                HttpResponse<String> healthy =
                        metrics.send(metricsRequest, HttpResponse.BodyHandlers.ofString());
                assertThat(healthy.statusCode()).isEqualTo(200);
                assertThat(healthy.body()).contains("messages_sent_total");

                remote.initializeDriver(config("fixture.crash.CrashDriver"));
                assertThatThrownBy(() -> remote.createTopics(new TopicsInfo(1, 1)))
                        .hasMessageContaining("exited with code 42");
                HttpResponse<String> failed =
                        metrics.send(metricsRequest, HttpResponse.BodyHandlers.ofString());
                assertThat(failed.statusCode()).isEqualTo(500);
                assertThat(failed.body()).contains("exited with code 42");
            }
        } finally {
            app.stop();
        }
    }

    @Test
    void terminatesSupervisorAndDriverWhenInitializationHangs() throws Exception {
        verifySupervisorExit(ExitMode.TERMINATE, "fixture.hung.HungDriver");
    }

    @Test
    void terminatesDriverWhenSupervisorIsKilledDuringInitialization() throws Exception {
        verifySupervisorExit(ExitMode.KILL, "fixture.hung.HungDriver");
    }

    @Test
    void terminatesDriverWhenSupervisorIsKilledAndDriverCloseHangs() throws Exception {
        verifySupervisorExit(ExitMode.KILL, "fixture.hungclose.HungCloseDriver");
    }

    @Test
    void closesWorkerWhenDriverInitializationHangs() throws Exception {
        verifySupervisorExit(ExitMode.CLOSE, "fixture.hung.HungDriver");
    }

    private enum ExitMode {
        TERMINATE,
        KILL,
        CLOSE
    }

    private static void verifySupervisorExit(ExitMode mode, String driverClass) throws Exception {
        String suffix = driverClass + "-" + mode;
        Path marker = dir.resolve("hung-" + suffix + ".pid");
        Path output = dir.resolve("hung-" + suffix + ".log");
        Path driverConfig = config(driverClass);
        List<String> command =
                new ArrayList<>(
                        List.of(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "-cp",
                                System.getProperty("java.class.path"),
                                BlockingSupervisor.class.getName(),
                                plugins.toString(),
                                driverConfig.toString()));
        if (mode == ExitMode.CLOSE) {
            command.add(marker.toString());
        }
        ProcessBuilder builder =
                new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile());
        builder
                .environment()
                .put("OMB_DRIVER_JVM_OPTS", "-Xmx256m -Dfixture.marker=\"" + marker + "\"");
        Process supervisor = builder.start();
        long driverPid = -1;
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while ((!Files.exists(marker) || Files.size(marker) == 0)
                    && supervisor.isAlive()
                    && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertThat(Files.exists(marker) && Files.size(marker) > 0)
                    .as("Supervisor output: %s", Files.readString(output))
                    .isTrue();
            driverPid = Long.parseLong(Files.readString(marker));
            if (mode == ExitMode.KILL) {
                supervisor.destroyForcibly();
            } else if (mode == ExitMode.TERMINATE) {
                supervisor.destroy();
            }
            assertThat(supervisor.waitFor(20, TimeUnit.SECONDS))
                    .as("Supervisor output: %s", Files.readString(output))
                    .isTrue();
            if (mode == ExitMode.CLOSE) {
                assertThat(supervisor.exitValue())
                        .as("Supervisor output: %s", Files.readString(output))
                        .isZero();
            }
            assertThat(isAlive(driverPid)).as("Driver process must not outlive its supervisor").isFalse();
        } finally {
            supervisor.destroyForcibly();
            if (driverPid != -1) {
                ProcessHandle.of(driverPid)
                        .filter(ProcessHandle::isAlive)
                        .ifPresent(ProcessHandle::destroyForcibly);
            }
            supervisor.waitFor(5, TimeUnit.SECONDS);
        }
    }

    public static final class BlockingSupervisor {
        public static void main(String[] args) throws Exception {
            Path location = Path.of(args[0]);
            try (ProcessWorker worker =
                    new ProcessWorker(
                            () -> PluginRegistry.discover(List.of(location)), HostRuntime::current)) {
                if (args.length == 2) {
                    worker.initializeDriver(Path.of(args[1]));
                    new CountDownLatch(1).await();
                } else {
                    AtomicReference<Exception> failure = new AtomicReference<>();
                    Thread initializer =
                            Thread.ofPlatform()
                                    .start(
                                            () -> {
                                                try {
                                                    worker.initializeDriver(Path.of(args[1]));
                                                } catch (Exception e) {
                                                    failure.set(e);
                                                }
                                            });
                    Path marker = Path.of(args[2]);
                    while (!Files.exists(marker) && initializer.isAlive()) {
                        Thread.sleep(25);
                    }
                    worker.close();
                    initializer.join();
                    if (failure.get() == null) {
                        throw new IllegalStateException("Hanging initialization unexpectedly succeeded");
                    }
                }
            }
        }
    }

    private static ProcessWorker newWorker() {
        return new ProcessWorker(() -> PluginRegistry.discover(List.of(plugins)), HostRuntime::current);
    }

    private static Path config(String driverClass) throws IOException {
        Path file = Files.createTempFile(dir, "driver", ".yaml");
        Files.writeString(file, "name: test\ndriverClass: " + driverClass + "\n");
        return file;
    }

    private static boolean isAlive(long pid) {
        Optional<ProcessHandle> handle = ProcessHandle.of(pid);
        if (handle.isEmpty()) {
            return false;
        }
        handle
                .get()
                .onExit()
                .completeOnTimeout(null, Duration.ofSeconds(15).toMillis(), TimeUnit.MILLISECONDS)
                .join();
        return handle.get().isAlive();
    }

    private static void bundle(
            String id, String driverClass, String prefix, String initializeBody, Path library)
            throws IOException {
        bundle(id, driverClass, prefix, initializeBody, "", library);
    }

    private static void bundle(
            String id,
            String driverClass,
            String prefix,
            String initializeBody,
            String closeBody,
            Path library)
            throws IOException {
        Path bundle = descriptor(plugins.resolve(id), id, List.of(driverClass), "");
        compileJar(
                bundle.resolve("driver " + id + ".jar"),
                Map.of(driverClass, driverSource(driverClass, prefix, initializeBody, closeBody)),
                List.of(library));
        Files.createDirectories(bundle.resolve("lib"));
        Files.copy(library, bundle.resolve("lib").resolve(library.getFileName()));
    }

    private static String lib(String version) {
        return "package fixture.lib;\npublic class Lib {\n"
                + "  public static String version() { return \""
                + version
                + "\"; }\n}\n";
    }

    // Expression telling whether classes of the other test plugins are visible to a driver
    private static String isolation() {
        return "fixture.lib.Lib.version() + (Thread.currentThread().getContextClassLoader()"
                + ".getResource(\"fixture/alpha/AlphaDriver.class\") != null"
                + " && Thread.currentThread().getContextClassLoader()"
                + ".getResource(\"fixture/beta/BetaDriver.class\") != null ? \"-leaked\" : \"-isolated\")";
    }
}
