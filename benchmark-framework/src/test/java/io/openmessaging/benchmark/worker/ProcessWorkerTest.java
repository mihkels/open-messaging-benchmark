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

import io.openmessaging.benchmark.plugin.HostRuntime;
import io.openmessaging.benchmark.plugin.PluginRegistry;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
            assertThat(worker.scrapeMetrics()).isNotNull();
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

            worker.initializeDriver(config("fixture.alpha.AlphaDriver"));
            assertThat(worker.createTopics(new TopicsInfo(1, 1))).hasSize(1);
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
                .completeOnTimeout(
                        null, Duration.ofSeconds(10).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                .join();
        return handle.get().isAlive();
    }

    private static void bundle(
            String id, String driverClass, String prefix, String initializeBody, Path library)
            throws IOException {
        Path bundle = descriptor(plugins.resolve(id), id, List.of(driverClass), "");
        compileJar(
                bundle.resolve("driver " + id + ".jar"),
                Map.of(driverClass, driverSource(driverClass, prefix, initializeBody)),
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
