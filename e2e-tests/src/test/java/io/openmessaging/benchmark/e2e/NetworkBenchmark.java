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
package io.openmessaging.benchmark.e2e;

import io.openmessaging.benchmark.Workload;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;

/** Runs the packaged benchmark and its isolated driver process on a broker network. */
final class NetworkBenchmark implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NetworkBenchmark.class);
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper JSON = new ObjectMapper();
    private static Future<String> image;

    private final GenericContainer<?> container;
    private final Path artifactsRoot;

    NetworkBenchmark(Network network) {
        this(
                network,
                Path.of(System.getProperty("omb.home")).resolve("e2e-tests/target/testcontainers-runs"));
    }

    NetworkBenchmark(Network network, Path artifactsRoot) {
        this.artifactsRoot = artifactsRoot;
        container =
                new GenericContainer<>(runnerImage())
                        .withNetwork(network)
                        .withEnv(
                                "OMB_DRIVER_JVM_OPTS",
                                "-Xms64m -Xmx512m -XX:+UseG1GC --enable-native-access=ALL-UNNAMED"
                                        + " --add-opens=java.base/java.nio=ALL-UNNAMED"
                                        + " --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"
                                        + " --add-opens=java.base/java.util.zip=ALL-UNNAMED"
                                        + " -Dio.netty.tryReflectionSetAccessible=true"
                                        + " -Dorg.apache.pulsar.shade.io.netty.tryReflectionSetAccessible=true")
                        .withCommand("sleep", "infinity");
    }

    void start() {
        container.start();
    }

    SmokeResult run(Path config, Workload workload) throws Exception {
        return run(Files.readString(config), workload);
    }

    SmokeResult run(String config, Workload workload) throws Exception {
        return run(config, workload, FunctionalSmoke.class);
    }

    SmokeResult run(String config, Workload workload, Class<?> mainClass) throws Exception {
        String runId = UUID.randomUUID().toString();
        log.info("Starting networked functional check {}", runId);
        String root = "/tmp/functional-smoke-" + runId;
        String driverFile = "/tmp/driver-" + runId + ".yaml";
        String workloadFile = "/tmp/workload-" + runId + ".yaml";
        String outputFile = "/tmp/result-" + runId + ".json";
        Container.ExecResult mkdir =
                container.execInContainer("mkdir", "-p", root + "/io/openmessaging/benchmark/e2e");
        requireSuccessfulRun(mkdir.getExitCode(), mkdir.getStdout() + mkdir.getStderr());
        copyClass(FunctionalSmoke.class, root);
        copyClass(SmokeResult.class, root);
        if (mainClass != FunctionalSmoke.class) {
            copyClass(mainClass, root);
        }
        container.copyFileToContainer(Transferable.of(config), driverFile);
        container.copyFileToContainer(Transferable.of(YAML.writeValueAsBytes(workload)), workloadFile);
        Container.ExecResult execution =
                container.execInContainer(
                        "timeout",
                        "180s",
                        "java",
                        "--enable-native-access=ALL-UNNAMED",
                        "-Xmx256m",
                        "-cp",
                        root + ":/benchmark/lib/*",
                        mainClass.getName(),
                        driverFile,
                        workloadFile,
                        outputFile);
        String output = execution.getStdout() + "\n" + execution.getStderr();
        Path artifacts = artifactsRoot.resolve(runId);
        Files.createDirectories(artifacts);
        Files.writeString(artifacts.resolve("benchmark.log"), output);
        Path result = artifacts.resolve("result.json");
        Container.ExecResult resultExists = container.execInContainer("test", "-f", outputFile);
        if (resultExists.getExitCode() == 0) {
            try {
                container.copyFileFromContainer(outputFile, result.toString());
            } catch (RuntimeException e) {
                throw new AssertionError("Could not retain benchmark result " + outputFile + "\n" + output, e);
            }
        } else if (resultExists.getExitCode() != 1) {
            throw new AssertionError(
                    "Could not check benchmark result "
                            + outputFile
                            + ": "
                            + resultExists.getStderr()
                            + "\n"
                            + output);
        }
        requireSuccessfulRun(execution.getExitCode(), output);
        try {
            SmokeResult smoke = JSON.readValue(result.toFile(), SmokeResult.class);
            log.info(
                    "Completed networked functional check {}: {} sent, {} received",
                    runId,
                    smoke.messagesSent(),
                    smoke.messagesReceived());
            return smoke;
        } catch (RuntimeException e) {
            throw new AssertionError(
                    "Smoke check did not produce a fresh, parseable result at "
                            + outputFile
                            + "\n"
                            + output,
                    e);
        }
    }

    static void requireSuccessfulRun(int exitCode, String output) {
        if (exitCode != 0 || output.contains("Failed to run the workload")) {
            throw new AssertionError("Container command failed with exit code " + exitCode + ":\n" + output);
        }
    }

    private void copyClass(Class<?> type, String root) throws IOException {
        String resource = "/" + type.getName().replace('.', '/') + ".class";
        try (InputStream classFile = type.getResourceAsStream(resource)) {
            if (classFile == null) {
                throw new IllegalStateException("Class file is missing: " + type.getName());
            }
            container.copyFileToContainer(Transferable.of(classFile.readAllBytes()), root + resource);
        }
    }

    Container.ExecResult exec(String... command) throws IOException, InterruptedException {
        return container.execInContainer(command);
    }

    void verifyOrderedPayloads(String plugin, String driverClass, Path config) throws Exception {
        runProbe(plugin, driverClass, List.of(), config, null, false);
    }

    void verifyOrderedPayloadsAndTimestamps(String plugin, String driverClass, Path config)
            throws Exception {
        runProbe(plugin, driverClass, List.of(), config, null, true);
    }

    void verifySelector(
            String plugin, String driverClass, String delegate, Path config, String unmatchedConfig)
            throws Exception {
        runProbe(plugin, driverClass, List.of(delegate), config, unmatchedConfig, false);
    }

    void verifySelector(String plugin, String driverClass, Path config, String unmatchedConfig)
            throws Exception {
        runProbe(plugin, driverClass, List.of(), config, unmatchedConfig, false);
    }

    private void runProbe(
            String plugin,
            String driverClass,
            List<String> delegates,
            Path config,
            String unmatchedConfig,
            boolean checkTimestamps)
            throws Exception {
        log.info("Starting {} driver message probe", plugin);
        String root = "/tmp/message-probe-" + UUID.randomUUID();
        String packageDir = root + "/io/openmessaging/benchmark/e2e";
        Container.ExecResult mkdir = container.execInContainer("mkdir", "-p", packageDir);
        requireSuccessfulRun(mkdir.getExitCode(), mkdir.getStdout() + mkdir.getStderr());
        try (InputStream classFile =
                DriverMessageProbe.class.getResourceAsStream("DriverMessageProbe.class")) {
            if (classFile == null) {
                throw new IllegalStateException("Driver message probe class is missing");
            }
            container.copyFileToContainer(
                    Transferable.of(classFile.readAllBytes()), packageDir + "/DriverMessageProbe.class");
        }
        String configPath = root + "/driver.yaml";
        copy(config, configPath);
        StringBuilder classpath = new StringBuilder(root).append(":/benchmark/lib/*");
        List<String> pluginIds = new ArrayList<>();
        pluginIds.add(plugin);
        pluginIds.addAll(delegates);
        for (String id : pluginIds) {
            if (!id.matches("[a-z0-9-]+")) {
                throw new IllegalArgumentException("Invalid plugin name: " + id);
            }
            String pluginDir = "/benchmark/drivers/driver-" + id;
            classpath
                    .append(':')
                    .append(pluginDir)
                    .append("/*:")
                    .append(pluginDir)
                    .append("/lib/*:")
                    .append(pluginDir)
                    .append("/ext/*");
        }
        List<String> command =
                new ArrayList<>(
                        List.of(
                        "timeout",
                        "180s",
                        "java",
                        "--enable-native-access=ALL-UNNAMED",
                        "--add-opens",
                        "java.base/java.nio=ALL-UNNAMED",
                        "--add-opens",
                        "java.base/jdk.internal.misc=ALL-UNNAMED",
                        "-cp",
                        classpath.toString(),
                        DriverMessageProbe.class.getName(),
                        driverClass,
                        configPath));
        if (unmatchedConfig != null) {
            String unmatchedPath = root + "/unmatched.yaml";
            container.copyFileToContainer(Transferable.of(unmatchedConfig), unmatchedPath);
            command.add(unmatchedPath);
        } else if (checkTimestamps) {
            command.add("--timestamps");
        }
        Container.ExecResult result = container.execInContainer(command.toArray(String[]::new));
        requireSuccessfulRun(result.getExitCode(), result.getStdout() + "\n" + result.getStderr());
        log.info("Completed {} driver message probe", plugin);
    }

    void copy(Path source, String destination) {
        container.copyFileToContainer(MountableFile.forHostPath(source), destination);
    }

    @Override
    public void close() {
        container.close();
    }

    private static synchronized Future<String> runnerImage() {
        if (image == null) {
            String distribution = System.getProperty("omb.e2e.distribution");
            if (distribution == null || !Files.isRegularFile(Path.of(distribution))) {
                throw new IllegalStateException(
                        "The benchmark distribution is missing at "
                                + distribution
                                + "; run Maven with -pl e2e-tests -am -De2e.");
            }
            image =
                    new ImageFromDockerfile()
                            .withFileFromPath("distribution.tar.gz", Path.of(distribution))
                            .withFileFromString(
                                    "Dockerfile",
                                    """
                                    FROM eclipse-temurin:27-jre
                                    ADD distribution.tar.gz /opt/
                                    RUN mv /opt/openmessaging-benchmark-* /benchmark && \\
                                        mkdir -p /benchmark/drivers/driver-jms/ext
                                    WORKDIR /benchmark
                                    ENV OMB_HOME=/benchmark
                                    """);
        }
        return image;
    }
}
