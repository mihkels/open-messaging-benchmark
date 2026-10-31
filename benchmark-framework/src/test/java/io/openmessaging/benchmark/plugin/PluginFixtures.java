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
package io.openmessaging.benchmark.plugin;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

/** Builds plugin bundles, jars and driver classes for tests. */
public final class PluginFixtures {

    private PluginFixtures() {}

    /**
     * Compiles {@code sources} against the driver API into {@code jar}.
     *
     * @param jar the jar to write
     * @param sources source code by fully qualified class name
     * @param extraClasspath additional compile classpath entries
     * @return {@code jar}
     * @throws IOException if the jar cannot be written
     */
    public static Path compileJar(Path jar, Map<String, String> sources, List<Path> extraClasspath)
            throws IOException {
        try (TemporaryDirectory work =
                new TemporaryDirectory(Files.createTempDirectory("omb-fixture-"))) {
            return compileJar(jar, sources, extraClasspath, work.path());
        }
    }

    private static Path compileJar(
            Path jar, Map<String, String> sources, List<Path> extraClasspath, Path work)
            throws IOException {
        Path src = work.resolve("src");
        Path classes = work.resolve("classes");
        Files.createDirectories(classes);
        List<String> args = new ArrayList<>();
        List<Path> classpath = new ArrayList<>(driverApiClasspath());
        classpath.addAll(extraClasspath);
        args.add("-classpath");
        args.add(
                classpath.stream()
                        .map(Path::toString)
                        .collect(Collectors.joining(java.io.File.pathSeparator)));
        args.add("-d");
        args.add(classes.toString());
        args.add("-proc:none");
        for (Map.Entry<String, String> source : sources.entrySet()) {
            Path file = src.resolve(source.getKey().replace('.', '/') + ".java");
            Files.createDirectories(file.getParent());
            Files.writeString(file, source.getValue());
            args.add(file.toString());
        }
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StringWriter errors = new StringWriter();
        int result =
                compiler.run(
                        null,
                        new WriterOutputStream(errors),
                        new WriterOutputStream(errors),
                        args.toArray(String[]::new));
        if (result != 0) {
            throw new IllegalStateException("Compilation failed: " + errors);
        }
        Map<String, byte[]> entries = new java.util.TreeMap<>();
        try (Stream<Path> files = Files.walk(classes)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                entries.put(
                        classes.relativize(file).toString().replace('\\', '/'), Files.readAllBytes(file));
            }
        }
        return writeJar(jar, entries);
    }

    /**
     * Writes a jar that carries Maven coordinates like a jar copied by the dependency plugin.
     *
     * @param dir the directory to write the jar to
     * @param groupId the Maven group id
     * @param artifactId the Maven artifact id
     * @param version the Maven version
     * @return the written jar
     * @throws IOException if the jar cannot be written
     */
    public static Path artifactJar(Path dir, String groupId, String artifactId, String version)
            throws IOException {
        String properties =
                "groupId=" + groupId + "\nartifactId=" + artifactId + "\nversion=" + version + "\n";
        return writeJar(
                dir.resolve(artifactId + "-" + version + ".jar"),
                Map.of(
                        "META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties",
                        properties.getBytes(StandardCharsets.UTF_8)));
    }

    public static Path writeJar(Path jar, Map<String, byte[]> entries) throws IOException {
        Files.createDirectories(jar.getParent());
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                out.putNextEntry(new JarEntry(entry.getKey()));
                out.write(entry.getValue());
                out.closeEntry();
            }
        }
        return jar;
    }

    /**
     * Writes a {@code plugin.yaml} to {@code dir} declaring {@code drivers} and the default
     * classpath.
     *
     * @param dir the bundle directory
     * @param id the plugin id
     * @param drivers the exported driver classes
     * @param extraYaml additional descriptor content
     * @return {@code dir}
     * @throws IOException if the descriptor cannot be written
     */
    public static Path descriptor(Path dir, String id, List<String> drivers, String extraYaml)
            throws IOException {
        Files.createDirectories(dir);
        String yaml =
                "descriptorVersion: 1\n"
                        + "id: "
                        + id
                        + "\n"
                        + "version: 1.0\n"
                        + "driverApiVersion: 1\n"
                        + "drivers:\n"
                        + drivers.stream().map(d -> "  - " + d + "\n").collect(Collectors.joining())
                        + extraYaml;
        if (!extraYaml.contains("classpath:")) {
            yaml += "classpath:\n  - \"*.jar\"\n  - \"lib/*.jar\"\n";
        }
        Files.writeString(dir.resolve(PluginBundle.DESCRIPTOR_FILE), yaml);
        return dir;
    }

    /**
     * Source of a driver whose topic name prefix is computed by {@code prefixExpression}.
     *
     * @param className the fully qualified driver class name
     * @param prefixExpression Java expression returning the topic name prefix
     * @param initializeBody statements run by {@code initialize}
     * @return the driver source code
     */
    public static String driverSource(
            String className, String prefixExpression, String initializeBody) {
        return driverSource(className, prefixExpression, initializeBody, "");
    }

    /**
     * Source of a driver with custom initialization and close behavior.
     *
     * @param className the fully qualified driver class name
     * @param prefixExpression Java expression returning the topic name prefix
     * @param initializeBody statements run by {@code initialize}
     * @param closeBody statements run by {@code close}
     * @return the driver source code
     */
    public static String driverSource(
            String className, String prefixExpression, String initializeBody, String closeBody) {
        int dot = className.lastIndexOf('.');
        return "package "
                + className.substring(0, dot)
                + ";\n"
                + "import io.openmessaging.benchmark.driver.*;\n"
                + "import java.util.Optional;\n"
                + "import java.util.concurrent.CompletableFuture;\n"
                + "public class "
                + className.substring(dot + 1)
                + " implements BenchmarkDriver {\n"
                + "  public void initialize(java.nio.file.Path config,"
                + " io.micrometer.prometheusmetrics.PrometheusMeterRegistry registry) throws java.io.IOException {\n"
                + initializeBody
                + "\n  }\n"
                + "  public String getTopicNamePrefix() { return "
                + prefixExpression
                + "; }\n"
                + "  public CompletableFuture<Void> createTopic(String topic, int partitions) {\n"
                + "    if (topic.contains(\"-0000000-\") && getTopicNamePrefix().startsWith(\"crash\")) {"
                + " Runtime.getRuntime().halt(42); }\n"
                + "    return CompletableFuture.completedFuture(null);\n"
                + "  }\n"
                + "  public CompletableFuture<BenchmarkProducer> createProducer(String topic) {\n"
                + "    return CompletableFuture.failedFuture(new UnsupportedOperationException());\n"
                + "  }\n"
                + "  public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String s,"
                + " ConsumerCallback c) {\n"
                + "    return CompletableFuture.failedFuture(new UnsupportedOperationException());\n"
                + "  }\n"
                + "  public void close() { "
                + closeBody
                + " }\n"
                + "}\n";
    }

    private static List<Path> driverApiClasspath() {
        return Stream.of(BenchmarkDriver.class, PrometheusMeterRegistry.class, MeterRegistry.class)
                .map(PluginFixtures::codeSource)
                .distinct()
                .toList();
    }

    private static Path codeSource(Class<?> type) {
        try {
            return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private record TemporaryDirectory(Path path) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            try (Stream<Path> files = Files.walk(path)) {
                for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(file);
                }
            }
        }
    }

    private static final class WriterOutputStream extends OutputStream {
        private final StringWriter writer;

        WriterOutputStream(StringWriter writer) {
            this.writer = writer;
        }

        @Override
        public void write(int b) {
            writer.write(b);
        }
    }
}
