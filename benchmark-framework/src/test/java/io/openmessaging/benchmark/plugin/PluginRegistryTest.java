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

import static io.openmessaging.benchmark.plugin.PluginFixtures.artifactJar;
import static io.openmessaging.benchmark.plugin.PluginFixtures.compileJar;
import static io.openmessaging.benchmark.plugin.PluginFixtures.descriptor;
import static io.openmessaging.benchmark.plugin.PluginFixtures.driverSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.DriverApiVersion;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PluginRegistryTest {

    private static final String KAFKA = "test.kafka.KafkaDriver";
    private static final String JMS = "test.jms.JmsDriver";

    @TempDir Path dir;

    private Path bundle(String id, List<String> drivers, String extraYaml) throws IOException {
        Path bundle = descriptor(dir.resolve("drivers").resolve(id), id, drivers, extraYaml);
        PluginFixtures.writeJar(bundle.resolve(id + ".jar"), Map.of("marker-" + id, new byte[0]));
        return bundle;
    }

    private PluginRegistry discover() {
        return PluginRegistry.discover(List.of(dir.resolve("drivers")));
    }

    @Test
    void resolvesDriverToItsBundleClasspath() throws IOException {
        Path kafka = bundle("kafka", List.of(KAFKA), "");
        Path lib = artifactJar(kafka.resolve("lib"), "org.example", "client", "1.0");
        bundle("other", List.of("test.other.OtherDriver"), "");

        ResolvedDriver resolved = discover().resolve(KAFKA, Map.of());

        assertThat(resolved.bundles()).extracting(PluginBundle::id).containsExactly("kafka");
        assertThat(real(resolved.primary().classpath()))
                .containsExactly(kafka.resolve("kafka.jar").toRealPath(), lib.toRealPath());
    }

    @Test
    void versionOneDescriptorStillAcceptsBinaryIncompatibleOlderDriver() throws Exception {
        String oldDriver = "fixture.legacy.LegacyDriver";
        String oldApi =
                "package io.openmessaging.benchmark.driver;\n"
                        + "public interface BenchmarkDriver extends AutoCloseable {\n"
                        + "  void initialize(java.nio.file.Path config,"
                        + " io.micrometer.prometheusmetrics.PrometheusMeterRegistry registry)"
                        + " throws java.io.IOException, InterruptedException;\n"
                        + "}\n";
        String oldDriverSource =
                driverSource(oldDriver, "\"legacy\"", "")
                        .replace(
                                "io.micrometer.core.instrument.MeterRegistry registry",
                                "io.micrometer.prometheusmetrics.PrometheusMeterRegistry registry");
        Path legacyBundle = descriptor(dir.resolve("drivers/legacy"), "legacy", List.of(oldDriver), "");
        Path jar = legacyBundle.resolve("legacy.jar");
        URL prometheusClasspath =
                PrometheusMeterRegistry.class.getProtectionDomain().getCodeSource().getLocation();
        compileJar(
                jar,
                Map.of(
                        "io.openmessaging.benchmark.driver.BenchmarkDriver",
                        oldApi,
                        oldDriver,
                        oldDriverSource),
                List.of(Path.of(prometheusClasspath.toURI())));
        String entry = "fixture/legacy/LegacyDriver.class";
        byte[] legacyClass;
        try (JarFile compiled = new JarFile(jar.toFile())) {
            legacyClass = compiled.getInputStream(compiled.getJarEntry(entry)).readAllBytes();
        }
        PluginFixtures.writeJar(jar, Map.of(entry, legacyClass));

        assertThat(DriverApiVersion.CURRENT).isEqualTo(1);
        assertThat(discover().resolve(oldDriver, Map.of()).primary().id()).isEqualTo("legacy");
        try (URLClassLoader loader =
                new URLClassLoader(
                        new URL[] {jar.toUri().toURL()}, BenchmarkDriver.class.getClassLoader())) {
            BenchmarkDriver legacy =
                    (BenchmarkDriver) loader.loadClass(oldDriver).getDeclaredConstructor().newInstance();
            assertThatThrownBy(() -> legacy.initialize(Path.of("unused"), null))
                    .isInstanceOf(AbstractMethodError.class);
        }
    }

    @Test
    void followsClassReferencePropertiesToOtherBundles() throws IOException {
        bundle("kafka", List.of(KAFKA), "");
        bundle(
                "jms",
                List.of(JMS),
                "optionalClasspath:\n  - \"ext/*.jar\"\n"
                        + "classReferenceProperties:\n  - delegateForAdminOperationsClassName\n");
        PluginRegistry registry = discover();

        assertThat(registry.resolve(JMS, Map.of()).bundles())
                .extracting(PluginBundle::id)
                .containsExactly("jms");
        assertThat(
                        registry.resolve(JMS, Map.of("delegateForAdminOperationsClassName", KAFKA)).bundles())
                .extracting(PluginBundle::id)
                .containsExactly("jms", "kafka");
        assertThatThrownBy(
                        () -> registry.resolve(JMS, Map.of("delegateForAdminOperationsClassName", "x.Missing")))
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("delegateForAdminOperationsClassName")
                .hasMessageContaining("x.Missing");
    }

    @Test
    void reportsUnknownDriverClass() throws IOException {
        bundle("kafka", List.of(KAFKA), "");

        assertThatThrownBy(() -> discover().resolve("x.Unknown", Map.of()))
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("x.Unknown")
                .hasMessageContaining(KAFKA);
    }

    @Test
    void collectsErrorsOfAllInvalidBundles() throws IOException {
        bundle("kafka", List.of(KAFKA), "");
        Path badVersion = bundle("bad-version", List.of("a.A"), "");
        Files.writeString(
                badVersion.resolve(PluginBundle.DESCRIPTOR_FILE),
                Files.readString(badVersion.resolve(PluginBundle.DESCRIPTOR_FILE))
                        .replace("descriptorVersion: 1", "descriptorVersion: 2")
                        .replace("driverApiVersion: 1", "driverApiVersion: 99"));
        bundle("escape", List.of("b.B"), "classpath:\n  - \"../kafka/kafka.jar\"\n");
        bundle("missing", List.of("c.C"), "classpath:\n  - absent.jar\n");
        bundle("unknown-field", List.of("d.D"), "surprise: true\n");
        bundle("kafka-copy", List.of(KAFKA), "");

        PluginRegistry registry = discover();

        assertThat(registry.errors())
                .anySatisfy(e -> assertThat(e).contains("unsupported descriptorVersion 2"))
                .anySatisfy(e -> assertThat(e).contains("driverApiVersion 99"))
                .anySatisfy(e -> assertThat(e).contains("escapes the bundle directory"))
                .anySatisfy(e -> assertThat(e).contains("'absent.jar' does not match any file"))
                .anySatisfy(e -> assertThat(e).contains("surprise"))
                .anySatisfy(e -> assertThat(e).contains(KAFKA).contains("kafka-copy"));
        assertThatThrownBy(() -> registry.resolve(KAFKA, Map.of()))
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("Invalid driver plugin bundles");
    }

    @Test
    void rejectsDuplicateIds() throws IOException {
        bundle("kafka", List.of(KAFKA), "");
        descriptor(dir.resolve("drivers").resolve("kafka2"), "kafka", List.of("e.E"), "");
        PluginFixtures.writeJar(dir.resolve("drivers/kafka2/x.jar"), Map.of("x", new byte[0]));

        assertThat(discover().errors())
                .anySatisfy(e -> assertThat(e).contains("Duplicate plugin id 'kafka'"));
    }

    @Test
    void unrelatedInvalidBundleDoesNotBlockAValidDriver() throws IOException {
        bundle("kafka", List.of(KAFKA), "");
        Path invalid = bundle("invalid", List.of("test.invalid.InvalidDriver"), "");
        Path descriptor = invalid.resolve(PluginBundle.DESCRIPTOR_FILE);
        Files.writeString(
                descriptor,
                Files.readString(descriptor).replace("driverApiVersion: 1", "driverApiVersion: 99"));
        PluginRegistry registry = discover();

        assertThat(registry.errors())
                .anySatisfy(error -> assertThat(error).contains("driverApiVersion 99"));
        assertThat(registry.resolve(KAFKA, Map.of()).primary().id()).isEqualTo("kafka");
        assertThatThrownBy(() -> registry.resolve("test.invalid.InvalidDriver", Map.of()))
                .hasMessageContaining("test.invalid.InvalidDriver")
                .hasMessageContaining("driverApiVersion 99");
    }

    @Test
    void malformedClasspathEntriesAreReportedWithoutBlockingOtherDrivers() throws IOException {
        bundle("kafka", List.of(KAFKA), "");
        bundle("invalid-glob", List.of("test.invalid.GlobDriver"), "classpath:\n  - \"bad[*\"\n");
        bundle("invalid-path", List.of("test.invalid.PathDriver"), "classpath:\n  - \"bad\\0path\"\n");
        Path invalidNull = bundle("invalid-null", List.of("test.invalid.NullDriver"), "");
        Path descriptor = invalidNull.resolve(PluginBundle.DESCRIPTOR_FILE);
        Files.writeString(
                descriptor,
                Files.readString(descriptor).replace("  - test.invalid.NullDriver", "  - null"));

        PluginRegistry registry = discover();

        assertThat(registry.resolve(KAFKA, Map.of()).primary().id()).isEqualTo("kafka");
        assertThat(registry.errors())
                .anySatisfy(error -> assertThat(error).contains("invalid-glob"))
                .anySatisfy(error -> assertThat(error).contains("invalid-path"))
                .anySatisfy(error -> assertThat(error).contains("invalid-null"));
    }

    @Test
    void unrelatedDuplicateDriversDoNotBlockAValidDriver() throws IOException {
        bundle("kafka", List.of(KAFKA), "");
        bundle("other", List.of("test.other.OtherDriver"), "");
        bundle("other-copy", List.of("test.other.OtherDriver"), "");
        PluginRegistry registry = discover();

        assertThat(registry.resolve(KAFKA, Map.of()).primary().id()).isEqualTo("kafka");
        assertThatThrownBy(() -> registry.resolve("test.other.OtherDriver", Map.of()))
                .hasMessageContaining("test.other.OtherDriver")
                .hasMessageContaining("other-copy");
    }

    @Test
    void rejectsBothDriversOfBundlesWithTheSameId() throws IOException {
        bundle("kafka", List.of(KAFKA), "");
        descriptor(
                dir.resolve("drivers").resolve("duplicate"), "kafka", List.of("test.other.Driver"), "");
        PluginFixtures.writeJar(dir.resolve("drivers/duplicate/x.jar"), Map.of("x", new byte[0]));
        PluginRegistry registry = discover();

        assertThatThrownBy(() -> registry.resolve(KAFKA, Map.of()))
                .hasMessageContaining("Duplicate plugin id 'kafka'");
        assertThatThrownBy(() -> registry.resolve("test.other.Driver", Map.of()))
                .hasMessageContaining("Duplicate plugin id 'kafka'");
    }

    @Test
    void rejectsBundledDriverApiArtifactsWithAnotherVersion() throws IOException {
        Path kafka = bundle("kafka", List.of(KAFKA), "");
        artifactJar(kafka.resolve("lib"), "io.micrometer", "micrometer-core", "0.1");
        artifactJar(kafka.resolve("lib"), "org.example", "shared", "0.1");
        HostRuntime host =
                new HostRuntime(
                        List.of(
                                hostEntry("io.micrometer", "micrometer-core", "1.0"),
                                hostEntry("org.example", "shared", "1.0")));
        ResolvedDriver resolved = discover().resolve(KAFKA, Map.of());

        assertThatThrownBy(() -> RuntimeClasspath.compose(host, resolved))
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("io.micrometer:micrometer-core:0.1")
                .hasMessageContaining("provided scope");
    }

    @Test
    void prefersHostCopiesOfOtherSharedArtifacts() throws IOException {
        Path kafka = bundle("kafka", List.of(KAFKA), "");
        Path bundled = artifactJar(kafka.resolve("lib"), "org.example", "shared", "0.1");
        Path same = artifactJar(kafka.resolve("lib"), "org.example", "same", "1.0");
        HostRuntime.Entry hostShared = hostEntry("org.example", "shared", "1.0");
        HostRuntime.Entry hostSame = hostEntry("org.example", "same", "1.0");

        RuntimeClasspath classpath =
                RuntimeClasspath.compose(
                        new HostRuntime(List.of(hostShared, hostSame)), discover().resolve(KAFKA, Map.of()));

        assertThat(real(classpath.entries()))
                .startsWith(hostShared.path().toRealPath(), hostSame.path().toRealPath())
                .contains(kafka.resolve("kafka.jar").toRealPath())
                .doesNotContain(bundled.toRealPath(), same.toRealPath());
        assertThat(classpath.warnings()).singleElement().asString().contains("org.example:shared:0.1");
    }

    @Test
    void rejectsConflictingVersionsBetweenBundlesOfOneProcess() throws IOException {
        Path kafka = bundle("kafka", List.of(KAFKA), "");
        Path jms =
                bundle(
                        "jms",
                        List.of(JMS),
                        "classReferenceProperties:\n  - delegateForAdminOperationsClassName\n");
        artifactJar(kafka.resolve("lib"), "org.example", "client", "1.0");
        artifactJar(jms.resolve("lib"), "org.example", "client", "2.0");
        ResolvedDriver resolved =
                discover().resolve(JMS, Map.of("delegateForAdminOperationsClassName", KAFKA));

        assertThatThrownBy(() -> RuntimeClasspath.compose(new HostRuntime(List.of()), resolved))
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("org.example:client:1.0")
                .hasMessageContaining("org.example:client:2.0");
    }

    @Test
    void usesHighestVersionOfAnnotationArtifactsSharedByBundlesOfOneProcess() throws IOException {
        Path kafka = bundle("kafka", List.of(KAFKA), "");
        Path jms =
                bundle(
                        "jms",
                        List.of(JMS),
                        "classReferenceProperties:\n  - delegateForAdminOperationsClassName\n");
        Path older =
                artifactJar(
                        jms.resolve("lib"), "com.google.errorprone", "error_prone_annotations", "2.5.1");
        Path newer =
                artifactJar(
                        kafka.resolve("lib"), "com.google.errorprone", "error_prone_annotations", "2.41.0");
        ResolvedDriver resolved =
                discover().resolve(JMS, Map.of("delegateForAdminOperationsClassName", KAFKA));

        RuntimeClasspath classpath = RuntimeClasspath.compose(new HostRuntime(List.of()), resolved);

        assertThat(real(classpath.entries()))
                .contains(newer.toRealPath())
                .doesNotContain(older.toRealPath());
        assertThat(classpath.warnings())
                .singleElement()
                .asString()
                .contains("error_prone_annotations:2.41.0");
    }

    @Test
    void comparesAnnotationVersionsWithMavenQualifierOrdering() {
        assertThat(RuntimeClasspath.compareVersions("2.0-rc1", "2.0")).isNegative();
        assertThat(RuntimeClasspath.compareVersions("2.0", "2.0-rc1")).isPositive();
        assertThat(RuntimeClasspath.compareVersions("2.10", "2.9")).isPositive();
        assertThat(RuntimeClasspath.compareVersions("2.0-final", "2.0")).isZero();
        assertThat(RuntimeClasspath.compareVersions("2.0-sp1", "2.0")).isPositive();
    }

    @Test
    void prefersAReleaseAnnotationArtifactOverItsPrerelease() throws IOException {
        Path kafka = bundle("kafka", List.of(KAFKA), "");
        Path jms =
                bundle(
                        "jms",
                        List.of(JMS),
                        "classReferenceProperties:\n  - delegateForAdminOperationsClassName\n");
        Path candidate =
                artifactJar(
                        jms.resolve("lib"), "com.google.errorprone", "error_prone_annotations", "2.0-rc1");
        Path release =
                artifactJar(
                        kafka.resolve("lib"), "com.google.errorprone", "error_prone_annotations", "2.0");
        ResolvedDriver resolved =
                discover().resolve(JMS, Map.of("delegateForAdminOperationsClassName", KAFKA));

        RuntimeClasspath classpath = RuntimeClasspath.compose(new HostRuntime(List.of()), resolved);

        assertThat(real(classpath.entries()))
                .contains(release.toRealPath())
                .doesNotContain(candidate.toRealPath());
    }

    @Test
    void reportsCorruptPluginJarsInsteadOfSilentlyUsingThem() throws IOException {
        Path kafka = bundle("kafka", List.of(KAFKA), "");
        Path jar = kafka.resolve("lib/broken.jar");
        Files.createDirectories(jar.getParent());
        Files.writeString(jar, "not a jar");

        assertThatThrownBy(
                        () ->
                                RuntimeClasspath.compose(
                                        new HostRuntime(List.of()), discover().resolve(KAFKA, Map.of())))
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("broken.jar")
                .hasMessageContaining("inspect");
    }

    @Test
    void retainsDistinctNativeClassifiersAndDeduplicatesMatchingVariants() throws IOException {
        Path kafka = bundle("kafka", List.of(KAFKA), "");
        Path jms =
                bundle(
                        "jms",
                        List.of(JMS),
                        "classReferenceProperties:\n  - delegateForAdminOperationsClassName\n");
        Path jmsLib = jms.resolve("lib");
        Path kafkaLib = kafka.resolve("lib");
        Path generic = artifactJar(jmsLib, "io.netty", "native", "1.0");
        Path arm = classifiedArtifact(jmsLib, "linux-aarch_64");
        Path x86 = classifiedArtifact(kafkaLib, "linux-x86_64");
        Path duplicate = classifiedArtifact(kafkaLib, "linux-aarch_64");

        ResolvedDriver resolved =
                discover().resolve(JMS, Map.of("delegateForAdminOperationsClassName", KAFKA));
        RuntimeClasspath classpath = RuntimeClasspath.compose(new HostRuntime(List.of()), resolved);

        assertThat(real(classpath.entries()))
                .contains(generic.toRealPath(), arm.toRealPath(), x86.toRealPath())
                .doesNotContain(duplicate.toRealPath());
    }

    @Test
    void hostUnclassifiedArtifactDoesNotReplaceClassifiedPluginArtifact() throws IOException {
        Path kafka = bundle("kafka", List.of(KAFKA), "");
        HostRuntime.Entry host = hostEntry("io.netty", "native", "1.0");
        Path nativeJar = classifiedArtifact(kafka.resolve("lib"), "linux-x86_64");

        RuntimeClasspath classpath =
                RuntimeClasspath.compose(
                        new HostRuntime(List.of(host)), discover().resolve(KAFKA, Map.of()));

        assertThat(real(classpath.entries()))
                .contains(host.path().toRealPath(), nativeJar.toRealPath());
    }

    @Test
    void resolvesUnpackagedDriverDirectoryOverride() throws IOException {
        Path module = dir.resolve("module with spaces");
        Files.createDirectories(module.resolve("target/classes"));
        Path lib = artifactJar(module.resolve("lib"), "org.example", "client", "1.0");

        ResolvedDriver resolved = discover().resolveOverride(KAFKA, Map.of(), module);

        assertThat(real(resolved.primary().classpath()))
                .contains(module.resolve("target/classes").toRealPath(), lib.toRealPath());
    }

    private static List<Path> real(List<Path> paths) {
        return paths.stream()
                .map(
                        p -> {
                            try {
                                return p.toRealPath();
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        })
                .toList();
    }

    private static Path classifiedArtifact(Path dir, String classifier) throws IOException {
        Path jar = artifactJar(dir.resolve("staging"), "io.netty", "native", "1.0");
        return Files.move(jar, dir.resolve("native-1.0-" + classifier + ".jar"));
    }

    private HostRuntime.Entry hostEntry(String groupId, String artifactId, String version)
            throws IOException {
        Path jar = artifactJar(dir.resolve("host"), groupId, artifactId, version);
        return new HostRuntime.Entry(jar, new ArtifactCoordinates(groupId, artifactId, version));
    }
}
