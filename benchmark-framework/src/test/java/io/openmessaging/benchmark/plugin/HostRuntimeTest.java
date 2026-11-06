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

import static org.assertj.core.api.Assertions.assertThat;

import io.openmessaging.benchmark.driver.BenchmarkDriver;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HostRuntimeTest {
    @TempDir Path dir;

    @Test
    void currentRuntimeIncludesTheFrameworkAndDriverApi() {
        HostRuntime runtime = HostRuntime.current();

        assertThat(runtime.entries())
                .extracting(entry -> entry.coordinates().artifactId())
                .contains("benchmark-framework", "driver-api");
        assertThat(runtime.entries())
                .anySatisfy(
                        entry ->
                                assertThat(entry.path())
                                        .isEqualTo(
                                                Path.of(
                                                        BenchmarkDriver.class
                                                                .getProtectionDomain()
                                                                .getCodeSource()
                                                                .getLocation()
                                                                .getPath())));
    }

    @Test
    void includesManifestClasspathReferences() throws IOException {
        Path dependency = Files.createFile(dir.resolve("dependency.jar"));
        Path runtimeJar = dir.resolve("runtime.jar");
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, "dependency.jar");
        try (JarOutputStream output =
                new JarOutputStream(Files.newOutputStream(runtimeJar), manifest)) {
            output.putNextEntry(new JarEntry("marker"));
            output.write(1);
            output.closeEntry();
        }

        String oldClasspath = System.getProperty("java.class.path");
        try {
            System.setProperty("java.class.path", runtimeJar.toString());

            assertThat(HostRuntime.jvmClasspath()).containsExactly(runtimeJar, dependency);
        } finally {
            if (oldClasspath == null) {
                System.clearProperty("java.class.path");
            } else {
                System.setProperty("java.class.path", oldClasspath);
            }
        }
    }

    @Test
    void skipsBlankEntriesDirectoriesAndJarsWithoutUsableManifests() throws IOException {
        Path classes = Files.createDirectories(dir.resolve("classes"));
        Path textFile = Files.writeString(dir.resolve("notes.txt"), "text");
        Path noManifest =
                PluginFixtures.writeJar(dir.resolve("plain.jar"), Map.of("a", new byte[] {1}));
        Path noClassPath = jarWithManifest("no-classpath.jar", null);
        Path remoteRefs = jarWithManifest("remote.jar", "http://example.com/remote.jar local.jar");
        Path broken = Files.writeString(dir.resolve("broken.jar"), "not a zip");
        String sep = File.pathSeparator;

        List<Path> classpath =
                withClasspath(
                        String.join(
                                sep,
                                "",
                                " ",
                                classes.toString(),
                                textFile.toString(),
                                noManifest.toString(),
                                noClassPath.toString(),
                                remoteRefs.toString(),
                                broken.toString()),
                        HostRuntime::jvmClasspath);

        assertThat(classpath)
                .containsExactly(
                        classes,
                        textFile,
                        noManifest,
                        noClassPath,
                        remoteRefs,
                        dir.resolve("local.jar"),
                        broken);
    }

    @Test
    void locatesArtifactsMissingFromTheClasspathThroughTheClassLoader() {
        HostRuntime.Entry hdr = hdrHistogram(HostRuntime.current());

        HostRuntime runtime =
                withClasspath(classpathWithout(hdr.path(), List.of()), HostRuntime::current);

        assertThat(runtime.entries()).contains(hdr);
    }

    @Test
    void locatesArtifactsByGroupQualifiedFileName() throws IOException {
        HostRuntime.Entry hdr = hdrHistogram(HostRuntime.current());
        ArtifactCoordinates c = hdr.coordinates();
        Path copy =
                Files.copy(
                        hdr.path(),
                        dir.resolve(c.groupId() + "-" + c.artifactId() + "-" + c.version() + ".jar"));

        HostRuntime runtime =
                withClasspath(classpathWithout(hdr.path(), List.of(copy)), HostRuntime::current);

        assertThat(runtime.entries()).contains(new HostRuntime.Entry(copy, c));
    }

    private static HostRuntime.Entry hdrHistogram(HostRuntime runtime) {
        return runtime.entries().stream()
                .filter(entry -> entry.coordinates().artifactId().equals("HdrHistogram"))
                .findFirst()
                .orElseThrow();
    }

    private static String classpathWithout(Path excluded, List<Path> extra) {
        List<String> entries = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            if (!entry.isBlank() && !Path.of(entry).toAbsolutePath().normalize().equals(excluded)) {
                entries.add(entry);
            }
        }
        extra.forEach(path -> entries.add(path.toString()));
        return String.join(File.pathSeparator, entries);
    }

    private Path jarWithManifest(String name, String classPath) throws IOException {
        Path jar = dir.resolve(name);
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (classPath != null) {
            manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, classPath);
        }
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
            output.putNextEntry(new JarEntry("marker"));
            output.write(1);
            output.closeEntry();
        }
        return jar;
    }

    private static <T> T withClasspath(String classpath, Supplier<T> action) {
        String oldClasspath = System.getProperty("java.class.path");
        try {
            System.setProperty("java.class.path", classpath);
            return action.get();
        } finally {
            if (oldClasspath == null) {
                System.clearProperty("java.class.path");
            } else {
                System.setProperty("java.class.path", oldClasspath);
            }
        }
    }
}
