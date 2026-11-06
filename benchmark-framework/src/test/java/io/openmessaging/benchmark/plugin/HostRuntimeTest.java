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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
}
