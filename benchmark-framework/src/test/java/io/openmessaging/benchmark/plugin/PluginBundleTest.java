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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.openmessaging.benchmark.driver.DriverApiVersion;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PluginBundleTest {
    @TempDir Path dir;

    @Test
    void rejectsDirectoriesWithoutDescriptor() {
        assertThatThrownBy(() -> PluginBundle.load(dir))
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("has no " + PluginBundle.DESCRIPTOR_FILE);
    }

    @Test
    void rejectsEmptyDescriptors() throws IOException {
        Files.writeString(dir.resolve(PluginBundle.DESCRIPTOR_FILE), "");

        assertThatThrownBy(() -> PluginBundle.load(dir))
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("Invalid plugin descriptor");
    }

    @Test
    void rejectsClasspathsWhereNoPatternMatches() throws IOException {
        PluginFixtures.descriptor(
                dir, "empty", List.of("test.Driver"), "classpath:\n  - \"lib/*.jar\"\n  - \"x?.jar\"\n");

        assertThatThrownBy(() -> PluginBundle.load(dir))
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("classpath does not match any file");
    }

    @Test
    void matchesSingleCharacterWildcardsAndDeduplicatesEntries() throws IOException {
        Path a = PluginFixtures.writeJar(dir.resolve("a.jar"), Map.of("a", new byte[0]));
        Path b = PluginFixtures.writeJar(dir.resolve("b.jar"), Map.of("b", new byte[0]));
        Files.writeString(dir.resolve("c.txt"), "not a jar");
        Files.createDirectories(dir.resolve("d.jar"));

        PluginBundle bundle =
                bundle(List.of("a.jar", "?.jar", "*.jar"), List.of("a.jar", "missing/*.jar"));

        assertThat(bundle.classpath()).containsExactly(a, b);
    }

    @Test
    void rejectsEntriesThatEscapeTheBundleDirectory() {
        PluginBundle bundle = bundle(List.of("../outside.jar"), List.of());

        assertThatThrownBy(bundle::classpath)
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("escapes");
    }

    private PluginBundle bundle(List<String> classpath, List<String> optionalClasspath) {
        return new PluginBundle(
                dir,
                new PluginDescriptor(
                        1,
                        "test",
                        null,
                        DriverApiVersion.CURRENT,
                        null,
                        List.of("test.Driver"),
                        classpath,
                        optionalClasspath,
                        List.of(),
                        null));
    }
}
