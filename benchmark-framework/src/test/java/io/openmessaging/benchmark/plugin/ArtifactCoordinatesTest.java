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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArtifactCoordinatesTest {
    @TempDir Path dir;

    @Test
    void formatsCoordinatesWithAndWithoutClassifier() {
        ArtifactCoordinates main = new ArtifactCoordinates("g", "a", "1");
        ArtifactCoordinates linux = new ArtifactCoordinates("g", "a", "1", "linux");

        assertThat(main).hasToString("g:a:1");
        assertThat(main.variantKey()).isEqualTo("g:a");
        assertThat(linux).hasToString("g:a:1:linux");
        assertThat(linux.variantKey()).isEqualTo("g:a:linux");
        assertThat(linux.key()).isEqualTo("g:a");
    }

    @Test
    void ignoresDirectoriesAndNonJarFiles() throws IOException {
        Path notAJar = Files.writeString(dir.resolve("a-1.txt"), "text");

        assertThat(ArtifactCoordinates.detect(dir)).isEmpty();
        assertThat(ArtifactCoordinates.detect(notAJar)).isEmpty();
        assertThat(ArtifactCoordinates.detect(dir.resolve("missing.jar"))).isEmpty();
    }

    @Test
    void detectsCoordinatesFromJarFileNames() throws IOException {
        assertThat(detect("g-a-1.jar")).contains(new ArtifactCoordinates("g", "a", "1"));
        assertThat(detect("a-1.jar")).contains(new ArtifactCoordinates("g", "a", "1"));
        assertThat(detect("a-1-linux.jar")).contains(new ArtifactCoordinates("g", "a", "1", "linux"));
        assertThat(detect("g-a-1-osx.jar")).contains(new ArtifactCoordinates("g", "a", "1", "osx"));
        assertThat(detect("a-1-.jar")).isEmpty();
        assertThat(detect("shaded.jar")).isEmpty();
    }

    @Test
    void ignoresIncompleteAndUnrelatedJarEntries() throws IOException {
        Path jar =
                PluginFixtures.writeJar(
                        dir.resolve("a-1.jar"),
                        Map.of(
                                "META-INF/maven/g/a/pom.properties",
                                "groupId=g\nartifactId=a\n".getBytes(StandardCharsets.UTF_8),
                                "META-INF/maven/g/b/pom.properties",
                                "groupId=g\nversion=1\n".getBytes(StandardCharsets.UTF_8),
                                "META-INF/maven/g/c/pom.properties",
                                "artifactId=a\nversion=1\n".getBytes(StandardCharsets.UTF_8),
                                "META-INF/maven/g/a/pom.xml",
                                new byte[0],
                                "io/example/Driver.class",
                                new byte[0]));

        assertThat(ArtifactCoordinates.detect(jar)).isEmpty();
    }

    @Test
    void rejectsUnreadableJars() throws IOException {
        Path jar = Files.writeString(dir.resolve("broken-1.jar"), "not a zip");

        assertThatThrownBy(() -> ArtifactCoordinates.detect(jar))
                .isInstanceOf(PluginException.class)
                .hasMessageContaining("Cannot inspect plugin jar");
    }

    private Optional<ArtifactCoordinates> detect(String fileName) throws IOException {
        Path jar =
                PluginFixtures.writeJar(
                        dir.resolve(fileName),
                        Map.of(
                                "META-INF/maven/g/a/pom.properties",
                                " groupId=g\nartifactId=a\nversion=1\n".getBytes(StandardCharsets.UTF_8)));
        return ArtifactCoordinates.detect(jar);
    }
}
