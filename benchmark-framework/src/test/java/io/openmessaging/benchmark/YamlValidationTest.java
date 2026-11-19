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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Loads workload and driver files through the schema checks the CLI applies. */
class YamlValidationTest {
    private static final Path ROOT = Path.of("..");

    @TempDir Path directory;

    static Stream<Path> bundledWorkloads() throws IOException {
        return yamlFiles("workloads", "local-test/workloads", "e2e-tests/compose/workloads");
    }

    static Stream<Path> bundledDrivers() throws IOException {
        List<String> directories;
        try (Stream<Path> modules = Files.list(ROOT)) {
            directories =
                    modules
                            .filter(path -> path.getFileName().toString().startsWith("driver-"))
                            .map(path -> path.getFileName().toString())
                            .toList();
        }
        return Stream.concat(
                yamlFiles(directories.toArray(String[]::new)),
                yamlFiles("local-test/drivers", "e2e-tests/compose/drivers"));
    }

    @ParameterizedTest
    @MethodSource("bundledWorkloads")
    void bundledWorkloadPassesTheSchemaAndValidation(Path workload) {
        assertThat(Benchmark.readWorkloads(List.of(workload.toString()))).hasSize(1);
    }

    @ParameterizedTest
    @MethodSource("bundledDrivers")
    void bundledDriverConfigurationPassesTheSchema(Path driver) {
        assertThat(Benchmark.readDriverConfig(driver.toString())).containsKeys("name", "driverClass");
    }

    @Test
    void bundledFilesAreFound() throws IOException {
        assertThat(bundledWorkloads()).hasSizeGreaterThan(100);
        assertThat(bundledDrivers()).hasSizeGreaterThan(30);
    }

    @Test
    void unknownWorkloadKeyIsRejected() throws IOException {
        assertRejected(
                WorkloadYaml.minimal("typo") + "producerRat: 1000\n",
                "property 'producerRat' is not defined in the schema");
    }

    @Test
    void unknownNestedWorkloadKeyIsRejected() throws IOException {
        assertRejected(
                WorkloadYaml.minimal("nested") + "thresholds:\n  maxLatency: 5\n",
                "property 'maxLatency' is not defined in the schema");
    }

    @Test
    void missingCountIsRejected() throws IOException {
        assertRejected(
                WorkloadYaml.minimal("missing").replace("topics: 1\n", ""),
                "required property 'topics' not found");
    }

    @Test
    void zeroCountIsRejected() throws IOException {
        assertRejected(
                WorkloadYaml.minimal("zero").replace("producersPerTopic: 1", "producersPerTopic: 0"),
                "must have a minimum value of 1");
    }

    @Test
    void fixedRateWorkloadWithoutADurationIsRejected() throws IOException {
        assertRejected(
                WorkloadYaml.minimal("endless").replace("testDurationMinutes: 1\n", ""),
                "required property 'testDurationMinutes' not found");
    }

    @Test
    void stagedWorkloadNeedsNoRateOrDuration() throws IOException {
        Path file =
                write(
                        "staged",
                        "name: staged\n"
                                + WorkloadYaml.COUNTS
                                + "messageSize: 1024\nstages:\n  - durationMinutes: 1\n    targetRate: 100\n");

        List<String> paths = List.of(file.toString());

        assertThatNoException().isThrownBy(() -> Benchmark.readWorkloads(paths));
    }

    @Test
    void sizeDistributionWorkloadNeedsNoMessageSize() throws IOException {
        Path file =
                write(
                        "distribution",
                        "name: distribution\n"
                                + WorkloadYaml.COUNTS
                                + "producerRate: 100\ntestDurationMinutes: 1\n"
                                + "messageSizeDistribution:\n  \"256-1024\": 1\n");

        List<String> paths = List.of(file.toString());

        assertThatNoException().isThrownBy(() -> Benchmark.readWorkloads(paths));
    }

    @Test
    void driverConfigurationWithoutADriverClassIsRejected() throws IOException {
        String file = write("driver", "name: fixture\nbrokerUrl: localhost\n").toString();

        assertThatThrownBy(() -> Benchmark.readDriverConfig(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Invalid driver configuration " + file + ": required property 'driverClass' not found");
    }

    private void assertRejected(String yaml, String violation) throws IOException {
        Path file = write("workload", yaml);
        List<String> paths = List.of(file.toString());

        assertThatThrownBy(() -> Benchmark.readWorkloads(paths))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid workload " + file + ": ")
                .hasMessageContaining(violation);
    }

    private Path write(String name, String yaml) throws IOException {
        return Files.writeString(directory.resolve(name + ".yaml"), yaml);
    }

    private static Stream<Path> yamlFiles(String... directories) throws IOException {
        Stream.Builder<Path> files = Stream.builder();
        for (String directory : directories) {
            try (Stream<Path> listing = Files.list(ROOT.resolve(directory))) {
                listing.filter(path -> path.toString().endsWith(".yaml")).sorted().forEach(files);
            }
        }
        return files.build();
    }
}
