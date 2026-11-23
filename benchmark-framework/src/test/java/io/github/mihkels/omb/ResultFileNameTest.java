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
package io.github.mihkels.omb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResultFileNameTest {

    @Test
    void explicitOutputWinsOverResultsDirectory() {
        assertThat(Benchmark.resultFileName("out.json", "workload", "Kafka", "/results", null))
                .isEqualTo("out.json");
    }

    @Test
    void generatedNameIsRelativeWithoutResultsDirectory() {
        assertThat(Benchmark.resultFileName(null, "workload", "Kafka", null, null))
                .matches("workload-Kafka-\\d{4}(-\\d{2}){5}\\.json");
        assertThat(Benchmark.resultFileName("", "workload", "Kafka", "", null))
                .matches("workload-Kafka-\\d{4}(-\\d{2}){5}\\.json");
    }

    @Test
    void generatedNameIsPlacedInResultsDirectory() {
        Path file = Path.of(Benchmark.resultFileName(null, "workload", "Kafka", "/results", null));

        assertThat(file).hasParentRaw(Path.of("/results"));
        assertThat(file.getFileName().toString()).startsWith("workload-Kafka-").endsWith(".json");
    }

    @Test
    void driverNameCannotLeaveTheResultsDirectory() {
        Path file = Path.of(Benchmark.resultFileName(null, "w", "../Kafka/KRaft", "/results", null));

        assertThat(file).hasParentRaw(Path.of("/results"));
        assertThat(file.getFileName().toString()).startsWith("w-.._Kafka_KRaft-");
        assertThat(Benchmark.resultFileName(null, "w", "Nats Streaming", null, null))
                .startsWith("w-Nats Streaming-");
    }

    @Test
    void suffixGoesBeforeTheExtension() {
        assertThat(Benchmark.resultFileName("out/result.json", "w", "Kafka", null, "2"))
                .isEqualTo("out/result-2.json");
        assertThat(Benchmark.resultFileName("result", "w", "Kafka", null, "summary"))
                .isEqualTo("result-summary");
        assertThat(Benchmark.resultFileName("out.d/result", "w", "Kafka", null, "1"))
                .isEqualTo("out.d/result-1");
        assertThat(Benchmark.resultFileName(null, "workload", "Kafka", null, "3"))
                .matches("workload-Kafka-\\d{4}(-\\d{2}){5}-3\\.json");
    }

    @Test
    void createsMissingResultDirectories(@TempDir Path temp) throws IOException {
        Path output = temp.resolve("a/b/run.json");
        Benchmark.createResultDirectory(output.toString(), "/ignored-when-output-is-set");
        assertThat(output.getParent()).isDirectory();

        Path results = temp.resolve("results/nested");
        Benchmark.createResultDirectory(null, results.toString());
        assertThat(results).isDirectory();
    }

    @Test
    void rejectsResultDirectoryThatIsAFile(@TempDir Path temp) throws IOException {
        Path file = Files.createFile(temp.resolve("taken"));

        assertThatThrownBy(
                        () -> Benchmark.createResultDirectory(file.resolve("run.json").toString(), null))
                .isInstanceOf(IOException.class);
    }

    @Test
    void emptyOutputFallsBackToResultsDirectory(@TempDir Path temp) throws IOException {
        Path results = temp.resolve("results");
        Benchmark.createResultDirectory("", results.toString());
        assertThat(results).isDirectory();

        // Nothing to create: the generated name is relative to the working directory
        Benchmark.createResultDirectory("", "");
    }

    @Test
    void rejectsReadOnlyResultDirectory(@TempDir Path temp) throws IOException {
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        Path readOnly = Files.createDirectory(temp.resolve("read-only"));
        Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("r-xr-xr-x"));
        // Root can write anyway
        assumeTrue(!Files.isWritable(readOnly));

        assertThatThrownBy(() -> Benchmark.createResultDirectory(null, readOnly.toString()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not writable");
    }

    @Test
    void createsAndValidatesReportDirectories(@TempDir Path temp) throws IOException {
        Benchmark.createReportDirectory(null);
        Benchmark.createReportDirectory("");

        Path report = temp.resolve("reports/nested/report.html");
        Benchmark.createReportDirectory(report.toString());
        assertThat(report.getParent()).isDirectory();

        Path existingDir = Files.createDirectory(temp.resolve("existing-dir"));
        assertThatThrownBy(() -> Benchmark.createReportDirectory(existingDir.toString()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("is a directory");

        Path blocker = Files.createFile(temp.resolve("blocker"));
        assertThatThrownBy(() -> Benchmark.createReportDirectory(blocker.resolve("r.html").toString()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void rejectsReadOnlyReportDirectory(@TempDir Path temp) throws IOException {
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        Path readOnly = Files.createDirectory(temp.resolve("read-only-reports"));
        Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("r-xr-xr-x"));
        assumeTrue(!Files.isWritable(readOnly));

        assertThatThrownBy(
                        () -> Benchmark.createReportDirectory(readOnly.resolve("report.html").toString()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not writable");
    }

    @Test
    void rejectsExistingReadOnlyReportFile(@TempDir Path temp) throws IOException {
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        Path report = Files.createFile(temp.resolve("report.html"));
        Files.setPosixFilePermissions(report, PosixFilePermissions.fromString("r--r--r--"));
        assumeTrue(!Files.isWritable(report));

        assertThatThrownBy(() -> Benchmark.createReportDirectory(report.toString()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Report file")
                .hasMessageContaining("not writable");
    }
}
