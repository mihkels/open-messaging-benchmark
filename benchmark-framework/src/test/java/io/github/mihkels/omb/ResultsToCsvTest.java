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

import java.io.File;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResultsToCsvTest {

    @TempDir Path tempDir;

    @Test
    void writesSortedResultsFromJsonFiles() throws Exception {
        Path results = Files.createDirectory(tempDir.resolve("results"));
        String golden = resource("golden/test-result.json");
        Files.writeString(results.resolve("test-result.json"), withRates(golden, 100, 200, 80, 120));
        Files.writeString(
                results.resolve("smaller-result.json"),
                withRates(
                        golden.replace("\"messageSize\" : 1024", "\"messageSize\" : 512"), 10, 20, 8, 12));
        Files.writeString(results.resolve("ignored.txt"), "not JSON");
        // A --repeat summary is not a result and must not become a row
        Files.writeString(results.resolve("test-result-summary.json"), "{\"repetitions\":3}");
        // Neither is any other JSON object
        Files.writeString(results.resolve("other.json"), "{\"name\":\"x\"}");

        Path csv = tempDir.resolve("results.csv");
        new ResultsToCsv().writeAllResultFiles(results.toString(), csv.toFile());

        var lines = Files.readAllLines(csv);
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0))
                .isEqualTo(
                        "topics,partitions,message-size,producers-per-topic,consumers-per-topic,"
                                + "prod-rate-min,prod-rate-avg,prod-rate-std-dev,prod-rate-max,"
                                + "con-rate-min,con-rate-avg,con-rate-std-dev,con-rate-max,validity,workload,driver");
        assertThat(lines.get(1)).startsWith("2,3,512,1,2,");
        assertThat(lines.get(1).split(",", -1)).hasSize(16);
        assertThat(lines.get(2)).startsWith("2,3,1024,1,2,");
        assertThat(lines.get(2).split(",", -1)).hasSize(16);
    }

    @Test
    void reportsExactRateStatistics() {
        TestResult result = new TestResult();
        result.publishRate = List.of(70_000.0, 12_000_000.0);
        result.consumeRate = List.of();

        assertThat(new ResultsToCsv().extractResults(result))
                .isEqualTo("0,0,0,0,0,70000,6035000,5965000,12000000,,,,,,,");

        result.validity = Validity.of(List.of());
        result.workload = "1 topic, 16 partitions \"max\"";
        result.driver = "Kafka";
        assertThat(new ResultsToCsv().extractResults(result))
                .endsWith(",,,,,valid,\"1 topic, 16 partitions \"\"max\"\"\",Kafka");
    }

    @Test
    void decimalsDoNotFollowTheDefaultLocale() {
        TestResult result = new TestResult();
        result.publishRate = List.of(1.0, 2.0);
        result.consumeRate = List.of();
        Locale previous = Locale.getDefault(Locale.Category.FORMAT);
        Locale.setDefault(Locale.Category.FORMAT, Locale.GERMANY);
        try {
            assertThat(new ResultsToCsv().extractResults(result)).contains(",0.5,");
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, previous);
        }
    }

    @Test
    void placesGeneratedFileInResultsDirectory() {
        assertThat(ResultsToCsv.outputFile("/results").toPath()).hasParentRaw(Path.of("/results"));
        assertThat(ResultsToCsv.outputFile(null)).hasNoParent();
        assertThat(ResultsToCsv.outputFile("").getName()).matches("results-\\d+\\.csv");
    }

    @Test
    void createsMissingParentDirectoryForCsvOutput() throws Exception {
        Path results = Files.createDirectory(tempDir.resolve("empty-results"));
        File csv = ResultsToCsv.outputFile(tempDir.resolve("missing/nested").toString());

        new ResultsToCsv().writeAllResultFiles(results.toString(), csv);

        assertThat(csv).isFile();
        assertThat(Files.readAllLines(csv.toPath())).hasSize(1);
    }

    @Test
    void failsWhenTheCsvCannotBeWritten() throws Exception {
        Path results = Files.createDirectory(tempDir.resolve("unwritable"));
        Path fileParent = Files.createFile(tempDir.resolve("blocker"));
        File blockedCsv = fileParent.resolve("results.csv").toFile();
        ResultsToCsv resultsToCsv = new ResultsToCsv();
        String resultsDirectory = results.toString();

        assertThatThrownBy(() -> resultsToCsv.writeAllResultFiles(resultsDirectory, blockedCsv))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("results.csv");

        if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            Path readOnly = Files.createDirectory(tempDir.resolve("read-only-csv"));
            Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("r-xr-xr-x"));
            assumeTrue(!Files.isWritable(readOnly));
            File readOnlyCsv = ResultsToCsv.outputFile(readOnly.toString());
            assertThatThrownBy(() -> resultsToCsv.writeAllResultFiles(resultsDirectory, readOnlyCsv))
                    .isInstanceOf(UncheckedIOException.class);
        }
    }

    @Test
    void writesHeaderForEmptyDirectory() throws Exception {
        Path results = Files.createDirectory(tempDir.resolve("empty"));
        Path csv = tempDir.resolve("empty.csv");

        new ResultsToCsv().writeAllResultFiles(results.toString(), csv.toFile());

        var lines = Files.readAllLines(csv);
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).startsWith("topics,partitions,");
    }

    @Test
    void rejectsMalformedInputJsonWithoutWritingCsv() throws Exception {
        Path results = Files.createDirectory(tempDir.resolve("malformed"));
        Files.writeString(results.resolve("bad.json"), "{");
        Path csv = tempDir.resolve("malformed.csv");
        ResultsToCsv resultsToCsv = new ResultsToCsv();
        String resultsDirectory = results.toString();
        File csvFile = csv.toFile();

        assertThatThrownBy(() -> resultsToCsv.writeAllResultFiles(resultsDirectory, csvFile))
                .isInstanceOf(RuntimeException.class);

        assertThat(csv).doesNotExist();
    }

    private static String resource(String name) throws Exception {
        try (var stream = ResultsToCsvTest.class.getClassLoader().getResourceAsStream(name)) {
            return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    private static String withRates(
            String json,
            double publishRate1,
            double publishRate2,
            double consumeRate1,
            double consumeRate2) {
        int closingBrace = json.lastIndexOf('}');
        return json.substring(0, closingBrace)
                + ",\"publishRate\":["
                + publishRate1
                + ","
                + publishRate2
                + "],\"consumeRate\":["
                + consumeRate1
                + ","
                + consumeRate2
                + "]"
                + json.substring(closingBrace);
    }
}
