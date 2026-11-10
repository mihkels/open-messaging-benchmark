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
package io.openmessaging.benchmark.tool.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.beust.jcommander.ParameterException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkReportTest {
    @TempDir Path tempDir;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private int run(String... args) {
        return BenchmarkReport.run(
                args,
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    private String out() {
        return out.toString(StandardCharsets.UTF_8);
    }

    private String err() {
        return err.toString(StandardCharsets.UTF_8);
    }

    private static String fixture(String name) {
        return Fixtures.path(name).toString();
    }

    private static String[] sides(String baseline, String candidate, String... options) {
        List<String> args = new ArrayList<>(List.of("diff", "--baseline"));
        args.addAll(List.of(Fixtures.files(baseline)));
        args.add("--candidate");
        args.addAll(List.of(Fixtures.files(candidate)));
        args.addAll(List.of(options));
        return args.toArray(String[]::new);
    }

    @Test
    void comparesTwoFilesGivenPositionally() {
        int exit = run("diff", fixture("baseline-1.json"), fixture("candidate-improvement-1.json"));
        assertThat(exit).isZero();
        assertThat(out())
                .contains("Baseline:       1 run (baseline-1.json)")
                .contains("Publish latency p99 (ms)          5.000      4.000  -1.000  -20.0%  improved");
        assertThat(err()).isEmpty();
    }

    @Test
    void failsOnARegressionBeyondTheThreshold() {
        int exit = run(sides("baseline", "candidate-regression", "--fail-on-regression", "10%"));
        assertThat(exit).isEqualTo(DiffCommand.EXIT_REGRESSION);
        assertThat(out()).contains("18 regressed");
        assertThat(err())
                .contains(
                        "Regression: Publish latency p99 (ms) +30.0% (5.000 [4.900 – 5.100] -> 6.500 [6.400 – 6.600])")
                .contains("Regression: Publish errors n/a (0 -> 5)")
                .contains("18 metric(s) regressed by at least 10%");
    }

    @Test
    void countsNewErrorsAsARegressionAtAnyThreshold() {
        assertThat(run(sides("baseline", "candidate-regression", "--fail-on-regression", "50")))
                .isEqualTo(2);
        // Only the new publish errors, from a zero baseline, exceed 50%
        assertThat(err()).contains("1 metric(s) regressed by at least 50%");
    }

    @Test
    void passesOnAnImprovement() {
        int exit = run(sides("baseline", "candidate-improvement", "--fail-on-regression", "0.5"));
        assertThat(exit).isZero();
        assertThat(out()).contains("17 improved, 2 unchanged");
    }

    @Test
    void treatsOverlappingRepeatedRunsAsNoise() {
        int exit = run(sides("baseline", "candidate-noise", "--fail-on-regression", "1%"));
        assertThat(exit).isZero();
        assertThat(out()).contains("within noise").doesNotContain("regressed  ");
    }

    @Test
    void refusesMismatchedWorkloads() {
        int exit = run("diff", fixture("baseline-1.json"), fixture("mismatched-workload.json"));
        assertThat(exit).isEqualTo(1);
        assertThat(out()).isEmpty();
        assertThat(err())
                .contains(
                        "Error: Workload producerRate differs: 10000 (baseline-1.json)"
                                + " vs 20000 (mismatched-workload.json)")
                .contains("pass --allow-workload-mismatch");
    }

    @Test
    void comparesMismatchedWorkloadsWithAWarningWhenAllowed() {
        int exit =
                run(
                        "diff",
                        "--format",
                        "markdown",
                        "--allow-workload-mismatch",
                        fixture("baseline-1.json"),
                        fixture("mismatched-workload.json"));
        assertThat(exit).isZero();
        assertThat(out()).contains("> **Warning:** Workload messageSize differs");
    }

    @Test
    void writesTheReportToAFile() {
        Path report = tempDir.resolve("report.json");
        int exit =
                run(
                        "diff",
                        "--format",
                        "JSON",
                        "--output",
                        report.toString(),
                        fixture("baseline-1.json"),
                        fixture("baseline-2.json"));
        assertThat(exit).isZero();
        assertThat(out()).isEmpty();
        assertThat(Fixtures.MAPPER.readTree(report.toFile()).get("rows"))
                .hasSize(Metric.values().length);
    }

    @Test
    void reportsAnUnwritableOutput() {
        int exit =
                run(
                        "diff",
                        "--output",
                        tempDir.toString(),
                        fixture("baseline-1.json"),
                        fixture("baseline-2.json"));
        assertThat(exit).isEqualTo(1);
        assertThat(err()).startsWith("Cannot write " + tempDir);
    }

    @Test
    void reportsAnUnreadableResultFile() {
        int exit = run("diff", tempDir.resolve("missing.json").toString(), fixture("baseline-1.json"));
        assertThat(exit).isEqualTo(1);
        assertThat(err()).startsWith("Cannot read result file: ").contains("missing.json");
    }

    @Test
    void reportsAnInvalidHistogram() {
        Map<String, String> invalid = Map.of("publish", "!", "publishDelay", "!", "endToEnd", "!");
        Path baseline = Fixtures.withHistogramStrings("baseline-1.json", tempDir, invalid);
        Path candidate = Fixtures.withHistogramStrings("baseline-2.json", tempDir, invalid);
        int exit = run("diff", baseline.toString(), candidate.toString());
        assertThat(exit).isEqualTo(1);
        assertThat(err()).startsWith("Cannot compare: ").contains("invalid publish histogram");
    }

    @Test
    void comparesRawHistogramsWhenBothSidesHaveThem() {
        Path baseline = Fixtures.withHistograms("baseline-1.json", tempDir, 1);
        Path candidate = Fixtures.withHistograms("baseline-2.json", tempDir, 2);
        int exit = run("diff", baseline.toString(), candidate.toString(), "--fail-on-regression", "90");
        assertThat(exit).isEqualTo(2);
        assertThat(out()).contains("Latency source: raw histograms");
        assertThat(err()).contains("Publish latency p50 (ms) +100.0%");
    }

    @Test
    void rejectsInconsistentFileArguments() {
        assertThat(run("diff", fixture("baseline-1.json"))).isEqualTo(1);
        assertThat(err())
                .contains("Expected BASELINE and CANDIDATE files, got 1")
                .contains("Usage: diff");

        assertThat(run("diff", "--baseline", fixture("baseline-1.json"))).isEqualTo(1);
        assertThat(err()).contains("Both --baseline and --candidate need at least one file");

        assertThat(run("diff", "--candidate", fixture("baseline-1.json"))).isEqualTo(1);

        assertThat(
                        run(
                                "diff",
                                fixture("baseline-3.json"),
                                "--baseline",
                                fixture("baseline-1.json"),
                                "--candidate",
                                fixture("baseline-2.json")))
                .isEqualTo(1);
        assertThat(err()).contains("either positionally or with --baseline/--candidate");
    }

    @Test
    void rejectsBadOptions() {
        assertThat(run("diff", "--format", "xml", "a", "b")).isEqualTo(1);
        assertThat(err()).contains("Unknown format 'xml'").contains("Usage: benchmark-report");
        assertThat(run("diff", "--min-change", "-1", "a", "b")).isEqualTo(1);
        assertThat(err()).contains("Percentage must be zero or more: '-1'");
    }

    @Test
    void printsUsage() {
        assertThat(run("--help")).isZero();
        assertThat(out()).contains("Usage: benchmark-report").contains("diff");
        out.reset();
        assertThat(run("diff", "--help")).isZero();
        assertThat(out()).contains("Usage: diff").contains("--fail-on-regression");
        assertThat(run()).isEqualTo(1);
        assertThat(err()).contains("Usage: benchmark-report");
    }

    @Test
    void parsesPercentages() {
        DiffCommand.PercentConverter converter = new DiffCommand.PercentConverter();
        assertThat(converter.convert("10%")).isEqualTo(10);
        assertThat(converter.convert(" 2.5 % ")).isEqualTo(2.5);
        assertThat(converter.convert("0")).isZero();
        assertThatThrownBy(() -> converter.convert("ten")).isInstanceOf(ParameterException.class);
        assertThatThrownBy(() -> converter.convert("NaN")).isInstanceOf(ParameterException.class);
        assertThatThrownBy(() -> converter.convert("Infinity")).isInstanceOf(ParameterException.class);
        assertThat(DiffCommand.PercentConverter.format(10)).isEqualTo("10");
        assertThat(DiffCommand.PercentConverter.format(2.5)).isEqualTo("2.5");
    }

    @Test
    void writtenFilesAreUtf8() throws IOException {
        Path report = tempDir.resolve("report.md");
        run(
                sides(
                        "baseline", "candidate-noise", "--format", "markdown", "--output", report.toString()));
        assertThat(Files.readString(report, StandardCharsets.UTF_8)).contains("Δ%");
    }

    @Test
    void rendersAnHtmlReportToStdout() {
        int exit = run("html", fixture("baseline-1.json"), fixture("legacy-v1.json"));
        assertThat(exit).isZero();
        assertThat(out())
                .contains("<!DOCTYPE html>")
                .contains("\"baseline-1.json\"", "\"legacy-v1.json\"", "echarts@5.6.0");
        assertThat(err()).isEmpty();
    }

    @Test
    void writesTheHtmlReportToAFileAndLabelsDuplicateNamesByPath() throws IOException {
        Path copy = Files.createDirectory(tempDir.resolve("other")).resolve("baseline-1.json");
        Files.copy(Fixtures.path("baseline-1.json"), copy);
        Path report = tempDir.resolve("report.html");

        int exit = run("html", "-o", report.toString(), fixture("baseline-1.json"), copy.toString());

        assertThat(exit).isZero();
        assertThat(out()).isEmpty();
        assertThat(Files.readString(report, StandardCharsets.UTF_8))
                .contains("\"baseline-1.json\"", "\"" + copy + "\"");
    }

    @Test
    void htmlReportsUnreadableAndUnwritableFiles() {
        assertThat(run("html", tempDir.resolve("missing.json").toString())).isEqualTo(1);
        assertThat(err()).startsWith("Cannot read result file: ").contains("missing.json");

        err.reset();
        assertThat(run("html", "--output", tempDir.toString(), fixture("baseline-1.json")))
                .isEqualTo(1);
        assertThat(err()).startsWith("Cannot write " + tempDir);
    }

    @Test
    void htmlNeedsAResultFileAndPrintsItsUsage() {
        assertThat(run("html")).isEqualTo(1);
        assertThat(err()).contains("Expected at least one result file");

        assertThat(run("html", "--help")).isZero();
        assertThat(out()).contains("Usage: html", "--output");
    }
}
