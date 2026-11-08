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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import io.openmessaging.benchmark.tool.report.Comparison.Row;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;

/** {@code benchmark-report diff}: compares baseline runs with candidate runs. */
@Parameters(
        commandNames = "diff",
        commandDescription =
                "Compare baseline and candidate result files: diff BASELINE CANDIDATE, or"
                        + " diff --baseline A1 A2 ... --candidate B1 B2 ...")
final class DiffCommand {
    /** Exit code when a regression exceeds {@code --fail-on-regression}. */
    static final int EXIT_REGRESSION = 2;

    @Parameter(description = "BASELINE CANDIDATE")
    List<Path> files = new ArrayList<>();

    @Parameter(
            names = "--baseline",
            description = "Baseline result files; repeated runs of the same setup",
            variableArity = true)
    List<Path> baseline = new ArrayList<>();

    @Parameter(
            names = "--candidate",
            description = "Candidate result files; repeated runs of the same setup",
            variableArity = true)
    List<Path> candidate = new ArrayList<>();

    @Parameter(
            names = "--format",
            description = "Output format: text, markdown or json",
            converter = FormatConverter.class)
    Format format = Format.TEXT;

    @Parameter(
            names = "--fail-on-regression",
            description =
                    "Exit with code 2 when a metric regresses by at least this percentage, for example 10%",
            converter = PercentConverter.class)
    @Nullable Double failOnRegression;

    @Parameter(
            names = "--min-change",
            description = "Smallest relative change reported as improved or regressed, for example 1%",
            converter = PercentConverter.class)
    double minChange = 1;

    @Parameter(
            names = "--allow-workload-mismatch",
            description = "Compare runs of different workloads, with a warning, instead of refusing")
    boolean allowWorkloadMismatch;

    @Parameter(names = "--output", description = "Write the report to this file instead of stdout")
    @Nullable Path output;

    @Parameter(
            names = {"-h", "--help"},
            description = "Help message",
            help = true)
    boolean help;

    /**
     * Runs the comparison.
     *
     * @param out receives the report unless --output is given
     * @param err receives errors and the list of regressions
     * @return 0 on success, 1 on bad input or a workload mismatch, 2 on a regression
     * @throws ParameterException if the files are not given as either two positional arguments or
     *     with --baseline and --candidate
     */
    int execute(PrintStream out, PrintStream err) {
        boolean named = !baseline.isEmpty() || !candidate.isEmpty();
        List<Path> baselineFiles = baseline;
        List<Path> candidateFiles = candidate;
        if (named && !files.isEmpty()) {
            throw new ParameterException(
                    "Give the files either positionally or with --baseline/--candidate");
        }
        if (!named) {
            if (files.size() != 2) {
                throw new ParameterException("Expected BASELINE and CANDIDATE files, got " + files.size());
            }
            baselineFiles = List.of(files.get(0));
            candidateFiles = List.of(files.get(1));
        } else if (baseline.isEmpty() || candidate.isEmpty()) {
            throw new ParameterException("Both --baseline and --candidate need at least one file");
        }

        List<Run> baselineRuns;
        List<Run> candidateRuns;
        try {
            baselineRuns = baselineFiles.stream().map(Run::read).toList();
            candidateRuns = candidateFiles.stream().map(Run::read).toList();
        } catch (JacksonException e) {
            err.println("Cannot read result file: " + e.getOriginalMessage());
            return 1;
        }

        List<String> mismatches =
                RunContext.workloadMismatches(RunContext.all(baselineRuns, candidateRuns));
        if (!mismatches.isEmpty() && !allowWorkloadMismatch) {
            mismatches.forEach(mismatch -> err.println("Error: " + mismatch));
            err.println("The runs are not comparable; pass --allow-workload-mismatch to compare anyway.");
            return 1;
        }

        DiffReport report;
        try {
            report = DiffReport.of(baselineRuns, candidateRuns, minChange, mismatches);
        } catch (IllegalArgumentException e) {
            err.println("Cannot compare: " + e.getMessage());
            return 1;
        }
        String rendered = format.render(report);
        Path target = output;
        if (target == null) {
            out.print(rendered);
        } else {
            try {
                Files.writeString(target, rendered, StandardCharsets.UTF_8);
            } catch (IOException e) {
                err.println("Cannot write " + target + ": " + e.getMessage());
                return 1;
            }
        }

        Double threshold = failOnRegression;
        if (threshold != null) {
            List<Row> regressions = Comparison.regressions(report.rows(), threshold);
            if (!regressions.isEmpty()) {
                for (Row row : regressions) {
                    err.println(
                            "Regression: "
                                    + row.metric().labelWithUnit()
                                    + " "
                                    + DiffReport.deltaPercent(row)
                                    + " ("
                                    + DiffReport.spread(row.metric(), row.baseline())
                                    + " -> "
                                    + DiffReport.spread(row.metric(), row.candidate())
                                    + ")");
                }
                err.printf(
                        Locale.ROOT,
                        "%d metric(s) regressed by at least %s%%%n",
                        regressions.size(),
                        PercentConverter.format(threshold));
                return EXIT_REGRESSION;
            }
        }
        return 0;
    }

    /** Parses a format name in any case. */
    public static final class FormatConverter implements IStringConverter<Format> {
        @Override
        public Format convert(String value) {
            try {
                return Format.valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new ParameterException(
                        "Unknown format '" + value + "'; expected text, markdown or json", e);
            }
        }
    }

    /** Parses a non-negative percentage such as {@code 10%} or {@code 10}. */
    public static final class PercentConverter implements IStringConverter<Double> {
        @Override
        public Double convert(String value) {
            String number = value.strip();
            if (number.endsWith("%")) {
                number = number.substring(0, number.length() - 1).strip();
            }
            double percent;
            try {
                percent = Double.parseDouble(number);
            } catch (NumberFormatException e) {
                throw new ParameterException("Not a percentage: '" + value + "'", e);
            }
            if (!(percent >= 0) || Double.isInfinite(percent)) {
                throw new ParameterException("Percentage must be zero or more: '" + value + "'");
            }
            return percent;
        }

        static String format(double percent) {
            return percent == Math.rint(percent)
                    ? String.valueOf((long) percent)
                    : String.valueOf(percent);
        }
    }
}
