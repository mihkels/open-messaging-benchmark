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
        List<List<Run>> sides;
        try {
            sides = sides().stream().map(paths -> paths.stream().map(Run::read).toList()).toList();
        } catch (JacksonException e) {
            err.println("Cannot read result file: " + e.getOriginalMessage());
            return 1;
        }
        List<Run> baselineRuns = sides.get(0);
        List<Run> candidateRuns = sides.get(1);

        List<Run> allRuns = RunContext.all(baselineRuns, candidateRuns);
        List<String> mismatches = RunContext.workloadMismatches(allRuns);
        if (!mismatches.isEmpty() && !allowWorkloadMismatch) {
            mismatches.forEach(mismatch -> err.println("Error: " + mismatch));
            err.println("The runs are not comparable; pass --allow-workload-mismatch to compare anyway.");
            return 1;
        }

        DiffReport report;
        try {
            report = DiffReport.of(baselineRuns, candidateRuns, allRuns, minChange, mismatches);
        } catch (IllegalArgumentException e) {
            err.println("Cannot compare: " + e.getMessage());
            return 1;
        }
        if (!write(format.render(report), out, err)) {
            return 1;
        }
        Double threshold = failOnRegression;
        return threshold == null ? 0 : checkRegressions(report, threshold, err);
    }

    /**
     * Returns the baseline and candidate files, given either positionally or with --baseline and
     * --candidate.
     *
     * @return the baseline files, then the candidate files
     * @throws ParameterException if the files are given both ways, or incompletely
     */
    private List<List<Path>> sides() {
        boolean named = !baseline.isEmpty() || !candidate.isEmpty();
        if (!named) {
            if (files.size() != 2) {
                throw new ParameterException("Expected BASELINE and CANDIDATE files, got " + files.size());
            }
            return List.of(List.of(files.get(0)), List.of(files.get(1)));
        }
        if (!files.isEmpty()) {
            throw new ParameterException(
                    "Give the files either positionally or with --baseline/--candidate");
        }
        if (baseline.isEmpty() || candidate.isEmpty()) {
            throw new ParameterException("Both --baseline and --candidate need at least one file");
        }
        return List.of(baseline, candidate);
    }

    // Writes the report to --output, or to out without it; returns false if writing failed
    private boolean write(String rendered, PrintStream out, PrintStream err) {
        Path target = output;
        if (target == null) {
            out.print(rendered);
            return true;
        }
        try {
            Files.writeString(target, rendered, StandardCharsets.UTF_8);
            return true;
        } catch (IOException e) {
            err.println("Cannot write " + target + ": " + e.getMessage());
            return false;
        }
    }

    // Lists the regressions at or past the threshold; returns the exit code
    private static int checkRegressions(DiffReport report, double threshold, PrintStream err) {
        List<Row> regressions = Comparison.regressions(report.rows(), threshold);
        if (regressions.isEmpty()) {
            return 0;
        }
        for (Row row : regressions) {
            err.printf(
                    Locale.ROOT,
                    "Regression: %s %s (%s -> %s)%n",
                    row.metric().labelWithUnit(),
                    DiffReport.deltaPercent(row),
                    DiffReport.spread(row.metric(), row.baseline()),
                    DiffReport.spread(row.metric(), row.candidate()));
        }
        err.printf(
                Locale.ROOT,
                "%d metric(s) regressed by at least %s%%%n",
                regressions.size(),
                PercentConverter.format(threshold));
        return EXIT_REGRESSION;
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
            if (Double.isNaN(percent) || percent < 0 || Double.isInfinite(percent)) {
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
