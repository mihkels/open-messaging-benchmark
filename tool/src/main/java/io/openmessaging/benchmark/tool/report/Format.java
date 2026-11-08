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

import io.openmessaging.benchmark.tool.report.Comparison.Row;
import io.openmessaging.benchmark.tool.report.RunContext.SettingDifference;
import java.util.ArrayList;
import java.util.List;

/** The output formats of {@code benchmark-report diff}. */
enum Format {
    /** Aligned columns for a terminal. */
    TEXT {
        @Override
        String render(DiffReport report) {
            StringBuilder out = new StringBuilder();
            out.append("Benchmark comparison: ").append(report.workload()).append('\n');
            out.append("Baseline:       ").append(runs(report.baselineFiles())).append('\n');
            out.append("Candidate:      ").append(runs(report.candidateFiles())).append('\n');
            out.append("Latency source: ").append(report.latencySource().description).append('\n');
            out.append("Result:         ").append(report.statusSummary()).append('\n');
            for (String warning : report.warnings()) {
                out.append("\nWarning: ").append(warning).append('\n');
            }
            if (!report.settingDifferences().isEmpty()) {
                out.append('\n');
                table(out, SETTING_HEADER, settingCells(report), new boolean[3]);
            }
            out.append('\n');
            table(out, METRIC_HEADER, metricCells(report, false), RIGHT_ALIGNED);
            return out.toString();
        }

        private void table(
                StringBuilder out, List<String> header, List<List<String>> rows, boolean[] rightAligned) {
            int[] widths = new int[header.size()];
            List<List<String>> all = new ArrayList<>();
            all.add(header);
            all.addAll(rows);
            for (List<String> row : all) {
                for (int i = 0; i < widths.length; i++) {
                    widths[i] = Math.max(widths[i], row.get(i).length());
                }
            }
            for (List<String> row : all) {
                StringBuilder line = new StringBuilder();
                for (int i = 0; i < widths.length; i++) {
                    String cell = row.get(i);
                    String padding = " ".repeat(widths[i] - cell.length());
                    line.append(rightAligned[i] ? padding + cell : cell + padding);
                    line.append(i < widths.length - 1 ? "  " : "");
                }
                out.append(line.toString().stripTrailing()).append('\n');
            }
        }
    },

    /** A GitHub-flavored Markdown table, ready to append to {@code $GITHUB_STEP_SUMMARY}. */
    MARKDOWN {
        @Override
        String render(DiffReport report) {
            StringBuilder out = new StringBuilder();
            out.append("## Benchmark comparison: ").append(escape(report.workload())).append("\n\n");
            out.append("- **Baseline:** ").append(escape(runs(report.baselineFiles()))).append('\n');
            out.append("- **Candidate:** ").append(escape(runs(report.candidateFiles()))).append('\n');
            out.append("- **Latency source:** ").append(report.latencySource().description).append('\n');
            out.append("- **Result:** ").append(report.statusSummary()).append('\n');
            for (String warning : report.warnings()) {
                out.append("\n> **Warning:** ").append(escape(warning)).append('\n');
            }
            if (!report.settingDifferences().isEmpty()) {
                out.append('\n');
                table(out, SETTING_HEADER, settingCells(report), new boolean[3]);
            }
            out.append('\n');
            table(out, METRIC_HEADER, metricCells(report, true), RIGHT_ALIGNED);
            return out.toString();
        }

        private void table(
                StringBuilder out, List<String> header, List<List<String>> rows, boolean[] rightAligned) {
            out.append("| ").append(String.join(" | ", header)).append(" |\n|");
            for (boolean right : rightAligned) {
                out.append(right ? " ---: |" : " --- |");
            }
            out.append('\n');
            for (List<String> row : rows) {
                out.append("| ")
                        .append(String.join(" | ", row.stream().map(Format::escape).toList()))
                        .append(" |\n");
            }
        }
    },

    /**
     * Machine-readable output. Each side is also given in Bencher Metric Format, so a benchmark
     * tracker can ingest it directly.
     */
    JSON {
        @Override
        String render(DiffReport report) {
            return JsonReport.render(report);
        }
    };

    private static final List<String> SETTING_HEADER = List.of("Setting", "Baseline", "Candidate");
    private static final List<String> METRIC_HEADER =
            List.of("Metric", "Baseline", "Candidate", "Δ", "Δ%", "Status");
    private static final boolean[] RIGHT_ALIGNED = {false, true, true, true, true, false};

    /**
     * Renders the report in this format.
     *
     * @param report the comparison to render
     * @return the rendered report
     */
    abstract String render(DiffReport report);

    private static String runs(List<String> files) {
        return files.size()
                + (files.size() == 1 ? " run (" : " runs (")
                + String.join(", ", files)
                + ")";
    }

    private static List<List<String>> settingCells(DiffReport report) {
        List<List<String>> cells = new ArrayList<>();
        for (SettingDifference difference : report.settingDifferences()) {
            cells.add(
                    List.of(
                            difference.name(),
                            oneLine(String.join(", ", difference.baseline())),
                            oneLine(String.join(", ", difference.candidate()))));
        }
        return cells;
    }

    private static List<List<String>> metricCells(DiffReport report, boolean emphasizeRegressions) {
        List<List<String>> cells = new ArrayList<>();
        for (Row row : report.rows()) {
            String status = row.status().label;
            if (emphasizeRegressions && row.status() == Comparison.Status.REGRESSED) {
                status = "**" + status + "**";
            }
            cells.add(
                    List.of(
                            row.metric().labelWithUnit(),
                            DiffReport.spread(row.metric(), row.baseline()),
                            DiffReport.spread(row.metric(), row.candidate()),
                            DiffReport.delta(row),
                            DiffReport.deltaPercent(row),
                            status));
        }
        return cells;
    }

    // Keeps a value on one table line; driver settings can hold multi-line property blocks
    private static String oneLine(String value) {
        return value.strip().replace("\r", "").replace("\n", "; ");
    }

    private static String escape(String value) {
        return value.replace("|", "\\|");
    }
}
