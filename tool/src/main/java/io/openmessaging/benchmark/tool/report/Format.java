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
            return Format.render(report, new TextStyle());
        }
    },

    /** A GitHub-flavored Markdown table, ready to append to {@code $GITHUB_STEP_SUMMARY}. */
    MARKDOWN {
        @Override
        String render(DiffReport report) {
            return Format.render(report, new MarkdownStyle());
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

    // The text and Markdown reports share one layout; a style writes each of its parts
    private static String render(DiffReport report, Style style) {
        StringBuilder out = new StringBuilder();
        style.title(out, report.workload());
        style.field(out, "Baseline", runs(report.baselineFiles()));
        style.field(out, "Candidate", runs(report.candidateFiles()));
        style.field(out, "Latency source", report.latencySource().description);
        style.field(out, "Result", report.statusSummary());
        for (String warning : report.warnings()) {
            style.warning(out, warning);
        }
        if (!report.settingDifferences().isEmpty()) {
            out.append('\n');
            style.table(out, SETTING_HEADER, settingCells(report), new boolean[3]);
        }
        out.append('\n');
        style.table(
                out, METRIC_HEADER, metricCells(report, style.emphasizeRegressions()), RIGHT_ALIGNED);
        return out.toString();
    }

    /** How {@link #render(DiffReport, Style)} writes each part of a report. */
    private interface Style {
        void title(StringBuilder out, String workload);

        void field(StringBuilder out, String name, String value);

        void warning(StringBuilder out, String warning);

        void table(
                StringBuilder out, List<String> header, List<List<String>> rows, boolean[] rightAligned);

        boolean emphasizeRegressions();
    }

    private static final class TextStyle implements Style {
        // The longest field name, "Latency source", with its colon and a space
        private static final int FIELD_WIDTH = 16;

        @Override
        public void title(StringBuilder out, String workload) {
            out.append("Benchmark comparison: ").append(workload).append('\n');
        }

        @Override
        public void field(StringBuilder out, String name, String value) {
            String label = name + ": ";
            out.append(label).append(" ".repeat(FIELD_WIDTH - label.length())).append(value).append('\n');
        }

        @Override
        public void warning(StringBuilder out, String warning) {
            out.append("\nWarning: ").append(warning).append('\n');
        }

        @Override
        public void table(
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

        @Override
        public boolean emphasizeRegressions() {
            return false;
        }
    }

    private static final class MarkdownStyle implements Style {
        @Override
        public void title(StringBuilder out, String workload) {
            out.append("## Benchmark comparison: ").append(escape(workload)).append("\n\n");
        }

        @Override
        public void field(StringBuilder out, String name, String value) {
            out.append("- **").append(name).append(":** ").append(escape(value)).append('\n');
        }

        @Override
        public void warning(StringBuilder out, String warning) {
            out.append("\n> **Warning:** ").append(escape(warning)).append('\n');
        }

        @Override
        public void table(
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

        @Override
        public boolean emphasizeRegressions() {
            return true;
        }
    }

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
