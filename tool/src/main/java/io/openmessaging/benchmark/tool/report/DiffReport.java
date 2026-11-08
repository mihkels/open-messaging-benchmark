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
import io.openmessaging.benchmark.tool.report.Comparison.Status;
import io.openmessaging.benchmark.tool.report.MetricExtractor.LatencySource;
import io.openmessaging.benchmark.tool.report.RunContext.SettingDifference;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Everything a formatter prints about one comparison.
 *
 * @param workload the workload name
 * @param baselineFiles the baseline file names
 * @param candidateFiles the candidate file names
 * @param latencySource where the latency values come from
 * @param settingDifferences the environment settings that differ between the sides
 * @param warnings problems that do not stop the comparison, such as a mismatched workload
 * @param rows one row per metric
 */
record DiffReport(
        String workload,
        List<String> baselineFiles,
        List<String> candidateFiles,
        LatencySource latencySource,
        List<SettingDifference> settingDifferences,
        List<String> warnings,
        List<Row> rows) {

    /** Builds the report of two non-empty lists of runs. */
    static DiffReport of(
            List<Run> baseline, List<Run> candidate, double minChangePercent, List<String> warnings) {
        List<Run> all = RunContext.all(baseline, candidate);
        LatencySource source = MetricExtractor.latencySource(all);
        List<Map<Metric, Double>> baselineValues =
                baseline.stream().map(run -> MetricExtractor.extract(run, source)).toList();
        List<Map<Metric, Double>> candidateValues =
                candidate.stream().map(run -> MetricExtractor.extract(run, source)).toList();
        return new DiffReport(
                RunContext.workloadName(all),
                baseline.stream().map(Run::name).toList(),
                candidate.stream().map(Run::name).toList(),
                source,
                RunContext.settingDifferences(baseline, candidate),
                List.copyOf(warnings),
                Comparison.compare(baselineValues, candidateValues, minChangePercent));
    }

    /** Counts the rows per status, such as "2 regressed, 1 improved, 16 unchanged". */
    String statusSummary() {
        Map<Status, Integer> counts = new EnumMap<>(Status.class);
        rows.forEach(row -> counts.merge(row.status(), 1, Integer::sum));
        List<String> parts = new ArrayList<>();
        counts.forEach((status, count) -> parts.add(count + " " + status.label));
        return String.join(", ", parts);
    }

    /** Formats a value in the unit of its metric, with thousands separators. */
    static String value(Metric metric, double value) {
        return String.format(Locale.ROOT, metric.unit.pattern, value);
    }

    /** Formats a spread as "median [min – max]", or only the median when all runs agree. */
    static String spread(Metric metric, @Nullable Spread spread) {
        if (spread == null) {
            return "n/a";
        }
        String median = value(metric, spread.median());
        if (spread.min() == spread.max()) {
            return median;
        }
        return median + " [" + value(metric, spread.min()) + " – " + value(metric, spread.max()) + "]";
    }

    /** Formats the delta with its sign. */
    static String delta(Row row) {
        if (Double.isNaN(row.delta())) {
            return "n/a";
        }
        return (row.delta() > 0 ? "+" : "") + value(row.metric(), row.delta());
    }

    /** Formats the relative delta with its sign and one decimal. */
    static String deltaPercent(Row row) {
        if (Double.isNaN(row.deltaPercent())) {
            return "n/a";
        }
        return String.format(Locale.ROOT, "%+.1f%%", row.deltaPercent());
    }
}
