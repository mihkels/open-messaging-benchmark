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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Compares the metrics of baseline runs with candidate runs.
 *
 * <p>The run is the unit of replication: each run contributes one value per metric, and a delta is
 * "within noise" when the [min, max] ranges of both sides overlap. Latency samples within one run
 * are autocorrelated, so no significance test is applied to the samples themselves.
 */
final class Comparison {

    /** The outcome of one metric. */
    enum Status {
        IMPROVED("improved"),
        REGRESSED("regressed"),
        UNCHANGED("unchanged"),
        WITHIN_NOISE("within noise"),
        NOT_AVAILABLE("n/a");

        final String label;

        Status(String label) {
            this.label = label;
        }
    }

    /**
     * One compared metric.
     *
     * @param metric the compared metric
     * @param baseline the baseline spread, or null when no baseline run has the metric
     * @param candidate the candidate spread, or null when no candidate run has the metric
     * @param delta the candidate median minus the baseline median, NaN when not available
     * @param deltaPercent the delta relative to the baseline median, NaN when the baseline is zero
     * @param status the outcome
     */
    record Row(
            Metric metric,
            @Nullable Spread baseline,
            @Nullable Spread candidate,
            double delta,
            double deltaPercent,
            Status status) {}

    private Comparison() {}

    /**
     * Compares each metric.
     *
     * @param baseline the per-run values of the baseline side
     * @param candidate the per-run values of the candidate side
     * @param minChangePercent the smallest relative change that counts as a difference
     * @return one row per metric, in {@link Metric} order
     */
    static List<Row> compare(
            List<Map<Metric, Double>> baseline,
            List<Map<Metric, Double>> candidate,
            double minChangePercent) {
        List<Row> rows = new ArrayList<>();
        for (Metric metric : Metric.values()) {
            rows.add(row(metric, spread(baseline, metric), spread(candidate, metric), minChangePercent));
        }
        return rows;
    }

    static Row row(
            Metric metric,
            @Nullable Spread baseline,
            @Nullable Spread candidate,
            double minChangePercent) {
        if (baseline == null || candidate == null) {
            return new Row(metric, baseline, candidate, Double.NaN, Double.NaN, Status.NOT_AVAILABLE);
        }
        double base = baseline.median();
        double delta = candidate.median() - base;
        double deltaPercent = base == 0 ? Double.NaN : delta / Math.abs(base) * 100;
        return new Row(
                metric,
                baseline,
                candidate,
                delta,
                deltaPercent,
                status(metric, baseline, candidate, delta, deltaPercent, minChangePercent));
    }

    private static Status status(
            Metric metric,
            Spread baseline,
            Spread candidate,
            double delta,
            double deltaPercent,
            double minChangePercent) {
        if (delta == 0 || Math.abs(deltaPercent) < minChangePercent) {
            return Status.UNCHANGED;
        }
        if (baseline.runs() > 1 && candidate.runs() > 1 && baseline.overlaps(candidate)) {
            return Status.WITHIN_NOISE;
        }
        boolean higher = delta > 0;
        boolean better = higher == (metric.direction == Metric.Direction.HIGHER_IS_BETTER);
        return better ? Status.IMPROVED : Status.REGRESSED;
    }

    /**
     * Returns the regressed rows whose change is at least the threshold. A regression from a zero
     * baseline, such as new publish errors, always counts.
     *
     * @param rows the compared metrics
     * @param thresholdPercent the smallest relative regression to return
     * @return the regressions at or beyond the threshold
     */
    static List<Row> regressions(List<Row> rows, double thresholdPercent) {
        return rows.stream()
                .filter(row -> row.status() == Status.REGRESSED)
                .filter(row ->
                        Double.isNaN(row.deltaPercent()) || Math.abs(row.deltaPercent()) >= thresholdPercent)
                .toList();
    }

    private static @Nullable Spread spread(List<Map<Metric, Double>> runs, Metric metric) {
        List<Double> values =
                runs.stream().map(run -> run.get(metric)).filter(Objects::nonNull).toList();
        return values.isEmpty() ? null : Spread.of(values);
    }
}
