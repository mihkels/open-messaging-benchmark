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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.mihkels.omb.Finding.Severity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToDoubleFunction;
import org.jspecify.annotations.Nullable;

/**
 * The run-to-run spread of one workload and driver combination run with {@code --repeat}, written
 * next to the per-repetition results. Failed repetitions are listed but left out of the metrics.
 *
 * @param workload the workload name
 * @param driver the driver name
 * @param repetitions the repetitions requested
 * @param succeeded the repetitions that produced a result, after their retries
 * @param results each repetition that ran
 * @param metrics the spread of each metric over the successful repetitions; empty when none did
 * @param thresholds the workload thresholds evaluated against the medians, and the delivery limits
 *     against the worst repetition
 * @param findings the summary's own findings: run-to-run variance and failed repetitions
 * @param validity the worst-of verdict of the summary's findings and every finding of each
 *     successful repetition
 */
public record RepeatSummary(
        @Nullable String workload,
        String driver,
        int repetitions,
        int succeeded,
        List<Repetition> results,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, Stats> metrics,
        @JsonInclude(JsonInclude.Include.NON_NULL) Thresholds.@Nullable Result thresholds,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) List<Finding> findings,
        Validity validity) {
    static final String FAILED_REPETITIONS = "failedRepetitions";

    public RepeatSummary {
        results = List.copyOf(results);
        // Keeps the documented metric order
        metrics = Collections.unmodifiableMap(new LinkedHashMap<>(metrics));
        findings = List.copyOf(findings);
    }

    /**
     * One repetition.
     *
     * @param repetition its number, from 1
     * @param succeeded whether it produced a result
     * @param file the result file, when it succeeded
     * @param failure why it failed, when it did
     * @param validity its result's verdict, when it succeeded
     * @param result its result, when it succeeded; not written to the summary
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Repetition(
            int repetition,
            boolean succeeded,
            @Nullable String file,
            @Nullable String failure,
            Validity.@Nullable Status validity,
            @JsonIgnore @Nullable TestResult result) {}

    /**
     * Spread of one metric.
     *
     * @param median the median, the mean of the middle two for an even count
     * @param min the lowest value
     * @param max the highest value
     * @param cv the coefficient of variation, sample standard deviation over mean; absent with fewer
     *     than 2 successful repetitions or a zero mean
     */
    public record Stats(
            double median,
            double min,
            double max,
            @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable Double cv) {}

    // Throughput and the main latency percentiles, in milliseconds, keyed as in the result file
    private static final Map<String, ToDoubleFunction<TestResult>> TRACKED_METRICS =
            new LinkedHashMap<>();

    static {
        TRACKED_METRICS.put("publishRateAvg", r -> r.summary.publishRateAvg());
        TRACKED_METRICS.put("consumeRateAvg", r -> r.summary.consumeRateAvg());
        TRACKED_METRICS.put("throughputMbSec", r -> r.summary.throughputMbSec());
        TRACKED_METRICS.put("aggregatedPublishLatency50pct", r -> r.aggregatedPublishLatency50pct);
        TRACKED_METRICS.put("aggregatedPublishLatency99pct", r -> r.aggregatedPublishLatency99pct);
        TRACKED_METRICS.put("aggregatedPublishLatency999pct", r -> r.aggregatedPublishLatency999pct);
        TRACKED_METRICS.put("aggregatedEndToEndLatency50pct", r -> r.aggregatedEndToEndLatency50pct);
        TRACKED_METRICS.put("aggregatedEndToEndLatency99pct", r -> r.aggregatedEndToEndLatency99pct);
        TRACKED_METRICS.put("aggregatedEndToEndLatency999pct", r -> r.aggregatedEndToEndLatency999pct);
    }

    /**
     * Summarizes the repetitions of a combination.
     *
     * @param workload the workload, for its thresholds and checks
     * @param driver the driver name
     * @param repetitions the repetitions requested
     * @param runs each repetition that ran, in order
     * @return the summary; metrics and thresholds only when a repetition succeeded
     */
    static RepeatSummary of(
            Workload workload, String driver, int repetitions, List<Repetition> runs) {
        List<TestResult> successes =
                runs.stream().map(Repetition::result).filter(Objects::nonNull).toList();
        Map<String, Stats> metrics = new LinkedHashMap<>();
        List<Finding> findings = new ArrayList<>();
        Thresholds.Result evaluated = null;
        if (!successes.isEmpty()) {
            TRACKED_METRICS.forEach((name, metric) ->
                    metrics.put(name, stats(successes.stream().mapToDouble(metric).toArray())));
            findings.addAll(
                    Objects.requireNonNullElseGet(workload.checks, RunChecks::new).runToRunVariance(metrics));
            Thresholds thresholds = workload.thresholds;
            if (thresholds != null) {
                evaluated =
                        thresholds.evaluate(
                                median(successes, r -> r.aggregatedPublishLatency99pct),
                                (long) median(successes, r -> r.summary.totalMessagesSent()),
                                median(successes, r -> r.summary.publishRateAvg()),
                                successes.getFirst().summary.targetRate());
                // Delivery limits judge the worst repetition, not the median
                thresholds.evaluateDelivery(
                        evaluated, successes.stream().<@Nullable Delivery>map(r -> r.delivery).toList());
            }
        }
        int failed = repetitions - successes.size();
        if (failed > 0) {
            findings.add(
                    new Finding(
                            FAILED_REPETITIONS,
                            successes.isEmpty() ? Severity.INVALID : Severity.WARNING,
                            failed
                                    + " of "
                                    + repetitions
                                    + " repetitions failed or were skipped and are left out of the statistics",
                            (double) failed,
                            0.0));
        }
        // Each successful repetition's findings, tagged with its number, count toward the verdict
        List<Finding> verdictFindings = new ArrayList<>(findings);
        for (Repetition run : runs) {
            int number = run.repetition();
            TestResult result = run.result();
            Validity validity = result == null ? null : result.validity;
            if (validity != null) {
                validity
                        .findings()
                        .forEach(f ->
                                verdictFindings.add(
                                        new Finding(
                                                f.check(),
                                                f.severity(),
                                                "repetition " + number + ": " + f.message(),
                                                f.value(),
                                                f.limit())));
            }
        }
        return new RepeatSummary(
                workload.name,
                driver,
                repetitions,
                successes.size(),
                runs,
                metrics,
                evaluated,
                findings,
                Validity.of(verdictFindings));
    }

    static Stats stats(double[] values) {
        double min = Arrays.stream(values).min().orElse(0);
        double max = Arrays.stream(values).max().orElse(0);
        Double cv = null;
        if (values.length >= 2) {
            double mean = Arrays.stream(values).average().orElse(0);
            double squares = Arrays.stream(values).map(v -> (v - mean) * (v - mean)).sum();
            if (mean != 0) {
                cv = Math.sqrt(squares / (values.length - 1)) / Math.abs(mean);
            }
        }
        return new Stats(median(values), min, max, cv);
    }

    private static double median(List<TestResult> results, ToDoubleFunction<TestResult> metric) {
        return median(results.stream().mapToDouble(metric).toArray());
    }

    /**
     * The median, the mean of the middle two for an even count.
     *
     * @param values the values, not modified; at least one
     * @return the median
     */
    public static double median(double[] values) {
        double[] sorted = values.clone();
        Arrays.sort(sorted);
        int middle = sorted.length / 2;
        return sorted.length % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    }
}
