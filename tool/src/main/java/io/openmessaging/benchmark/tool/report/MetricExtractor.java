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

import io.openmessaging.benchmark.TestResult;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.HdrHistogram.Histogram;

/** Reads the {@link Metric} values of a run. */
final class MetricExtractor {
    /** The raw histograms every latency metric reads. */
    static final List<String> HISTOGRAMS =
            Arrays.stream(Metric.values())
                    .map(metric -> metric.histogram)
                    .filter(Objects::nonNull)
                    .distinct()
                    .toList();

    /** Where latency values come from. */
    enum LatencySource {
        HISTOGRAMS("raw histograms"),
        AGGREGATED("aggregated quantiles");

        final String description;

        LatencySource(String description) {
            this.description = description;
        }
    }

    private MetricExtractor() {}

    /**
     * Chooses the raw histograms only when every run has them, so that both sides of a comparison are
     * read the same way.
     *
     * @param runs the runs of both sides
     * @return the latency source to read every run with
     */
    static LatencySource latencySource(Collection<Run> runs) {
        boolean allHaveHistograms =
                runs.stream()
                        .allMatch(run -> {
                            Map<String, String> histograms = run.result().histograms;
                            return histograms != null && histograms.keySet().containsAll(HISTOGRAMS);
                        });
        return allHaveHistograms ? LatencySource.HISTOGRAMS : LatencySource.AGGREGATED;
    }

    /**
     * Returns the values of a run. A metric the file cannot provide, such as the error count of a
     * version 1 file, is absent from the map.
     *
     * @param run the run to read
     * @param source where to read latencies from
     * @return the value of each available metric
     */
    static Map<Metric, Double> extract(Run run, LatencySource source) {
        TestResult result = run.result();
        Map<Metric, Double> values = new EnumMap<>(Metric.class);
        if (run.hasMetadata()) {
            values.put(Metric.PUBLISH_RATE, result.summary.publishRateAvg());
            values.put(Metric.CONSUME_RATE, result.summary.consumeRateAvg());
            values.put(Metric.PUBLISH_ERRORS, (double) result.summary.totalErrors());
            values.put(Metric.MAX_BACKLOG, (double) result.summary.maxBacklog());
        } else {
            // Version 1 files have no summary; derive it from the per-period series
            values.put(Metric.PUBLISH_RATE, TestResult.average(result.publishRate));
            values.put(Metric.CONSUME_RATE, TestResult.average(result.consumeRate));
            values.put(
                    Metric.MAX_BACKLOG,
                    (double) result.backlog.stream().mapToLong(Long::longValue).max().orElse(0));
        }

        Map<String, Histogram> decoded = new HashMap<>();
        for (Metric metric : Metric.values()) {
            String histogram = metric.histogram;
            if (histogram == null) {
                continue;
            }
            if (source == LatencySource.HISTOGRAMS) {
                Histogram h = decoded.computeIfAbsent(histogram, key -> decode(run, key));
                values.put(metric, TestResult.millisAt(h, metric.percentile));
            } else {
                values.put(metric, Objects.requireNonNull(metric.aggregated).applyAsDouble(result));
            }
        }
        return values;
    }

    /**
     * Decodes one raw histogram of a run.
     *
     * @param run the run holding the histogram
     * @param key the histogram key, such as {@code publish}
     * @return the decoded histogram, in microseconds
     * @throws IllegalArgumentException if the run has no such histogram or it cannot be decoded
     */
    private static Histogram decode(Run run, String key) {
        Map<String, String> histograms = run.result().histograms;
        String encoded = histograms == null ? null : histograms.get(key);
        if (encoded == null) {
            throw new IllegalArgumentException(run.name() + " has no " + key + " histogram");
        }
        try {
            return TestResult.decodeHistogram(encoded);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    run.name() + " has an invalid " + key + " histogram: " + e.getMessage(), e);
        }
    }
}
