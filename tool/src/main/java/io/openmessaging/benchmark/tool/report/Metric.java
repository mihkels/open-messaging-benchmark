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
import java.util.Locale;
import java.util.function.ToDoubleFunction;
import org.jspecify.annotations.Nullable;

/** A compared value of a run, in the order it is reported. */
enum Metric {
    PUBLISH_RATE("publish-rate", "Publish rate", Unit.RATE, Direction.HIGHER_IS_BETTER),
    CONSUME_RATE("consume-rate", "Consume rate", Unit.RATE, Direction.HIGHER_IS_BETTER),
    PUBLISH_LATENCY_P50(
            Labels.PUBLISH_LATENCY,
            TestResult.PUBLISH_HISTOGRAM,
            50,
            r -> r.aggregatedPublishLatency50pct),
    PUBLISH_LATENCY_P95(
            Labels.PUBLISH_LATENCY,
            TestResult.PUBLISH_HISTOGRAM,
            95,
            r -> r.aggregatedPublishLatency95pct),
    PUBLISH_LATENCY_P99(
            Labels.PUBLISH_LATENCY,
            TestResult.PUBLISH_HISTOGRAM,
            99,
            r -> r.aggregatedPublishLatency99pct),
    PUBLISH_LATENCY_P999(
            Labels.PUBLISH_LATENCY,
            TestResult.PUBLISH_HISTOGRAM,
            99.9,
            r -> r.aggregatedPublishLatency999pct),
    PUBLISH_LATENCY_MAX(
            Labels.PUBLISH_LATENCY,
            TestResult.PUBLISH_HISTOGRAM,
            100,
            r -> r.aggregatedPublishLatencyMax),
    PUBLISH_DELAY_P50(
            Labels.PUBLISH_DELAY,
            TestResult.PUBLISH_DELAY_HISTOGRAM,
            50,
            r -> fromMicros(r.aggregatedPublishDelayLatency50pct)),
    PUBLISH_DELAY_P95(
            Labels.PUBLISH_DELAY,
            TestResult.PUBLISH_DELAY_HISTOGRAM,
            95,
            r -> fromMicros(r.aggregatedPublishDelayLatency95pct)),
    PUBLISH_DELAY_P99(
            Labels.PUBLISH_DELAY,
            TestResult.PUBLISH_DELAY_HISTOGRAM,
            99,
            r -> fromMicros(r.aggregatedPublishDelayLatency99pct)),
    PUBLISH_DELAY_P999(
            Labels.PUBLISH_DELAY,
            TestResult.PUBLISH_DELAY_HISTOGRAM,
            99.9,
            r -> fromMicros(r.aggregatedPublishDelayLatency999pct)),
    PUBLISH_DELAY_MAX(
            Labels.PUBLISH_DELAY,
            TestResult.PUBLISH_DELAY_HISTOGRAM,
            100,
            r -> fromMicros(r.aggregatedPublishDelayLatencyMax)),
    END_TO_END_LATENCY_P50(
            Labels.END_TO_END_LATENCY,
            TestResult.END_TO_END_HISTOGRAM,
            50,
            r -> r.aggregatedEndToEndLatency50pct),
    END_TO_END_LATENCY_P95(
            Labels.END_TO_END_LATENCY,
            TestResult.END_TO_END_HISTOGRAM,
            95,
            r -> r.aggregatedEndToEndLatency95pct),
    END_TO_END_LATENCY_P99(
            Labels.END_TO_END_LATENCY,
            TestResult.END_TO_END_HISTOGRAM,
            99,
            r -> r.aggregatedEndToEndLatency99pct),
    END_TO_END_LATENCY_P999(
            Labels.END_TO_END_LATENCY,
            TestResult.END_TO_END_HISTOGRAM,
            99.9,
            r -> r.aggregatedEndToEndLatency999pct),
    END_TO_END_LATENCY_MAX(
            Labels.END_TO_END_LATENCY,
            TestResult.END_TO_END_HISTOGRAM,
            100,
            r -> r.aggregatedEndToEndLatencyMax),
    PUBLISH_ERRORS("publish-errors", "Publish errors", Unit.COUNT, Direction.LOWER_IS_BETTER),
    MAX_BACKLOG("max-backlog", "Max backlog", Unit.COUNT, Direction.LOWER_IS_BETTER);

    /** Labels shared by the percentile metrics of one histogram. */
    private static final class Labels {
        static final String PUBLISH_LATENCY = "Publish latency";
        static final String PUBLISH_DELAY = "Publish delay";
        static final String END_TO_END_LATENCY = "End-to-end latency";

        private Labels() {}
    }

    /** Whether a larger value is an improvement. */
    enum Direction {
        HIGHER_IS_BETTER,
        LOWER_IS_BETTER
    }

    /** The unit of a metric, with the decimals it is printed with. */
    enum Unit {
        RATE("msg/s", "%,.0f"),
        MILLIS("ms", "%,.3f"),
        COUNT("", "%,.0f");

        final String symbol;

        /** The {@link String#format} pattern of a value, with thousands separators. */
        final String pattern;

        Unit(String symbol, String pattern) {
            this.symbol = symbol;
            this.pattern = pattern;
        }
    }

    final String id;
    final String label;
    final Unit unit;
    final Direction direction;

    /** The key of the raw histogram in {@code TestResult.histograms}, or null for other metrics. */
    final @Nullable String histogram;

    /** The percentile read from the raw histogram; 100 is the maximum. */
    final double percentile;

    /**
     * The value from the aggregated fields of the result, in milliseconds; null for other metrics.
     */
    final @Nullable ToDoubleFunction<TestResult> aggregated;

    Metric(String id, String label, Unit unit, Direction direction) {
        this.id = id;
        this.label = label;
        this.unit = unit;
        this.direction = direction;
        this.histogram = null;
        this.percentile = 0;
        this.aggregated = null;
    }

    /**
     * A latency percentile, with an id and label derived from the latency name, such as {@code
     * publish-latency-p99.9} and {@code Publish latency p99.9}.
     *
     * @param latency the latency name, such as {@code Publish latency}
     * @param histogram the key of the raw histogram in {@code TestResult.histograms}
     * @param percentile the percentile read from the raw histogram; 100 is the maximum
     * @param aggregated the value from the aggregated fields of the result, in milliseconds
     */
    Metric(
            String latency,
            String histogram,
            double percentile,
            ToDoubleFunction<TestResult> aggregated) {
        String suffix = percentile >= 100 ? "max" : "p" + formatPercentile(percentile);
        this.id = (latency + "-" + suffix).toLowerCase(Locale.ROOT).replace(' ', '-');
        this.label = latency + " " + suffix;
        this.unit = Unit.MILLIS;
        this.direction = Direction.LOWER_IS_BETTER;
        this.histogram = histogram;
        this.percentile = percentile;
        this.aggregated = aggregated;
    }

    /**
     * Returns the label with its unit, for table rows.
     *
     * @return the label, such as {@code Publish rate (msg/s)}
     */
    String labelWithUnit() {
        return unit.symbol.isEmpty() ? label : label + " (" + unit.symbol + ")";
    }

    // 50 as "50", 99.9 as "99.9"
    private static String formatPercentile(double percentile) {
        return percentile == Math.rint(percentile)
                ? String.valueOf((long) percentile)
                : String.valueOf(percentile);
    }

    // Publish delay is stored in microseconds; every latency is compared in milliseconds
    private static double fromMicros(long micros) {
        return micros / 1000.0;
    }
}
