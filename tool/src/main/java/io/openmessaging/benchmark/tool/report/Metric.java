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
import java.util.function.ToDoubleFunction;
import org.jspecify.annotations.Nullable;

/** A compared value of a run, in the order it is reported. */
enum Metric {
    PUBLISH_RATE("publish-rate", "Publish rate", Unit.RATE, Direction.HIGHER_IS_BETTER),
    CONSUME_RATE("consume-rate", "Consume rate", Unit.RATE, Direction.HIGHER_IS_BETTER),
    PUBLISH_LATENCY_P50(
            "publish-latency-p50",
            "Publish latency p50",
            "publish",
            50,
            r -> r.aggregatedPublishLatency50pct),
    PUBLISH_LATENCY_P95(
            "publish-latency-p95",
            "Publish latency p95",
            "publish",
            95,
            r -> r.aggregatedPublishLatency95pct),
    PUBLISH_LATENCY_P99(
            "publish-latency-p99",
            "Publish latency p99",
            "publish",
            99,
            r -> r.aggregatedPublishLatency99pct),
    PUBLISH_LATENCY_P999(
            "publish-latency-p99.9",
            "Publish latency p99.9",
            "publish",
            99.9,
            r -> r.aggregatedPublishLatency999pct),
    PUBLISH_LATENCY_MAX(
            "publish-latency-max",
            "Publish latency max",
            "publish",
            100,
            r -> r.aggregatedPublishLatencyMax),
    PUBLISH_DELAY_P50(
            "publish-delay-p50",
            "Publish delay p50",
            "publishDelay",
            50,
            r -> fromMicros(r.aggregatedPublishDelayLatency50pct)),
    PUBLISH_DELAY_P95(
            "publish-delay-p95",
            "Publish delay p95",
            "publishDelay",
            95,
            r -> fromMicros(r.aggregatedPublishDelayLatency95pct)),
    PUBLISH_DELAY_P99(
            "publish-delay-p99",
            "Publish delay p99",
            "publishDelay",
            99,
            r -> fromMicros(r.aggregatedPublishDelayLatency99pct)),
    PUBLISH_DELAY_P999(
            "publish-delay-p99.9",
            "Publish delay p99.9",
            "publishDelay",
            99.9,
            r -> fromMicros(r.aggregatedPublishDelayLatency999pct)),
    PUBLISH_DELAY_MAX(
            "publish-delay-max",
            "Publish delay max",
            "publishDelay",
            100,
            r -> fromMicros(r.aggregatedPublishDelayLatencyMax)),
    END_TO_END_LATENCY_P50(
            "end-to-end-latency-p50",
            "End-to-end latency p50",
            "endToEnd",
            50,
            r -> r.aggregatedEndToEndLatency50pct),
    END_TO_END_LATENCY_P95(
            "end-to-end-latency-p95",
            "End-to-end latency p95",
            "endToEnd",
            95,
            r -> r.aggregatedEndToEndLatency95pct),
    END_TO_END_LATENCY_P99(
            "end-to-end-latency-p99",
            "End-to-end latency p99",
            "endToEnd",
            99,
            r -> r.aggregatedEndToEndLatency99pct),
    END_TO_END_LATENCY_P999(
            "end-to-end-latency-p99.9",
            "End-to-end latency p99.9",
            "endToEnd",
            99.9,
            r -> r.aggregatedEndToEndLatency999pct),
    END_TO_END_LATENCY_MAX(
            "end-to-end-latency-max",
            "End-to-end latency max",
            "endToEnd",
            100,
            r -> r.aggregatedEndToEndLatencyMax),
    PUBLISH_ERRORS("publish-errors", "Publish errors", Unit.COUNT, Direction.LOWER_IS_BETTER),
    MAX_BACKLOG("max-backlog", "Max backlog", Unit.COUNT, Direction.LOWER_IS_BETTER);

    /** Whether a larger value is an improvement. */
    enum Direction {
        HIGHER_IS_BETTER,
        LOWER_IS_BETTER
    }

    /** The unit of a metric, with the decimals it is printed with. */
    enum Unit {
        RATE("msg/s", 0),
        MILLIS("ms", 3),
        COUNT("", 0);

        final String symbol;
        final int decimals;

        Unit(String symbol, int decimals) {
            this.symbol = symbol;
            this.decimals = decimals;
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

    Metric(
            String id,
            String label,
            String histogram,
            double percentile,
            ToDoubleFunction<TestResult> aggregated) {
        this.id = id;
        this.label = label;
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

    // Publish delay is stored in microseconds; every latency is compared in milliseconds
    private static double fromMicros(long micros) {
        return micros / 1000.0;
    }
}
