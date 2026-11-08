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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.openmessaging.benchmark.tool.report.Comparison.Row;
import io.openmessaging.benchmark.tool.report.Comparison.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ComparisonTest {
    private static final Metric LATENCY = Metric.PUBLISH_LATENCY_P99;
    private static final Metric RATE = Metric.PUBLISH_RATE;

    private static Row row(List<Row> rows, Metric metric) {
        return rows.stream().filter(row -> row.metric() == metric).findFirst().orElseThrow();
    }

    private static Spread single(double value) {
        return Spread.of(List.of(value));
    }

    private static Status status(Metric metric, Spread baseline, Spread candidate) {
        return Comparison.row(metric, baseline, candidate, 1).status();
    }

    @Test
    void spreadReportsMinMedianAndMax() {
        assertThat(Spread.of(List.of(3.0, 1.0, 2.0))).isEqualTo(new Spread(1, 2, 3, 3));
        assertThat(Spread.of(List.of(4.0, 1.0, 2.0, 3.0))).isEqualTo(new Spread(1, 2.5, 4, 4));
        assertThatThrownBy(() -> Spread.of(List.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void spreadsOverlapWhenTheirRangesShareAValue() {
        Spread middle = new Spread(2, 3, 4, 3);
        assertThat(middle.overlaps(new Spread(4, 5, 6, 3))).isTrue();
        assertThat(middle.overlaps(new Spread(5, 6, 7, 3))).isFalse();
        assertThat(middle.overlaps(new Spread(0, 1, 1.5, 3))).isFalse();
    }

    @Test
    void lowerLatencyIsAnImprovementAndHigherIsARegression() {
        assertThat(status(LATENCY, single(10), single(8))).isEqualTo(Status.IMPROVED);
        assertThat(status(LATENCY, single(10), single(12))).isEqualTo(Status.REGRESSED);
    }

    @Test
    void higherThroughputIsAnImprovementAndLowerIsARegression() {
        assertThat(status(RATE, single(1000), single(1200))).isEqualTo(Status.IMPROVED);
        assertThat(status(RATE, single(1000), single(800))).isEqualTo(Status.REGRESSED);
    }

    @Test
    void changesBelowTheMinimumAreUnchanged() {
        assertThat(status(LATENCY, single(100), single(100.5))).isEqualTo(Status.UNCHANGED);
        assertThat(status(LATENCY, single(0), single(0))).isEqualTo(Status.UNCHANGED);
        assertThat(Comparison.row(LATENCY, single(100), single(103), 5).status())
                .isEqualTo(Status.UNCHANGED);
    }

    @Test
    void deltasAreComputedFromMedians() {
        Row row = Comparison.row(LATENCY, single(10), single(12.5), 1);
        assertThat(row.delta()).isEqualTo(2.5);
        assertThat(row.deltaPercent()).isEqualTo(25);
    }

    @Test
    void aZeroBaselineHasNoRelativeDeltaButStillRegresses() {
        Row row = Comparison.row(Metric.PUBLISH_ERRORS, single(0), single(5), 1);
        assertThat(row.deltaPercent()).isNaN();
        assertThat(row.status()).isEqualTo(Status.REGRESSED);
    }

    @Test
    void overlappingRangesOfRepeatedRunsAreWithinNoise() {
        Spread baseline = new Spread(9, 10, 11, 3);
        Spread candidate = new Spread(10.5, 11.5, 12, 3);
        assertThat(status(LATENCY, baseline, candidate)).isEqualTo(Status.WITHIN_NOISE);
        assertThat(status(LATENCY, baseline, new Spread(12, 13, 14, 3))).isEqualTo(Status.REGRESSED);
    }

    @Test
    void aSingleRunOnEitherSideCannotBeJudgedAsNoise() {
        Spread repeated = new Spread(9, 10, 11, 3);
        assertThat(status(LATENCY, single(10.5), repeated)).isNotEqualTo(Status.WITHIN_NOISE);
        assertThat(status(LATENCY, repeated, single(10.5))).isNotEqualTo(Status.WITHIN_NOISE);
    }

    @Test
    void aMetricMissingOnEitherSideIsNotAvailable() {
        assertThat(Comparison.row(LATENCY, null, single(1), 1).status())
                .isEqualTo(Status.NOT_AVAILABLE);
        Row row = Comparison.row(LATENCY, single(1), null, 1);
        assertThat(row.status()).isEqualTo(Status.NOT_AVAILABLE);
        assertThat(row.delta()).isNaN();
    }

    @Test
    void compareBuildsOneRowPerMetricFromThePerRunValues() {
        List<Row> rows =
                Comparison.compare(
                        List.of(Map.of(LATENCY, 10.0), Map.of(LATENCY, 11.0)),
                        List.of(Map.of(LATENCY, 20.0, RATE, 5.0)),
                        1);
        assertThat(rows).hasSize(Metric.values().length);
        Row latency = row(rows, LATENCY);
        assertThat(latency.baseline()).isEqualTo(new Spread(10, 10.5, 11, 2));
        assertThat(latency.status()).isEqualTo(Status.REGRESSED);
        assertThat(row(rows, RATE).status()).isEqualTo(Status.NOT_AVAILABLE);
    }

    @Test
    void regressionsAreFilteredByThreshold() {
        Row small = Comparison.row(LATENCY, single(10), single(10.5), 1);
        Row large = Comparison.row(LATENCY, single(10), single(13), 1);
        Row fromZero = Comparison.row(Metric.PUBLISH_ERRORS, single(0), single(1), 1);
        Row improved = Comparison.row(LATENCY, single(10), single(5), 1);
        assertThat(Comparison.regressions(List.of(small, large, fromZero, improved), 10))
                .containsExactly(large, fromZero);
    }
}
