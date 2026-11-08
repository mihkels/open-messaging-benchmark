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
import static org.assertj.core.api.Assertions.within;

import io.openmessaging.benchmark.tool.report.MetricExtractor.LatencySource;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetricExtractorTest {
    @TempDir Path tempDir;

    @Test
    void readsTheSummaryAndAggregatedLatenciesOfAVersion2File() {
        Map<Metric, Double> values =
                MetricExtractor.extract(Fixtures.run("baseline-1.json"), LatencySource.AGGREGATED);
        assertThat(values).hasSize(Metric.values().length);
        assertThat(values).containsEntry(Metric.PUBLISH_RATE, 10000.0);
        assertThat(values).containsEntry(Metric.PUBLISH_ERRORS, 0.0);
        assertThat(values).containsEntry(Metric.MAX_BACKLOG, 100.0);
        assertThat(values).containsEntry(Metric.PUBLISH_LATENCY_P99, 5.0);
        assertThat(values).containsEntry(Metric.END_TO_END_LATENCY_MAX, 30.0);
    }

    @Test
    void convertsPublishDelayFromMicrosecondsToMilliseconds() {
        Map<Metric, Double> values =
                MetricExtractor.extract(Fixtures.run("baseline-1.json"), LatencySource.AGGREGATED);
        assertThat(values).containsEntry(Metric.PUBLISH_DELAY_P99, 0.2);
        assertThat(values).containsEntry(Metric.PUBLISH_DELAY_MAX, 0.9);
    }

    @Test
    void derivesTheSummaryOfAVersion1FileFromItsSeries() {
        Run legacy = Fixtures.run("legacy-v1.json");
        assertThat(legacy.hasMetadata()).isFalse();
        Map<Metric, Double> values = MetricExtractor.extract(legacy, LatencySource.AGGREGATED);
        assertThat(values).containsEntry(Metric.PUBLISH_RATE, 10000.0);
        assertThat(values).containsEntry(Metric.CONSUME_RATE, 9990.0);
        assertThat(values).containsEntry(Metric.MAX_BACKLOG, 120.0);
        assertThat(values).doesNotContainKey(Metric.PUBLISH_ERRORS);
    }

    @Test
    void usesRawHistogramsOnlyWhenEveryRunHasAllOfThem() {
        Run withHistograms = Run.read(Fixtures.withHistograms("baseline-1.json", tempDir, 1));
        Run partial =
                Run.read(
                        Fixtures.withHistogramStrings("baseline-2.json", tempDir, Map.of("publish", "unused")));
        Run without = Fixtures.run("baseline-3.json");

        assertThat(MetricExtractor.latencySource(List.of(withHistograms)))
                .isEqualTo(LatencySource.HISTOGRAMS);
        assertThat(MetricExtractor.latencySource(List.of(withHistograms, partial)))
                .isEqualTo(LatencySource.AGGREGATED);
        assertThat(MetricExtractor.latencySource(List.of(withHistograms, without)))
                .isEqualTo(LatencySource.AGGREGATED);
    }

    @Test
    void readsLatencyPercentilesFromRawHistogramsInMilliseconds() {
        // Values 10, 20, ... 10000 µs
        Run run = Run.read(Fixtures.withHistograms("baseline-1.json", tempDir, 1));
        Map<Metric, Double> values = MetricExtractor.extract(run, LatencySource.HISTOGRAMS);
        assertThat(values.get(Metric.PUBLISH_LATENCY_P50)).isCloseTo(5.0, within(0.01));
        assertThat(values.get(Metric.PUBLISH_DELAY_P99)).isCloseTo(9.9, within(0.01));
        assertThat(values.get(Metric.END_TO_END_LATENCY_MAX)).isCloseTo(10.0, within(0.01));
        assertThat(values).containsEntry(Metric.PUBLISH_RATE, 10000.0);
    }

    @Test
    void rejectsAMissingOrInvalidHistogram() {
        Run none = Fixtures.run("baseline-1.json");
        assertThatThrownBy(() -> MetricExtractor.extract(none, LatencySource.HISTOGRAMS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("baseline-1.json has no publish histogram");

        Run partial =
                Run.read(Fixtures.withHistogramStrings("baseline-1.json", tempDir, Map.of("other", "x")));
        assertThatThrownBy(() -> MetricExtractor.extract(partial, LatencySource.HISTOGRAMS))
                .hasMessageContaining("has no publish histogram");

        Run invalid =
                Run.read(
                        Fixtures.withHistogramStrings(
                                "baseline-2.json", tempDir, Map.of("publish", "not base64!")));
        assertThatThrownBy(() -> MetricExtractor.extract(invalid, LatencySource.HISTOGRAMS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid publish histogram");
    }

    @Test
    void labelsIncludeTheUnitExceptForCounts() {
        assertThat(Metric.PUBLISH_RATE.labelWithUnit()).isEqualTo("Publish rate (msg/s)");
        assertThat(Metric.PUBLISH_LATENCY_P99.labelWithUnit()).isEqualTo("Publish latency p99 (ms)");
        assertThat(Metric.MAX_BACKLOG.labelWithUnit()).isEqualTo("Max backlog");
    }
}
