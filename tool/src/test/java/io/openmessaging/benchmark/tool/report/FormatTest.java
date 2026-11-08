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

import io.openmessaging.benchmark.tool.report.Comparison.Row;
import io.openmessaging.benchmark.tool.report.MetricExtractor.LatencySource;
import io.openmessaging.benchmark.tool.report.RunContext.SettingDifference;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

class FormatTest {

    private static JsonNode row(JsonNode json, String metric) {
        for (JsonNode row : json.get("rows")) {
            if (row.get("metric").asString().equals(metric)) {
                return row;
            }
        }
        throw new AssertionError("No row " + metric);
    }

    private static DiffReport regression() {
        return DiffReport.of(
                Fixtures.runs("baseline"), Fixtures.runs("candidate-regression"), 1, List.of());
    }

    private static DiffReport single(String baseline, String candidate, List<String> warnings) {
        return DiffReport.of(
                List.of(Fixtures.run(baseline)), List.of(Fixtures.run(candidate)), 1, warnings);
    }

    @Test
    void textAlignsTheMetricTable() {
        String text = Format.TEXT.render(regression());
        assertThat(text)
                .startsWith("Benchmark comparison: kafka-1-topic\n")
                .contains("Baseline:       3 runs (baseline-1.json, baseline-2.json, baseline-3.json)")
                .contains("Result:         18 regressed, 1 unchanged")
                .contains("jdk                               27        27.0.1")
                .contains(
                        "Publish rate (msg/s)            10,000 [9,900 – 10,100]     8,500 [8,400 – 8,600]"
                                + "  -1,500  -15.0%  regressed")
                .containsPattern("Publish errors +0 +5 +\\+5 +n/a +regressed\n")
                .doesNotContain("Warning:");
        assertThat(text.lines()).allMatch(line -> line.equals(line.stripTrailing()));
    }

    @Test
    void textOmitsTheSettingsTableWhenNothingDiffers() {
        String text =
                Format.TEXT.render(single("baseline-1.json", "baseline-1.json", List.of("Careful")));
        assertThat(text)
                .contains("Baseline:       1 run (baseline-1.json)")
                .contains("\nWarning: Careful\n")
                .doesNotContain("Setting")
                .contains("19 unchanged");
    }

    @Test
    void markdownIsAGithubTableWithEmphasizedRegressions() {
        String markdown = Format.MARKDOWN.render(regression());
        assertThat(markdown)
                .startsWith("## Benchmark comparison: kafka-1-topic\n\n")
                .contains("- **Latency source:** aggregated quantiles")
                .contains("| Setting | Baseline | Candidate |\n| --- | --- | --- |\n| jdk | 27 | 27.0.1 |")
                .contains(
                        "| Metric | Baseline | Candidate | Δ | Δ% | Status |\n"
                                + "| --- | ---: | ---: | ---: | ---: | --- |")
                .contains(
                        "| Publish latency p99 (ms) | 5.000 [4.900 – 5.100] | 6.500 [6.400 – 6.600]"
                                + " | +1.500 | +30.0% | **regressed** |")
                .contains("| Max backlog | 100 | 100 | 0 | +0.0% | unchanged |");
    }

    @Test
    void markdownShowsWarningsAndKeepsCellsOnOneLine() {
        DiffReport report =
                new DiffReport(
                        "a|b",
                        List.of("x.json"),
                        List.of("y.json"),
                        LatencySource.HISTOGRAMS,
                        List.of(
                                new SettingDifference(
                                        "driverConfig.props", Set.of("acks=all\r\nlinger=1\n"), Set.of("acks=1"))),
                        List.of("Workload name differs"),
                        List.of(Comparison.row(Metric.PUBLISH_LATENCY_P99, null, Spread.of(List.of(1.0)), 1)));
        String markdown = Format.MARKDOWN.render(report);
        assertThat(markdown)
                .contains("## Benchmark comparison: a\\|b")
                .contains("> **Warning:** Workload name differs")
                .contains("- **Latency source:** raw histograms")
                .contains("| driverConfig.props | acks=all; linger=1 | acks=1 |")
                .contains("| Publish latency p99 (ms) | n/a | 1.000 | n/a | n/a | n/a |");
    }

    @Test
    void jsonHoldsRowsAndBencherMetricFormatPerSide() {
        JsonNode json = Fixtures.MAPPER.readTree(Format.JSON.render(regression()));
        assertThat(json.get("schemaVersion").asInt()).isEqualTo(JsonReport.SCHEMA_VERSION);
        assertThat(json.get("latencySource").asString()).isEqualTo("aggregated");
        assertThat(json.get("candidateFiles")).hasSize(3);
        assertThat(json.get("settingDifferences").get(0).get("name").asString()).isEqualTo("jdk");

        JsonNode rate = json.get("rows").get(0);
        assertThat(rate.get("metric").asString()).isEqualTo("publish-rate");
        assertThat(rate.get("direction").asString()).isEqualTo("higher");
        assertThat(rate.get("baseline").get("median").asDouble()).isEqualTo(10000);
        assertThat(rate.get("baseline").get("runs").asInt()).isEqualTo(3);
        assertThat(rate.get("deltaPercent").asDouble()).isEqualTo(-15);
        assertThat(rate.get("status").asString()).isEqualTo("regressed");

        JsonNode errors = row(json, "publish-errors");
        assertThat(errors.get("direction").asString()).isEqualTo("lower");
        assertThat(errors.get("deltaPercent").isNull()).isTrue();

        JsonNode measure = json.get("candidate").get("kafka-1-topic").get("publish-latency-p99");
        assertThat(measure.get("value").asDouble()).isEqualTo(6.5);
        assertThat(measure.get("lower_value").asDouble()).isEqualTo(6.4);
        assertThat(measure.get("upper_value").asDouble()).isEqualTo(6.6);
    }

    @Test
    void jsonUsesNullForValuesThatAreNotAvailable() {
        JsonNode json =
                Fixtures.MAPPER.readTree(
                        Format.JSON.render(single("legacy-v1.json", "baseline-1.json", List.of("w"))));
        assertThat(json.get("warnings").get(0).asString()).isEqualTo("w");
        JsonNode errors = row(json, "publish-errors");
        assertThat(errors.get("baseline").isNull()).isTrue();
        assertThat(errors.get("delta").isNull()).isTrue();
        assertThat(errors.get("status").asString()).isEqualTo("not_available");
        assertThat(json.get("baseline").get("kafka-1-topic").has("publish-errors")).isFalse();
        assertThat(json.get("candidate").get("kafka-1-topic").has("publish-errors")).isTrue();
    }

    @Test
    void formatsDeltasWithTheirSign() {
        Row lower =
                Comparison.row(
                        Metric.PUBLISH_LATENCY_P99, Spread.of(List.of(2.0)), Spread.of(List.of(1.0)), 1);
        assertThat(DiffReport.delta(lower)).isEqualTo("-1.000");
        assertThat(DiffReport.deltaPercent(lower)).isEqualTo("-50.0%");
    }
}
