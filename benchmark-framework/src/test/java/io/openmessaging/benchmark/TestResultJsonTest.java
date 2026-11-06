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
package io.openmessaging.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Pins the result JSON documented in docs/reference/results-schema.md and read by the reporting
 * scripts. Run with {@code -Dgolden.update=true} to rewrite the golden file after an intended
 * schema change.
 */
class TestResultJsonTest {
    private static final String GOLDEN = "/golden/test-result.json";
    // Rates depend on how long each report interval really took
    private static final List<String> TIMING_DEPENDENT =
            List.of("publishRate", "consumeRate", "publishErrorRate");
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final List<String> SCHEDULE_SUFFIXES =
            List.of("Avg", "50pct", "75pct", "95pct", "99pct", "999pct", "9999pct", "Max");

    @Test
    void resultJsonMatchesTheDocumentedSchema() throws Exception {
        ObjectNode actual = toTree(collectResult());
        for (String field : TIMING_DEPENDENT) {
            assertThat(actual.get(field)).hasSize(1);
            actual.remove(field);
        }
        for (String suffix : SCHEDULE_SUFFIXES) {
            assertThat(actual.get("publishScheduleLatency" + suffix).get(0).doubleValue()).isEqualTo(1.2);
            assertThat(actual.get("aggregatedPublishScheduleLatency" + suffix).doubleValue())
                    .isEqualTo(1.2);
        }
        assertThat(actual.get("aggregatedPublishScheduleLatencyQuantiles").get("100.0").doubleValue())
                .isEqualTo(1.2);
        removeScheduleFields(actual);

        if (Boolean.getBoolean("golden.update")) {
            Path golden = Path.of("src/test/resources" + GOLDEN);
            Files.createDirectories(golden.getParent());
            mapper.writerWithDefaultPrettyPrinter().writeValue(golden.toFile(), actual);
        }
        assertThat(actual).isEqualTo(golden());
    }

    @Test
    void goldenResultSurvivesARoundTrip() throws Exception {
        JsonNode golden = golden();

        TestResult result = mapper.treeToValue(golden, TestResult.class);
        assertThat(result.publishScheduleLatency99pct).isEmpty();
        assertThat(result.aggregatedPublishScheduleLatencyQuantiles).isEmpty();

        ObjectNode written = toTree(result);
        TIMING_DEPENDENT.forEach(written::remove);
        removeScheduleFields(written);
        assertThat(written).isEqualTo(golden);
    }

    private static void removeScheduleFields(ObjectNode json) {
        for (String suffix : SCHEDULE_SUFFIXES) {
            json.remove("publishScheduleLatency" + suffix);
            json.remove("aggregatedPublishScheduleLatency" + suffix);
        }
        json.remove("aggregatedPublishScheduleLatencyQuantiles");
    }

    // Writes and re-reads the value, so that number node types match those parsed from the file
    private static ObjectNode toTree(Object value) {
        return (ObjectNode) mapper.readTree(mapper.writeValueAsString(value));
    }

    private static JsonNode golden() throws Exception {
        try (InputStream input = TestResultJsonTest.class.getResourceAsStream(GOLDEN)) {
            assertThat(input).as("golden file " + GOLDEN).isNotNull();
            return mapper.readTree(input);
        }
    }

    private static TestResult collectResult() throws Exception {
        Histogram publish = histogram(60_000_000L, 1_000, 2_000, 3_000, 40_000);
        Histogram delay = histogram(60_000_000L, 100, 500, 900);
        Histogram endToEnd = histogram(43_200_000_000L, 5_000, 7_000, 11_000);
        Histogram schedule = histogram(60_000_000L, 1_200);
        PeriodStats period =
                new PeriodStats(100, 2, 1_000, 90, 900, 400, 3, 380, publish, delay, endToEnd, schedule);
        CumulativeLatencies cumulative = new CumulativeLatencies();
        cumulative.publishLatency.add(publish);
        cumulative.publishDelayLatency.add(delay);
        cumulative.endToEndLatency.add(endToEnd);
        cumulative.publishScheduleLatency.add(schedule);

        Worker worker = mock(Worker.class);
        when(worker.getPeriodStats()).thenReturn(period);
        when(worker.getCumulativeLatencies()).thenReturn(cumulative);
        Workload workload = new Workload();
        workload.name = "golden";
        workload.topics = 2;
        workload.partitionsPerTopic = 3;
        workload.messageSize = 1024;
        workload.producersPerTopic = 1;
        workload.consumerPerSubscription = 2;
        workload.subscriptionsPerTopic = 1;
        try (WorkloadGenerator generator =
                new WorkloadGenerator(
                        "driver",
                        workload,
                        worker,
                        Duration.ofMillis(5),
                        Duration.ofMillis(5),
                        Duration.ofSeconds(2))) {
            return generator.printAndCollectStats(1, TimeUnit.NANOSECONDS);
        }
    }

    private static Histogram histogram(long highest, long... values) {
        Histogram histogram = new Histogram(highest, 5);
        for (long value : values) {
            histogram.recordValue(value);
        }
        return histogram;
    }
}
