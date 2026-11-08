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
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
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
    private static final String GOLDEN_V1 = "/golden/test-result-v1.json";
    // Rates depend on how long each report interval really took
    private static final List<String> TIMING_DEPENDENT =
            List.of("publishRate", "consumeRate", "publishErrorRate");
    private static final List<String> TIMING_DEPENDENT_SUMMARY =
            List.of("publishRateAvg", "consumeRateAvg", "throughputMbSec");
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
        for (String field : TIMING_DEPENDENT_SUMMARY) {
            assertThat(actual.get("summary").get(field).isNumber()).isTrue();
            ((ObjectNode) actual.get("summary")).remove(field);
        }

        boolean updateGolden = Boolean.getBoolean("golden.update");
        if (updateGolden) {
            Path golden = Path.of("src/test/resources" + GOLDEN);
            Files.createDirectories(golden.getParent());
            mapper.writerWithDefaultPrettyPrinter().writeValue(golden.toFile(), actual);
        }
        if (!updateGolden) {
            assertThat(actual).isEqualTo(golden());
        }
    }

    @Test
    void goldenResultSurvivesARoundTrip() throws Exception {
        if (Boolean.getBoolean("golden.update")) {
            return;
        }
        JsonNode golden = golden();

        TestResult result = mapper.treeToValue(golden, TestResult.class);
        assertThat(result.publishScheduleLatency99pct).isEmpty();
        assertThat(result.aggregatedPublishScheduleLatencyQuantiles).isEmpty();

        ObjectNode written = toTree(result);
        TIMING_DEPENDENT.forEach(written::remove);
        removeScheduleFields(written);
        TIMING_DEPENDENT_SUMMARY.forEach(((ObjectNode) written.get("summary"))::remove);
        assertThat(written).isEqualTo(golden);
    }

    @Test
    void readsLegacyResultWithoutVersionedBlocks() throws Exception {
        TestResult result = mapper.treeToValue(golden(GOLDEN_V1), TestResult.class);

        assertThat(result.workload).isEqualTo("golden");
        assertThat(result.aggregatedPublishLatency99pct).isEqualTo(40.0);
        assertThat(result.schemaVersion).isEqualTo(2);
    }

    @Test
    void histogramBlockContainsDecodableCompressedHistograms() throws Exception {
        CumulativeLatencies aggregate = new CumulativeLatencies();
        aggregate.publishLatency.recordValue(1_000);
        aggregate.publishDelayLatency.recordValue(500);
        aggregate.endToEndLatency.recordValue(5_000);
        TestResult result = new TestResult();

        result.recordHistogramData(aggregate);

        assertThat(result.histograms).containsOnlyKeys("publish", "publishDelay", "endToEnd");
        assertThat(decodeHistogram(result.histograms.get("publish")).getValueAtPercentile(100))
                .isEqualTo(1_000);
        assertThat(decodeHistogram(result.histograms.get("publishDelay")).getValueAtPercentile(100))
                .isEqualTo(500);
        assertThat(decodeHistogram(result.histograms.get("endToEnd")).getValueAtPercentile(100))
                .isEqualTo(5_000);
        assertThat(toTree(result).get("histograms")).isNotNull();
    }

    @Test
    void redactsCredentialLikeDriverConfigurationKeysRecursively() {
        Map<String, Object> config =
                Map.of(
                        "password", "driver-password",
                        "authToken", "driver-token",
                        "saslJaasConfig", "driver-jaas",
                        "nested", Map.of("clientSecret", "driver-secret", "username", "user"));

        Map<String, Object> redacted = ResultMetadata.redactDriverConfig(config);

        assertThat(redacted)
                .containsEntry("password", "[REDACTED]")
                .containsEntry("authToken", "[REDACTED]")
                .containsEntry("saslJaasConfig", "[REDACTED]");
        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) redacted.get("nested");
        assertThat(nested)
                .containsEntry("clientSecret", "[REDACTED]")
                .containsEntry("username", "user");
    }

    @Test
    void resultJsonRedactsCredentialsEmbeddedInStringValues() {
        Map<String, Object> config =
                Map.of(
                        "commonConfig",
                        "bootstrap.servers=kafka:9092\n"
                                + "security.protocol=SASL_SSL\n"
                                + "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule"
                                + " required username=\"bench\" password=\"kafka-secret\";\n",
                        "connectionFactoryConfigurationParam",
                        "{\"brokerURL\":\"tcp://artemis:61616\",\"user\":\"bench\",\"password\":\"jms-secret\"}",
                        "amqpUri",
                        "amqp://bench:amqp-secret@rabbit:5672/vhost",
                        "redisUrl",
                        "redis://:redis-secret@redis:6379");
        ResultMetadata metadata =
                new ResultMetadata(
                        "run", "start", "end", "1.2.3", "abc", "25", "", 1, new Workload(), config);

        String json = mapper.writeValueAsString(metadata);

        assertThat(json).doesNotContain("kafka-secret", "jms-secret", "amqp-secret", "redis-secret");
        JsonNode driverConfig = mapper.readTree(json).get("driverConfig");
        assertThat(driverConfig.get("commonConfig").asString())
                .isEqualTo(
                        "bootstrap.servers=kafka:9092\n"
                                + "security.protocol=SASL_SSL\n"
                                + "sasl.jaas.config=[REDACTED]\n");
        assertThat(mapper.readTree(driverConfig.get("connectionFactoryConfigurationParam").asString()))
                .isEqualTo(
                        mapper.readTree(
                                "{\"brokerURL\":\"tcp://artemis:61616\",\"user\":\"bench\","
                                        + "\"password\":\"[REDACTED]\"}"));
        assertThat(driverConfig.get("amqpUri").asString())
                .isEqualTo("amqp://bench:[REDACTED]@rabbit:5672/vhost");
        assertThat(driverConfig.get("redisUrl").asString()).isEqualTo("redis://:[REDACTED]@redis:6379");
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

    private static Histogram decodeHistogram(String encoded)
            throws java.util.zip.DataFormatException {
        return Histogram.decodeFromCompressedByteBuffer(
                java.nio.ByteBuffer.wrap(Base64.getDecoder().decode(encoded)), Long.MIN_VALUE);
    }

    private static JsonNode golden() throws Exception {
        return golden(GOLDEN);
    }

    private static JsonNode golden(String resource) throws Exception {
        try (InputStream input = TestResultJsonTest.class.getResourceAsStream(resource)) {
            assertThat(input).as("golden file " + resource).isNotNull();
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
        when(worker.getCountersStats()).thenReturn(new CountersStats(4, 2, 3));
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
            TestResult result = generator.printAndCollectStats(1, TimeUnit.NANOSECONDS);
            result.metadata =
                    new ResultMetadata(
                            "018f0000-0000-7000-8000-000000000001",
                            Instant.parse("2026-01-01T00:00:00Z").toString(),
                            Instant.parse("2026-01-01T00:01:00Z").toString(),
                            "1.2.3",
                            "abc123",
                            "25",
                            "",
                            1,
                            workload,
                            Map.of("name", "driver"));
            return result;
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
