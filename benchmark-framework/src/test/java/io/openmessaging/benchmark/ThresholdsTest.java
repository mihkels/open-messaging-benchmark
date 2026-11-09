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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class ThresholdsTest {
    @Test
    void evaluatesAggregatedLatencyAndMeasuredThroughput() {
        Thresholds thresholds = new Thresholds();
        thresholds.maxPublishLatency99Ms = 25.0;
        thresholds.minThroughputRatio = 0.9;
        TestResult result = new TestResult();
        result.aggregatedPublishLatency99pct = 25.0;
        result.summary = new ResultSummary(80, 80, 0, 0, 0, 0, 100);

        Thresholds.Result evaluation = thresholds.evaluate(result);

        assertThat(evaluation.status()).isEqualTo(Thresholds.Status.FAIL);
        assertThat(evaluation.checks.get("maxPublishLatency99Ms").observed).isEqualTo(25.0);
        assertThat(evaluation.checks.get("maxPublishLatency99Ms").status)
                .isEqualTo(Thresholds.Status.PASS);
        assertThat(evaluation.checks.get("minThroughputRatio").observed).isEqualTo(0.8);
        assertThat(evaluation.checks.get("minThroughputRatio").status)
                .isEqualTo(Thresholds.Status.FAIL);
    }

    @Test
    void passesChecksAtTheirLimits() {
        Thresholds thresholds = new Thresholds();
        thresholds.maxPublishLatency99Ms = 25.0;
        thresholds.minThroughputRatio = 0.8;
        TestResult result = new TestResult();
        result.aggregatedPublishLatency99pct = 25.0;
        result.summary = new ResultSummary(80, 80, 0, 0, 0, 0, 100);

        assertThat(thresholds.evaluate(result).passed()).isTrue();
    }

    @Test
    void validatesConfiguredLimits() {
        Thresholds thresholds = new Thresholds();
        Workload workload = new Workload();
        workload.producerRate = 100;

        assertThatThrownBy(() -> thresholds.validate(workload))
                .hasMessage("At least one threshold must be specified");

        for (double invalid : new double[] {-1, Double.NaN, Double.POSITIVE_INFINITY}) {
            thresholds.maxPublishLatency99Ms = invalid;
            assertThatThrownBy(() -> thresholds.validate(workload))
                    .hasMessage("maxPublishLatency99Ms must be a finite non-negative number");
        }
        thresholds.maxPublishLatency99Ms = 0.0;
        thresholds.validate(workload);

        for (double invalid : new double[] {-1, Double.NaN, Double.POSITIVE_INFINITY}) {
            thresholds.minThroughputRatio = invalid;
            assertThatThrownBy(() -> thresholds.validate(workload))
                    .hasMessage("minThroughputRatio must be a finite non-negative number");
        }
        thresholds.minThroughputRatio = 0.0;
        thresholds.validate(workload);
    }

    @Test
    void evaluatesIndividualChecksAndMissingTargetRate() {
        Thresholds thresholds = new Thresholds();
        TestResult result = new TestResult();
        thresholds.maxPublishLatency99Ms = 10.0;
        result.aggregatedPublishLatency99pct = 11.0;

        Thresholds.Result latency = thresholds.evaluate(result);
        assertThat(latency.passed()).isFalse();
        assertThat(latency.checks).containsOnlyKeys("maxPublishLatency99Ms");
        assertThat(latency.checks.get("maxPublishLatency99Ms").status)
                .isEqualTo(Thresholds.Status.FAIL);

        thresholds.maxPublishLatency99Ms = null;
        thresholds.minThroughputRatio = 0.1;
        Thresholds.Result throughput = thresholds.evaluate(result);
        assertThat(throughput.passed()).isFalse();
        assertThat(throughput.checks).containsOnlyKeys("minThroughputRatio");
        assertThat(throughput.checks.get("minThroughputRatio").observed).isZero();
    }

    @Test
    void serialisesStatusStringsAndReadsThemBack() throws Exception {
        Map<String, Thresholds.Check> checks = new LinkedHashMap<>();
        checks.put("maxPublishLatency99Ms", new Thresholds.Check(10, 12, false));
        checks.put("minThroughputRatio", new Thresholds.Check(0.9, 1, true));
        ObjectMapper mapper = new ObjectMapper();

        String json = mapper.writeValueAsString(new Thresholds.Result(checks));

        assertThat(json)
                .isEqualTo(
                        "{\"checks\":{"
                                + "\"maxPublishLatency99Ms\":{\"limit\":10.0,\"observed\":12.0,\"status\":\"FAIL\"},"
                                + "\"minThroughputRatio\":{\"limit\":0.9,\"observed\":1.0,\"status\":\"PASS\"}},"
                                + "\"status\":\"FAIL\"}");
        Thresholds.Result read = mapper.readValue(json, Thresholds.Result.class);
        assertThat(read.passed()).isFalse();
        assertThat(read.checks.get("maxPublishLatency99Ms").passed()).isFalse();
        assertThat(read.checks.get("minThroughputRatio").passed()).isTrue();
        assertThat(mapper.writeValueAsString(read)).isEqualTo(json);
    }
}
