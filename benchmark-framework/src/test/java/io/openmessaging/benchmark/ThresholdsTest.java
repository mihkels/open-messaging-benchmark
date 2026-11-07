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

import org.junit.jupiter.api.Test;

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

        assertThat(evaluation.status).isEqualTo("FAIL");
        assertThat(evaluation.checks.get("maxPublishLatency99Ms").observed).isEqualTo(25.0);
        assertThat(evaluation.checks.get("maxPublishLatency99Ms").status).isEqualTo("PASS");
        assertThat(evaluation.checks.get("minThroughputRatio").observed).isEqualTo(0.8);
        assertThat(evaluation.checks.get("minThroughputRatio").status).isEqualTo("FAIL");
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
}
