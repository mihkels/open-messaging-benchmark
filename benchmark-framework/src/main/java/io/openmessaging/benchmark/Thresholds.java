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

import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Thresholds {
    public @Nullable Double maxPublishLatency99Ms;
    public @Nullable Double minThroughputRatio;

    void validate(Workload workload) {
        if (maxPublishLatency99Ms == null && minThroughputRatio == null) {
            throw new IllegalArgumentException("At least one threshold must be specified");
        }
        if (maxPublishLatency99Ms != null
                && (!Double.isFinite(maxPublishLatency99Ms) || maxPublishLatency99Ms < 0)) {
            throw new IllegalArgumentException(
                    "maxPublishLatency99Ms must be a finite non-negative number");
        }
        if (minThroughputRatio != null) {
            if (!Double.isFinite(minThroughputRatio) || minThroughputRatio < 0) {
                throw new IllegalArgumentException(
                        "minThroughputRatio must be a finite non-negative number");
            }
            if (workload.producerRate <= 0) {
                throw new IllegalArgumentException("minThroughputRatio requires a positive producerRate");
            }
        }
    }

    Result evaluate(TestResult result) {
        Map<String, Check> checks = new LinkedHashMap<>();
        if (maxPublishLatency99Ms != null) {
            checks.put(
                    "maxPublishLatency99Ms",
                    new Check(
                            maxPublishLatency99Ms,
                            result.aggregatedPublishLatency99pct,
                            result.aggregatedPublishLatency99pct <= maxPublishLatency99Ms));
        }
        if (minThroughputRatio != null) {
            double ratio =
                    result.summary.targetRate() > 0
                            ? result.summary.publishRateAvg() / result.summary.targetRate()
                            : 0;
            checks.put(
                    "minThroughputRatio", new Check(minThroughputRatio, ratio, ratio >= minThroughputRatio));
        }
        return new Result(checks);
    }

    public static final class Result {
        private static final Logger log = LoggerFactory.getLogger(Result.class);

        public String status = "";
        public Map<String, Check> checks = Map.of();

        public Result() {}

        Result(Map<String, Check> checks) {
            this.checks = checks;
            status = checks.values().stream().allMatch(Check::passed) ? "PASS" : "FAIL";
        }

        boolean passed() {
            return status.equals("PASS");
        }

        void print() {
            log.info("Thresholds: {}", status);
            log.info("| Check | Limit | Observed | Status |");
            checks.forEach((name, check) ->
                    log.info("| {} | {} | {} | {} |", name, check.limit, check.observed, check.status));
        }
    }

    public static final class Check {
        public double limit;
        public double observed;
        public String status = "";

        public Check() {}

        Check(double limit, double observed, boolean passed) {
            this.limit = limit;
            this.observed = observed;
            status = passed ? "PASS" : "FAIL";
        }

        private boolean passed() {
            return status.equals("PASS");
        }
    }
}
