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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Thresholds {
    public @Nullable Double maxPublishLatency99Ms;
    public @Nullable Double minThroughputRatio;

    /** Most lost messages allowed; needs verifyDelivery. */
    public @Nullable Long maxLostMessages;

    /** Most duplicated messages allowed; needs verifyDelivery. */
    public @Nullable Long maxDuplicatedMessages;

    void validate(Workload workload) {
        if (maxPublishLatency99Ms == null
                && minThroughputRatio == null
                && maxLostMessages == null
                && maxDuplicatedMessages == null) {
            throw new IllegalArgumentException("At least one threshold must be specified");
        }
        validateDeliveryLimit("maxLostMessages", maxLostMessages, workload);
        validateDeliveryLimit("maxDuplicatedMessages", maxDuplicatedMessages, workload);
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

    private static void validateDeliveryLimit(String name, @Nullable Long limit, Workload workload) {
        if (limit == null) {
            return;
        }
        if (limit < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        if (!workload.verifyDelivery) {
            throw new IllegalArgumentException(name + " requires verifyDelivery: true");
        }
    }

    Result evaluate(TestResult result) {
        Result evaluated =
                evaluate(
                        result.aggregatedPublishLatency99pct,
                        result.summary.totalMessagesSent(),
                        result.summary.publishRateAvg(),
                        result.summary.targetRate());
        evaluateDelivery(evaluated, Collections.singletonList(result.delivery));
        return evaluated;
    }

    // Adds the delivery checks on the worst count; repeated runs pass every repetition's delivery
    void evaluateDelivery(Result evaluated, List<@Nullable Delivery> deliveries) {
        // No delivery counts means verification did not finish, which must not pass
        boolean complete = deliveries.stream().allMatch(Objects::nonNull);
        List<Delivery> present = deliveries.stream().filter(Objects::nonNull).toList();
        if (maxLostMessages != null) {
            long lost = present.stream().mapToLong(Delivery::lost).max().orElse(0);
            evaluated.checks.put(
                    "maxLostMessages",
                    new Check(maxLostMessages, (double) lost, complete && lost <= maxLostMessages));
        }
        if (maxDuplicatedMessages != null) {
            long duplicated = present.stream().mapToLong(Delivery::duplicated).max().orElse(0);
            evaluated.checks.put(
                    "maxDuplicatedMessages",
                    new Check(
                            maxDuplicatedMessages,
                            (double) duplicated,
                            complete && duplicated <= maxDuplicatedMessages));
        }
    }

    // Also used with the medians of repeated runs
    Result evaluate(
            double publishLatency99Ms, long totalMessagesSent, double publishRateAvg, double targetRate) {
        Map<String, Check> checks = new LinkedHashMap<>();
        if (maxPublishLatency99Ms != null) {
            checks.put(
                    "maxPublishLatency99Ms",
                    new Check(
                            maxPublishLatency99Ms,
                            publishLatency99Ms,
                            // An empty publish histogram reports p99 = 0, so nothing sent must not pass
                            totalMessagesSent > 0 && publishLatency99Ms <= maxPublishLatency99Ms));
        }
        if (minThroughputRatio != null) {
            double ratio = targetRate > 0 ? publishRateAvg / targetRate : 0;
            checks.put(
                    "minThroughputRatio", new Check(minThroughputRatio, ratio, ratio >= minThroughputRatio));
        }
        return new Result(checks);
    }

    public enum Status {
        PASS,
        FAIL;

        static Status of(boolean passed) {
            return passed ? PASS : FAIL;
        }
    }

    public static final class Result {
        private static final Logger log = LoggerFactory.getLogger(Result.class);

        public Map<String, Check> checks = Map.of();

        public Result() {}

        Result(Map<String, Check> checks) {
            this.checks = checks;
        }

        boolean passed() {
            return checks.values().stream().allMatch(Check::passed);
        }

        @JsonProperty(value = "status", access = JsonProperty.Access.READ_ONLY)
        Status status() {
            return Status.of(passed());
        }

        void print() {
            if (!log.isInfoEnabled()) {
                return;
            }
            log.info("Thresholds: {}", status());
            log.info("| Check | Limit | Observed | Status |");
            checks.forEach((name, check) ->
                    log.info("| {} | {} | {} | {} |", name, check.limit, check.observed, check.status));
        }
    }

    public static final class Check {
        public double limit;
        public double observed;
        public Status status = Status.FAIL;

        public Check() {}

        Check(double limit, double observed, boolean passed) {
            this.limit = limit;
            this.observed = observed;
            this.status = Status.of(passed);
        }

        boolean passed() {
            return status == Status.PASS;
        }
    }
}
