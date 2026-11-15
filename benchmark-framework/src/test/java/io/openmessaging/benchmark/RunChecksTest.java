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
import static org.assertj.core.api.Assertions.within;

import io.openmessaging.benchmark.Finding.Severity;
import io.openmessaging.benchmark.worker.commands.RunPhase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class RunChecksTest {
    private static final int PERIODS = 12;

    @Test
    void healthyRunHasNoFindings() {
        assertThat(new RunChecks().evaluate(workload(), healthy())).isEmpty();
    }

    @Test
    void flagsRateShortfallForFixedRateWorkloads() {
        TestResult result = healthy();
        result.summary = summary(600, 1_000_000, 0);

        Finding finding = only(new RunChecks().evaluate(workload(), result));

        assertThat(finding.check()).isEqualTo("rateShortfall");
        assertThat(finding.severity()).isEqualTo(Severity.INVALID);
        assertThat(finding.value()).isEqualTo(0.4, within(1e-9));
        assertThat(finding.limit()).isEqualTo(0.05);
    }

    @Test
    void flagsRetriedRunWithEarlierFailures() {
        TestResult result = healthy();
        result.attempts = 3;
        result.failedAttempts = List.of("worker timed out", "connection refused");

        Finding finding = only(new RunChecks().evaluate(workload(), result));

        assertThat(finding.check()).isEqualTo("retried");
        assertThat(finding.severity()).isEqualTo(Severity.WARNING);
        assertThat(finding.message()).contains("3 attempts", "worker timed out; connection refused");
        assertThat(Validity.of(List.of(finding)).status()).isEqualTo(Validity.Status.WARNING);
    }

    @Test
    void verdictIsTheWorstFindingSeverity() {
        Finding info = new Finding("tailSamples", Severity.INFO, "few", null, null);
        Finding warning = new Finding("shortWindow", Severity.WARNING, "short", null, null);
        Finding invalid = new Finding("rateShortfall", Severity.INVALID, "slow", null, null);

        assertThat(Validity.of(new RunChecks().evaluate(workload(), healthy())).status())
                .isEqualTo(Validity.Status.VALID);
        assertThat(Validity.of(List.of(info)).status()).isEqualTo(Validity.Status.VALID);
        assertThat(Validity.of(List.of(info, warning)).status()).isEqualTo(Validity.Status.WARNING);
        assertThat(Validity.of(List.of(invalid, warning)).findings()).containsExactly(invalid, warning);
        assertThat(Validity.of(List.of(warning, invalid, info)).status())
                .isEqualTo(Validity.Status.INVALID);
    }

    @Test
    void skipsRateShortfallWithoutFixedRate() {
        Workload workload = workload();
        workload.producerRate = 0;
        TestResult result = healthy();
        result.summary = summary(600, 1_000_000, 0);

        assertThat(new RunChecks().evaluate(workload, result)).isEmpty();
    }

    @Test
    void flagsBacklogGrowingThroughTheMeasuredWindow() {
        TestResult result = healthy();
        for (int i = 0; i < result.backlog.size(); i++) {
            result.backlog.set(i, i * 5_000L);
        }

        Finding finding = only(new RunChecks().evaluate(workload(), result));

        assertThat(finding.check()).isEqualTo("backlogGrowth");
        assertThat(finding.value()).isEqualTo(50.0); // 50k messages at 1000 msg/s; warm-up excluded
    }

    @Test
    void skipsBacklogGrowthForPauseAndBacklogWorkloads() {
        TestResult result = healthy();
        for (int i = 0; i < result.backlog.size(); i++) {
            result.backlog.set(i, i * 5_000L);
        }
        Workload paused = workload();
        paused.consumerPause = new Workload.ConsumerPause(1, 1);
        Workload backlog = workload();
        backlog.consumerBacklogSizeGB = 1;

        assertThat(new RunChecks().evaluate(paused, result)).isEmpty();
        assertThat(new RunChecks().evaluate(backlog, result)).isEmpty();
    }

    @Test
    void flagsShortMeasuredWindow() {
        Workload workload = workload();
        workload.testDurationMinutes = 2;

        Finding finding = only(new RunChecks().evaluate(workload, healthy()));

        assertThat(finding.check()).isEqualTo("shortWindow");
        assertThat(finding.value()).isEqualTo(2.0);
        assertThat(finding.limit()).isEqualTo(5.0);
    }

    @Test
    void flagsTooFewSamplesForTheLowestUnreliablePercentile() {
        TestResult result = healthy();
        result.summary = summary(1000, 5_000, 0);

        Finding finding = only(new RunChecks().evaluate(workload(), result));

        assertThat(finding.check()).isEqualTo("tailSamples");
        assertThat(finding.severity()).isEqualTo(Severity.WARNING);
        assertThat(finding.message()).contains("p99.9");
        assertThat(finding.limit()).isEqualTo(10_000.0);
    }

    @Test
    void tooFewSamplesForP9999AloneIsInfo() {
        TestResult result = healthy();
        result.summary = summary(1000, 50_000, 0);

        Finding finding = only(new RunChecks().evaluate(workload(), result));

        assertThat(finding.severity()).isEqualTo(Severity.INFO);
        assertThat(finding.message()).contains("p99.99");
    }

    @Test
    void flagsSendErrorsWithTheReasonOfEachWorker() {
        TestResult result = healthy();
        result.summary = summary(1000, 1_000_000, 3);
        result.sendErrorReasons.put("worker-2", "TimeoutException: expired");
        result.sendErrorReasons.put("worker-1", "broker unavailable");

        Finding finding = only(new RunChecks().evaluate(workload(), result));

        assertThat(finding.check()).isEqualTo("sendErrors");
        assertThat(finding.value()).isEqualTo(3.0);
        assertThat(finding.message())
                .contains("3 sends failed")
                .contains(
                        "worker worker-1: broker unavailable; worker worker-2: TimeoutException: expired");
    }

    @Test
    void flagsGrowingPublishDelay() {
        TestResult result = healthy();
        for (int i = 0; i < result.publishDelayLatencyAvg.size(); i++) {
            result.publishDelayLatencyAvg.set(i, i * 10_000.0); // +10 ms per period
        }

        Finding finding = only(new RunChecks().evaluate(workload(), result));

        assertThat(finding.check()).isEqualTo("publishDelayGrowth");
        assertThat(finding.value()).isEqualTo(60.0);
    }

    @Test
    void disabledChecksStayQuiet() {
        Workload workload = workload();
        workload.testDurationMinutes = 2;
        TestResult result = healthy();
        result.summary = summary(600, 5_000, 3);
        RunChecks checks = new RunChecks();
        checks.disabled = List.of("rateShortfall", "shortWindow", "tailSamples");

        assertThat(checks.evaluate(workload, result))
                .extracting(Finding::check)
                .containsExactly("sendErrors");
    }

    @Test
    void limitsAreConfigurable() {
        TestResult result = healthy();
        result.summary = summary(900, 1_000_000, 3);
        RunChecks checks = new RunChecks();
        checks.maxRateShortfall = 0.2;
        checks.maxSendErrors = 5;

        assertThat(checks.evaluate(workload(), result)).isEmpty();
    }

    @Test
    void rejectsUnknownAndNegativeSettings() {
        Workload workload = workload();
        workload.checks = new RunChecks();
        workload.checks.disabled = List.of("nope");
        assertThatThrownBy(workload::validate).hasMessageContaining("Unknown check 'nope'");

        workload.checks = new RunChecks();
        workload.checks.maxRateShortfall = -1;
        assertThatThrownBy(workload::validate).isInstanceOf(IllegalArgumentException.class);

        workload.checks = new RunChecks();
        workload.checks.minTailSamples = -1;
        assertThatThrownBy(workload::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findingsRoundTripWithLowercaseSeverity() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TestResult result = new TestResult();
        result.findings =
                List.of(new Finding("sendErrors", Severity.WARNING, "3 sends failed", 3.0, 0.0));
        result.sendErrorReasons.put("worker-1", "boom");

        String json = mapper.writeValueAsString(result);
        TestResult read = mapper.readValue(json, TestResult.class);

        assertThat(json).contains("\"severity\":\"warning\"").doesNotContain("sendErrorReasons");
        assertThat(read.findings).isEqualTo(result.findings);
        assertThat(mapper.writeValueAsString(new TestResult())).doesNotContain("findings");
    }

    private static Finding only(List<Finding> findings) {
        assertThat(findings).hasSize(1);
        return findings.get(0);
    }

    private static Workload workload() {
        Workload workload = new Workload();
        workload.producerRate = 1000;
        workload.testDurationMinutes = 15;
        workload.subscriptionsPerTopic = 1;
        return workload;
    }

    // One warm-up period with a large backlog, then a steady measured window
    private static TestResult healthy() {
        TestResult result = new TestResult();
        result.phase = new ArrayList<>(List.of(RunPhase.WARMUP));
        result.phase.addAll(Collections.nCopies(PERIODS - 1, RunPhase.MEASURE));
        result.backlog = new ArrayList<>(List.of(90_000L));
        result.backlog.addAll(Collections.nCopies(PERIODS - 1, 20L));
        result.publishDelayLatencyAvg = new ArrayList<>(Collections.nCopies(PERIODS, 150.0));
        result.summary = summary(1000, 1_000_000, 0);
        return result;
    }

    private static ResultSummary summary(double publishRate, long sent, long errors) {
        return new ResultSummary(publishRate, publishRate, 1, sent, errors, 20, 1000);
    }
}
