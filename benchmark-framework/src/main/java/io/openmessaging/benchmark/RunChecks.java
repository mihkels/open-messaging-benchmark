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

import io.openmessaging.benchmark.Finding.Severity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in checks over a completed result that flag runs which finished but may not be a valid
 * measurement. Every limit has a default, so a workload only sets {@code checks} to tune or {@link
 * #disabled disable} them. Failures that abort a run, such as stalls, are not repeated here.
 */
public class RunChecks {
    static final String RATE_SHORTFALL = "rateShortfall";
    static final String BACKLOG_GROWTH = "backlogGrowth";
    static final String SHORT_WINDOW = "shortWindow";
    static final String TAIL_SAMPLES = "tailSamples";
    static final String SEND_ERRORS = "sendErrors";
    static final String PUBLISH_DELAY_GROWTH = "publishDelayGrowth";
    static final List<String> NAMES =
            List.of(
                    RATE_SHORTFALL,
                    BACKLOG_GROWTH,
                    SHORT_WINDOW,
                    TAIL_SAMPLES,
                    SEND_ERRORS,
                    PUBLISH_DELAY_GROWTH);

    // Reported tail percentiles and the inverse of the fraction of samples above each
    private static final List<String> TAIL_PERCENTILES = List.of("p99", "p99.9", "p99.99");
    private static final long[] TAIL_INVERSE_FRACTIONS = {100, 1_000, 10_000};

    private static final Logger log = LoggerFactory.getLogger(RunChecks.class);

    /** Names of checks to skip. */
    public List<String> disabled = List.of();

    /** Largest tolerated fraction by which the average publish rate may fall short of the target. */
    public double maxRateShortfall = 0.05;

    /** Largest tolerated backlog growth over the measured window, in seconds of publishing. */
    public double maxBacklogGrowthSeconds = 10;

    /** Shortest measured window, in minutes, that is not reported as short. */
    public int minMeasuredMinutes = 5;

    /** Fewest samples beyond a reported percentile for it to be trusted. */
    public long minTailSamples = 10;

    /** Most failed sends tolerated in the measured window. */
    public long maxSendErrors = 0;

    /** Largest tolerated rise in average publish delay from the first to the second half, in ms. */
    public double maxPublishDelayGrowthMs = 10;

    void validate() {
        for (String name : disabled) {
            if (!NAMES.contains(name)) {
                throw new IllegalArgumentException(
                        "Unknown check '" + name + "' in checks.disabled; known checks: " + NAMES);
            }
        }
        if (!(maxRateShortfall >= 0 && maxBacklogGrowthSeconds >= 0 && maxPublishDelayGrowthMs >= 0)
                || !Double.isFinite(maxRateShortfall + maxBacklogGrowthSeconds + maxPublishDelayGrowthMs)) {
            throw new IllegalArgumentException("checks limits must be finite non-negative numbers");
        }
        if (minMeasuredMinutes < 0 || minTailSamples < 0 || maxSendErrors < 0) {
            throw new IllegalArgumentException("checks limits must not be negative");
        }
    }

    /**
     * Runs every enabled check.
     *
     * @param workload the workload that produced the result
     * @param result the completed result, with its summary recorded
     * @return the findings, empty for a healthy run
     */
    List<Finding> evaluate(Workload workload, TestResult result) {
        List<Finding> findings = new ArrayList<>();
        ResultSummary summary = result.summary;
        rateShortfall(workload, summary, findings);
        backlogGrowth(workload, result, summary, findings);
        shortWindow(workload, findings);
        tailSamples(summary, findings);
        sendErrors(summary, result.sendErrorReasons, findings);
        publishDelayGrowth(result, findings);
        return findings;
    }

    private void rateShortfall(Workload workload, ResultSummary summary, List<Finding> findings) {
        if (enabled(RATE_SHORTFALL)
                && workload.producerRate > 0
                && !workload.usesStages()
                && summary.targetRate() > 0) {
            double shortfall = 1 - summary.publishRateAvg() / summary.targetRate();
            if (shortfall > maxRateShortfall) {
                findings.add(
                        new Finding(
                                RATE_SHORTFALL,
                                Severity.WARNING,
                                String.format(
                                        "Average publish rate %.1f msg/s is %.1f%% below the target %.1f msg/s",
                                        summary.publishRateAvg(), shortfall * 100, summary.targetRate()),
                                shortfall,
                                maxRateShortfall));
            }
        }
    }

    private void backlogGrowth(
            Workload workload, TestResult result, ResultSummary summary, List<Finding> findings) {
        if (enabled(BACKLOG_GROWTH)
                && workload.consumerBacklogSizeGB == 0
                && workload.consumerPause == null
                && summary.publishRateAvg() > 0) {
            List<Long> backlog = TestResult.measured(result.phase, result.backlog);
            if (backlog.size() >= 2) {
                long growth = backlog.get(backlog.size() - 1) - backlog.get(0);
                double seconds = growth / summary.publishRateAvg();
                if (seconds > maxBacklogGrowthSeconds) {
                    findings.add(
                            new Finding(
                                    BACKLOG_GROWTH,
                                    Severity.WARNING,
                                    String.format(
                                            "Backlog grew by %d messages (%.1f s of publishing) during the measured"
                                                    + " window; consumers did not keep up",
                                            growth, seconds),
                                    seconds,
                                    maxBacklogGrowthSeconds));
                }
            }
        }
    }

    private void shortWindow(Workload workload, List<Finding> findings) {
        int minutes = workload.measuredMinutes();
        if (enabled(SHORT_WINDOW) && minutes < minMeasuredMinutes) {
            findings.add(
                    new Finding(
                            SHORT_WINDOW,
                            Severity.WARNING,
                            String.format(
                                    "Measured window of %d min is shorter than %d min", minutes, minMeasuredMinutes),
                            (double) minutes,
                            (double) minMeasuredMinutes));
        }
    }

    private void tailSamples(ResultSummary summary, List<Finding> findings) {
        if (enabled(TAIL_SAMPLES)) {
            long samples = summary.totalMessagesSent();
            for (int i = 0; i < TAIL_PERCENTILES.size(); i++) {
                String percentile = TAIL_PERCENTILES.get(i);
                long required = minTailSamples * TAIL_INVERSE_FRACTIONS[i];
                if (samples < required) {
                    findings.add(
                            new Finding(
                                    TAIL_SAMPLES,
                                    // p99.99 alone is rarely the headline number
                                    "p99.99".equals(percentile) ? Severity.INFO : Severity.WARNING,
                                    String.format(
                                            "%d samples are too few for %s and above; it needs %d",
                                            samples, percentile, required),
                                    (double) samples,
                                    (double) required));
                    break;
                }
            }
        }
    }

    private void sendErrors(
            ResultSummary summary, Map<String, String> sendErrorReasons, List<Finding> findings) {
        if (enabled(SEND_ERRORS) && summary.totalErrors() > maxSendErrors) {
            String reasons =
                    sendErrorReasons.entrySet().stream()
                            .map(e -> "worker " + e.getKey() + ": " + e.getValue())
                            .collect(Collectors.joining("; "));
            findings.add(
                    new Finding(
                            SEND_ERRORS,
                            Severity.WARNING,
                            summary.totalErrors()
                                    + " sends failed in the measured window"
                                    + (reasons.isEmpty() ? "" : "; last reasons: " + reasons),
                            (double) summary.totalErrors(),
                            (double) maxSendErrors));
        }
    }

    private void publishDelayGrowth(TestResult result, List<Finding> findings) {
        if (enabled(PUBLISH_DELAY_GROWTH)) {
            List<Double> delay = TestResult.measured(result.phase, result.publishDelayLatencyAvg);
            if (delay.size() >= 2) {
                int half = delay.size() / 2;
                double growthMs =
                        (TestResult.average(delay.subList(delay.size() - half, delay.size()))
                                        - TestResult.average(delay.subList(0, half)))
                                / 1000.0;
                if (growthMs > maxPublishDelayGrowthMs) {
                    findings.add(
                            new Finding(
                                    PUBLISH_DELAY_GROWTH,
                                    Severity.WARNING,
                                    String.format(
                                            "Average publish delay rose by %.1f ms from the first to the second half"
                                                    + " of the measured window; producers fell behind their schedule",
                                            growthMs),
                                    growthMs,
                                    maxPublishDelayGrowthMs));
                }
            }
        }
    }

    private boolean enabled(String check) {
        return !disabled.contains(check);
    }

    static void print(List<Finding> findings) {
        if (findings.isEmpty()) {
            log.info("Run checks: no findings");
        }
        findings.forEach(f -> log.warn("Run check {} ({}): {}", f.check(), f.severity(), f.message()));
    }
}
