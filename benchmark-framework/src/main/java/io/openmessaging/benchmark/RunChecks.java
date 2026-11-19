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
import io.openmessaging.benchmark.worker.WorkerClock;
import io.openmessaging.benchmark.worker.commands.WorkerLoad;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
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
    static final String RETRIED = "retried";
    static final String CLOCK_OFFSET = "clockOffset";
    static final String RUN_TO_RUN_VARIANCE = "runToRunVariance";
    static final String GENERATOR_SATURATION = "generatorSaturation";
    static final String DELIVERY = "delivery";
    static final List<String> NAMES =
            List.of(
                    RATE_SHORTFALL,
                    BACKLOG_GROWTH,
                    SHORT_WINDOW,
                    TAIL_SAMPLES,
                    SEND_ERRORS,
                    PUBLISH_DELAY_GROWTH,
                    RETRIED,
                    CLOCK_OFFSET,
                    RUN_TO_RUN_VARIANCE,
                    GENERATOR_SATURATION,
                    DELIVERY);

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

    /**
     * Largest tolerated clock-offset spread between producer and consumer workers, as a fraction of
     * the p50 end-to-end latency.
     */
    public double maxClockOffsetFraction = 0.1;

    /** Largest tolerated coefficient of variation of a metric across {@code --repeat} runs. */
    public double maxRunToRunCv = 0.1;

    /** Highest tolerated worker CPU use in a measured period, as a fraction of its cores. */
    public double maxWorkerCpu = 0.85;

    /** Highest tolerated worker GC pause time in a measured period, as a fraction of the period. */
    public double maxWorkerGc = 0.1;

    void validate() {
        for (String name : disabled) {
            if (!NAMES.contains(name)) {
                throw new IllegalArgumentException(
                        "Unknown check '" + name + "' in checks.disabled; known checks: " + NAMES);
            }
        }
        if (!(maxRateShortfall >= 0
                        && maxBacklogGrowthSeconds >= 0
                        && maxPublishDelayGrowthMs >= 0
                        && maxClockOffsetFraction >= 0
                        && maxRunToRunCv >= 0
                        && maxWorkerCpu >= 0
                        && maxWorkerGc >= 0)
                || !Double.isFinite(
                        maxRateShortfall
                                + maxBacklogGrowthSeconds
                                + maxPublishDelayGrowthMs
                                + maxClockOffsetFraction
                                + maxRunToRunCv
                                + maxWorkerCpu
                                + maxWorkerGc)) {
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
        retried(result, findings);
        clockOffset(result, findings);
        generatorSaturation(result, findings);
        delivery(workload, result.delivery, findings);
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
                                // the run did not measure the load it was asked to
                                Severity.INVALID,
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
                long growth = backlog.getLast() - backlog.getFirst();
                // Backlog holds one copy per subscription
                double seconds =
                        growth / (summary.publishRateAvg() * Math.max(1, workload.subscriptionsPerTopic));
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

    private void retried(TestResult result, List<Finding> findings) {
        if (enabled(RETRIED) && result.attempts != null && result.attempts > 1) {
            findings.add(
                    new Finding(
                            RETRIED,
                            Severity.WARNING,
                            "The run needed "
                                    + result.attempts
                                    + " attempts; earlier failures: "
                                    + String.join("; ", result.failedAttempts),
                            (double) result.attempts,
                            1.0));
        }
    }

    private void delivery(Workload workload, @Nullable Delivery delivery, List<Finding> findings) {
        if (!enabled(DELIVERY) || delivery == null) {
            return;
        }
        if (delivery.lost() > 0) {
            findings.add(
                    new Finding(
                            DELIVERY,
                            Severity.INVALID,
                            delivery.lost() + " acknowledged messages were never received by a subscription",
                            (double) delivery.lost(),
                            0.0));
        }
        if (delivery.duplicated() > 0) {
            findings.add(
                    new Finding(
                            DELIVERY,
                            Severity.WARNING,
                            delivery.duplicated() + " messages were received more than once",
                            (double) delivery.duplicated(),
                            0.0));
        }
        if (delivery.outOfOrder() > 0) {
            boolean expected = workload.expectsOrdering();
            findings.add(
                    new Finding(
                            DELIVERY,
                            expected ? Severity.WARNING : Severity.INFO,
                            delivery.outOfOrder()
                                    + " messages arrived after a later message from the same producer"
                                    + (expected
                                            ? ""
                                            : "; expected with several partitions or consumers per subscription"),
                            (double) delivery.outOfOrder(),
                            0.0));
        }
    }

    private void clockOffset(TestResult result, List<Finding> findings) {
        double p50 = result.aggregatedEndToEndLatency50pct;
        if (enabled(CLOCK_OFFSET) && result.metadata != null && p50 > 0) {
            double spread = WorkerClock.spreadMs(result.metadata.clockOffsets());
            double limit = maxClockOffsetFraction * p50;
            if (spread > limit) {
                findings.add(
                        new Finding(
                                CLOCK_OFFSET,
                                Severity.WARNING,
                                String.format(
                                        "End-to-end latency is unreliable: producer and consumer worker clocks"
                                                + " differ by up to %.3f ms, %.0f%% of the p50 end-to-end latency of"
                                                + " %.3f ms",
                                        spread, spread / p50 * 100, p50),
                                spread,
                                limit));
            }
        }
    }

    private void generatorSaturation(TestResult result, List<Finding> findings) {
        if (enabled(GENERATOR_SATURATION)) {
            List<WorkerLoad> loads = TestResult.measured(result.phase, result.workerHealth);
            double cpu = loads.stream().mapToDouble(WorkerLoad::cpu).max().orElse(0);
            double gc = loads.stream().mapToDouble(WorkerLoad::gc).max().orElse(0);
            if (cpu > maxWorkerCpu) {
                findings.add(saturation("CPU use", cpu, maxWorkerCpu));
            }
            if (gc > maxWorkerGc) {
                findings.add(saturation("GC pause time", gc, maxWorkerGc));
            }
        }
    }

    private static Finding saturation(String what, double value, double limit) {
        return new Finding(
                GENERATOR_SATURATION,
                Severity.WARNING,
                String.format(
                        "Worker %s reached %.0f%% in the measured window, above %.0f%%; results may be"
                                + " limited by the load generator, add workers",
                        what, value * 100, limit * 100),
                value,
                limit);
    }

    /**
     * Flags metrics that vary too much between repetitions to compare.
     *
     * @param metrics the spread of each metric across the successful repetitions
     * @return one finding per metric whose coefficient of variation exceeds the limit
     */
    List<Finding> runToRunVariance(Map<String, RepeatSummary.Stats> metrics) {
        List<Finding> findings = new ArrayList<>();
        if (enabled(RUN_TO_RUN_VARIANCE)) {
            metrics.forEach((name, stats) -> {
                Double cv = stats.cv();
                if (cv != null && cv > maxRunToRunCv) {
                    findings.add(
                            new Finding(
                                    RUN_TO_RUN_VARIANCE,
                                    Severity.WARNING,
                                    String.format(
                                            "%s varies by %.1f%% between repetitions (min %.2f, max %.2f); the run is"
                                                    + " too noisy to compare",
                                            name, cv * 100, stats.min(), stats.max()),
                                    cv,
                                    maxRunToRunCv));
                }
            });
        }
        return findings;
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
