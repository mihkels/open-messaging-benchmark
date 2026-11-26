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
package io.github.mihkels.omb.worker;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.PackedHistogram;
import org.HdrHistogram.Recorder;

/**
 * One latency in microseconds, recorded into a per-interval histogram, a cumulative histogram and a
 * Micrometer timer.
 *
 * <p>Recording threads write to one of a few striped recorders chosen by thread, so producers and
 * callbacks on different threads do not contend on one histogram. Readers drain every stripe into
 * both the interval and the cumulative view, so each value is recorded once on the hot path.
 */
final class LatencyMetric {
    private static final int SIGNIFICANT_DIGITS = 4;
    private static final Duration HISTOGRAM_MIN = Duration.of(100, ChronoUnit.MICROS);
    private static final Duration HISTOGRAM_MAX = Duration.ofSeconds(60);
    // A power of two; threads beyond it share a stripe, which stays correct, only slower
    private static final int STRIPES = 4;

    private final boolean skipNegatives;
    private final long highestTrackableMicros;
    private final Recorder[] stripes;
    private final Timer timer;
    // Drained from the stripes but not yet taken; packed because they are usually sparse
    private final PackedHistogram pendingInterval;
    private final PackedHistogram pendingCumulative;

    private LatencyMetric(
            boolean skipNegatives,
            long highestTrackableMicros,
            String name,
            String description,
            MeterRegistry registry) {
        this.skipNegatives = skipNegatives;
        this.highestTrackableMicros = highestTrackableMicros;
        stripes = new Recorder[STRIPES];
        for (int i = 0; i < STRIPES; i++) {
            stripes[i] = new Recorder(highestTrackableMicros, SIGNIFICANT_DIGITS);
        }
        pendingInterval = new PackedHistogram(highestTrackableMicros, SIGNIFICANT_DIGITS);
        pendingCumulative = new PackedHistogram(highestTrackableMicros, SIGNIFICANT_DIGITS);
        // Buckets let dashboards compute percentiles across workers; above 60 s lands in +Inf
        timer =
                Timer.builder(name)
                        .description(description)
                        .publishPercentileHistogram()
                        .minimumExpectedValue(HISTOGRAM_MIN)
                        .maximumExpectedValue(HISTOGRAM_MAX)
                        .register(registry);
    }

    /**
     * A latency whose values above the highest trackable value are recorded as that value.
     *
     * @param highestTrackableMicros the highest value the histograms track
     * @param name the timer name
     * @param description the timer description
     * @param registry the registry of the timer
     * @return the metric
     */
    static LatencyMetric clamped(
            long highestTrackableMicros, String name, String description, MeterRegistry registry) {
        return new LatencyMetric(false, highestTrackableMicros, name, description, registry);
    }

    /**
     * A latency measured across hosts, where clock skew can make values negative. Negative values are
     * skipped; zero is a real sub-millisecond measurement and is recorded. Values above the highest
     * trackable value are recorded as that value.
     *
     * @param highestTrackableMicros the highest value the histograms track
     * @param name the timer name
     * @param description the timer description
     * @param registry the registry of the timer
     * @return the metric
     */
    static LatencyMetric skewTolerant(
            long highestTrackableMicros, String name, String description, MeterRegistry registry) {
        return new LatencyMetric(true, highestTrackableMicros, name, description, registry);
    }

    void record(long micros) {
        if (skipNegatives && micros < 0) {
            return;
        }
        long value = Math.min(highestTrackableMicros, micros);
        stripes[(int) Thread.currentThread().threadId() & (STRIPES - 1)].recordValue(value);
        timer.record(value, TimeUnit.MICROSECONDS);
    }

    /**
     * The values recorded since the previous call.
     *
     * @return the interval histogram
     */
    synchronized Histogram takeInterval() {
        drain();
        return take(pendingInterval);
    }

    /**
     * The values recorded since the previous call; the caller accumulates them.
     *
     * @return the values recorded since the previous call to this method
     */
    synchronized Histogram takeCumulative() {
        drain();
        return take(pendingCumulative);
    }

    synchronized void reset() {
        drain();
        pendingInterval.reset();
        pendingCumulative.reset();
    }

    private Histogram take(PackedHistogram pending) {
        Histogram taken = new Histogram(highestTrackableMicros, SIGNIFICANT_DIGITS);
        taken.add(pending);
        pending.reset();
        return taken;
    }

    private void drain() {
        for (Recorder stripe : stripes) {
            Histogram drained = stripe.getIntervalHistogram();
            pendingInterval.add(drained);
            pendingCumulative.add(drained);
        }
    }
}
