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
package io.openmessaging.benchmark.worker;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * One latency in microseconds, recorded into a per-interval histogram, a cumulative histogram and a
 * Micrometer timer.
 */
final class LatencyMetric {
    private static final int SIGNIFICANT_DIGITS = 5;

    private final boolean skipNegatives;
    private final long highestTrackableMicros;
    private final Recorder interval;
    private final Recorder cumulative;
    private final Timer timer;

    private LatencyMetric(
            boolean skipNegatives,
            long highestTrackableMicros,
            String name,
            String description,
            MeterRegistry registry) {
        this.skipNegatives = skipNegatives;
        this.highestTrackableMicros = highestTrackableMicros;
        interval = new Recorder(highestTrackableMicros, SIGNIFICANT_DIGITS);
        cumulative = new Recorder(highestTrackableMicros, SIGNIFICANT_DIGITS);
        timer = Timer.builder(name).description(description).register(registry);
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
        cumulative.recordValue(value);
        interval.recordValue(value);
        timer.record(value, TimeUnit.MICROSECONDS);
    }

    /**
     * The values recorded since the previous call.
     *
     * @return the interval histogram
     */
    Histogram takeInterval() {
        return interval.getIntervalHistogram();
    }

    /**
     * The values recorded since the previous call; the caller accumulates them.
     *
     * @return the cumulative recorder's interval histogram
     */
    Histogram takeCumulative() {
        return cumulative.getIntervalHistogram();
    }

    void reset() {
        interval.reset();
        cumulative.reset();
    }
}
