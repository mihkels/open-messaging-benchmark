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

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

class LatencyMetricTest {
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @Test
    void clampedMetricRecordsValuesAboveTheMaximumAsTheMaximum() {
        LatencyMetric metric = LatencyMetric.clamped(1_000, "clamped", "test", registry);

        metric.record(5);
        metric.record(50_000);

        Histogram interval = metric.takeInterval();
        assertThat(interval.getTotalCount()).isEqualTo(2);
        assertThat(interval.getMaxValue()).isEqualTo(interval.highestEquivalentValue(1_000));
        assertThat(registry.get("clamped").timer().count()).isEqualTo(2);
    }

    @Test
    void skewTolerantMetricRecordsZeroSkipsNegativesAndClampsAboveTheMaximum() {
        LatencyMetric metric = LatencyMetric.skewTolerant(1_000, "skewed", "test", registry);

        metric.record(0);
        metric.record(-3);
        metric.record(7);
        metric.record(50_000);

        Histogram interval = metric.takeInterval();
        assertThat(interval.getTotalCount()).isEqualTo(3);
        assertThat(interval.getCountAtValue(0)).isEqualTo(1);
        assertThat(interval.getMaxValue()).isEqualTo(interval.highestEquivalentValue(1_000));
        assertThat(metric.takeCumulative().getTotalCount()).isEqualTo(3);
        assertThat(registry.get("skewed").timer().count()).isEqualTo(3);
    }

    @Test
    void intervalAndCumulativeHistogramsAreReadIndependently() {
        LatencyMetric metric = LatencyMetric.clamped(1_000, "independent", "test", registry);
        metric.record(10);

        assertThat(metric.takeInterval().getTotalCount()).isEqualTo(1);
        metric.record(20);
        assertThat(metric.takeInterval().getTotalCount()).isEqualTo(1);
        assertThat(metric.takeCumulative().getTotalCount()).isEqualTo(2);

        metric.reset();
        assertThat(metric.takeInterval().getTotalCount()).isZero();
        assertThat(metric.takeCumulative().getTotalCount()).isZero();
    }

    @Test
    void valuesFromManyThreadsReachBothTheIntervalAndTheCumulativeHistogram() throws Exception {
        LatencyMetric metric = LatencyMetric.clamped(1_000, "threads", "test", registry);
        int threads = 8;
        int perThread = 10_000;
        List<Thread> recorders = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            recorders.add(
                    Thread.ofPlatform()
                            .start(() -> {
                                for (int i = 0; i < perThread; i++) {
                                    metric.record(i % 100);
                                }
                            }));
        }
        Histogram interval = metric.takeInterval();
        for (Thread recorder : recorders) {
            recorder.join();
        }
        interval.add(metric.takeInterval());

        assertThat(interval.getTotalCount()).isEqualTo((long) threads * perThread);
        assertThat(interval.getCountAtValue(42)).isEqualTo((long) threads * perThread / 100);
        assertThat(metric.takeCumulative().getTotalCount()).isEqualTo((long) threads * perThread);
        assertThat(metric.takeInterval().getTotalCount()).isZero();
    }

    @Test
    void timerIsFedFromTheDrainNotFromTheRecordingThread() {
        LatencyMetric metric = LatencyMetric.clamped(60_000_000, "drained", "test", registry);
        metric.record(250);
        metric.record(1_500);

        var timer = registry.get("drained").timer();
        assertThat(timer.count()).isZero();

        metric.flush();
        assertThat(timer.count()).isEqualTo(2);
        assertThat(timer.totalTime(TimeUnit.MICROSECONDS)).isEqualTo(1_750);
        assertThat(timer.max(TimeUnit.MICROSECONDS)).isEqualTo(1_500);
        // Flushing does not take the values from the benchmark histograms
        assertThat(metric.takeInterval().getTotalCount()).isEqualTo(2);
        assertThat(metric.takeCumulative().getTotalCount()).isEqualTo(2);
    }

    @Test
    void everyValueReachesTheTimerOnceWhicheverReadDrainsIt() {
        LatencyMetric metric = LatencyMetric.clamped(1_000, "once", "test", registry);
        metric.record(1);
        metric.flush();
        metric.record(2);
        metric.takeInterval();
        metric.record(3);
        metric.takeCumulative();
        metric.record(4);
        metric.reset();
        metric.flush();

        var timer = registry.get("once").timer();
        assertThat(timer.count()).isEqualTo(4);
        assertThat(timer.totalTime(TimeUnit.MICROSECONDS)).isEqualTo(10);
    }

    @Test
    void valuesBeyondHistogramPrecisionAreNotOverstated() {
        LatencyMetric metric = LatencyMetric.clamped(60_000_000, "coarse", "test", registry);
        long micros = 12_345_678;
        metric.record(micros);
        Histogram interval = metric.takeInterval();

        double replayed = registry.get("coarse").timer().max(TimeUnit.MICROSECONDS);
        assertThat(replayed)
                .isEqualTo(interval.lowestEquivalentValue(micros))
                .isLessThanOrEqualTo(micros);
        assertThat(micros - replayed).isLessThan(micros / 10_000.0);
    }
}
