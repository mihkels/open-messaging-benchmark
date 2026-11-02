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

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
    void positiveOnlyMetricSkipsValuesSkewedToZeroOrBelow() {
        LatencyMetric metric = LatencyMetric.positiveOnly(1_000, "skewed", "test", registry);

        metric.record(0);
        metric.record(-3);
        metric.record(7);

        assertThat(metric.takeInterval().getTotalCount()).isEqualTo(1);
        assertThat(metric.takeCumulative().getTotalCount()).isEqualTo(1);
        assertThat(registry.get("skewed").timer().totalTime(TimeUnit.MICROSECONDS)).isEqualTo(7);
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
}
