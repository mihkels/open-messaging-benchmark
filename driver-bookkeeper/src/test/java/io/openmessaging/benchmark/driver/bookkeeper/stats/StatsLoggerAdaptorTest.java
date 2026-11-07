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
package io.openmessaging.benchmark.driver.bookkeeper.stats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class StatsLoggerAdaptorTest {

    @Test
    void threadScopedMetricsDelegateAndPreserveSnapshots() {
        StatsLogger delegate = mock(StatsLogger.class);
        Counter counter = mock(Counter.class);
        OpStatsLogger operations = mock(OpStatsLogger.class);
        when(delegate.getThreadScopedCounter("counter")).thenReturn(counter);
        when(delegate.getThreadScopedOpStatsLogger("operations")).thenReturn(operations);
        when(operations.toOpStatsData())
                .thenReturn(new OpStatsData(2, 3, 4.5, new long[] {10, 50, 90, 99, 999, 9999}));

        StatsLoggerAdaptor adaptor = new StatsLoggerAdaptor(delegate);
        adaptor.getThreadScopedCounter("counter").addLatency(17, TimeUnit.MILLISECONDS);
        var adaptedOperations = adaptor.getThreadScopedOpStatsLogger("operations");
        adaptedOperations.registerSuccessfulEvent(19, TimeUnit.MILLISECONDS);
        var snapshot = adaptedOperations.toOpStatsData();

        verify(counter).addLatency(17, TimeUnit.MILLISECONDS);
        verify(operations).registerSuccessfulEvent(19, TimeUnit.MILLISECONDS);
        assertThat(snapshot.getNumSuccessfulEvents()).isEqualTo(2);
        assertThat(snapshot.getNumFailedEvents()).isEqualTo(3);
        assertThat(snapshot.getAvgLatencyMillis()).isEqualTo(4.5);
        assertThat(snapshot.getP10Latency()).isEqualTo(10);
        assertThat(snapshot.getP50Latency()).isEqualTo(50);
        assertThat(snapshot.getP90Latency()).isEqualTo(90);
        assertThat(snapshot.getP99Latency()).isEqualTo(99);
        assertThat(snapshot.getP999Latency()).isEqualTo(999);
        assertThat(snapshot.getP9999Latency()).isEqualTo(9999);
    }

    @Test
    void nullLoggerProvidesUsableNoOpThreadScopedMetrics() {
        StatsLoggerAdaptor adaptor = new StatsLoggerAdaptor(NullStatsLogger.INSTANCE);

        var counter = adaptor.getThreadScopedCounter("counter");
        counter.inc();
        counter.addLatency(1, TimeUnit.MILLISECONDS);
        assertThat(counter.get()).isZero();

        var operations = adaptor.getThreadScopedOpStatsLogger("operations");
        operations.registerSuccessfulEvent(1, TimeUnit.MILLISECONDS);
        assertThat(operations.toOpStatsData().getNumSuccessfulEvents()).isZero();
    }

    @Test
    void gaugesAndScopesDelegateAndUnregisterOnlyRegisteredResources() {
        StatsLogger delegate = mock(StatsLogger.class);
        StatsLogger scoped = mock(StatsLogger.class);
        when(delegate.scope("worker")).thenReturn(scoped);
        StatsLoggerAdaptor adaptor = new StatsLoggerAdaptor(delegate);
        dlshade.org.apache.bookkeeper.stats.Gauge<Integer> gauge =
                new dlshade.org.apache.bookkeeper.stats.Gauge<>() {
                    @Override
                    public Integer getDefaultValue() {
                        return -1;
                    }

                    @Override
                    public Integer getSample() {
                        return 42;
                    }
                };

        adaptor.registerGauge("depth", gauge);
        ArgumentCaptor<org.apache.bookkeeper.stats.Gauge<Integer>> adaptedGauge =
                ArgumentCaptor.forClass(org.apache.bookkeeper.stats.Gauge.class);
        verify(delegate).registerGauge(eq("depth"), adaptedGauge.capture());
        assertThat(adaptedGauge.getValue().getDefaultValue()).isEqualTo(-1);
        assertThat(adaptedGauge.getValue().getSample()).isEqualTo(42);

        adaptor.unregisterGauge("depth", gauge);
        adaptor.unregisterGauge("missing", gauge);
        verify(delegate).unregisterGauge("depth", adaptedGauge.getValue());
        verify(delegate, never()).unregisterGauge(eq("missing"), org.mockito.ArgumentMatchers.any());

        var adaptedScope = adaptor.scope("worker");
        adaptor.removeScope("worker", adaptedScope);
        adaptor.removeScope("worker", adaptedScope);
        verify(delegate).removeScope("worker", scoped);
    }
}
