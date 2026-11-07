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
package io.openmessaging.benchmark.worker.commands;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

class PeriodStatsTest {

    @Test
    void plus() {
        PeriodStats one =
                new PeriodStats(
                        1, 2, 3, 4, 5, 6, 7, 8, new Histogram(5), new Histogram(5), new Histogram(5));
        PeriodStats two =
                new PeriodStats(
                        10, 20, 30, 40, 50, 60, 70, 80, new Histogram(5), new Histogram(5), new Histogram(5));

        PeriodStats result = one.plus(two);
        assertThat(result)
                .satisfies(r -> {
                    assertThat(r.messagesSent()).isEqualTo(11);
                    assertThat(r.messageSendErrors()).isEqualTo(22);
                    assertThat(r.bytesSent()).isEqualTo(33);
                    assertThat(r.messagesReceived()).isEqualTo(44);
                    assertThat(r.bytesReceived()).isEqualTo(55);
                    assertThat(r.totalMessagesSent()).isEqualTo(66);
                    assertThat(r.totalMessageSendErrors()).isEqualTo(77);
                    assertThat(r.totalMessagesReceived()).isEqualTo(88);

                    two.publishLatency().add(one.publishLatency());
                    two.publishDelayLatency().add(one.publishDelayLatency());
                    two.endToEndLatency().add(one.endToEndLatency());

                    assertThat(r.publishLatency()).isEqualTo(two.publishLatency());
                    assertThat(r.publishDelayLatency()).isEqualTo(two.publishDelayLatency());
                    assertThat(r.endToEndLatency()).isEqualTo(two.endToEndLatency());
                });
    }

    @Test
    void zeroPlus() {
        PeriodStats one = new PeriodStats();
        PeriodStats two =
                new PeriodStats(
                        10, 20, 30, 40, 50, 60, 70, 80, new Histogram(5), new Histogram(5), new Histogram(5));
        PeriodStats result = one.plus(two);
        assertThat(result)
                .satisfies(r -> {
                    assertThat(r.messagesSent()).isEqualTo(10);
                    assertThat(r.messageSendErrors()).isEqualTo(20);
                    assertThat(r.bytesSent()).isEqualTo(30);
                    assertThat(r.messagesReceived()).isEqualTo(40);
                    assertThat(r.bytesReceived()).isEqualTo(50);
                    assertThat(r.totalMessagesSent()).isEqualTo(60);
                    assertThat(r.totalMessageSendErrors()).isEqualTo(70);
                    assertThat(r.totalMessagesReceived()).isEqualTo(80);

                    assertThat(r.publishLatency()).isEqualTo(two.publishLatency());
                    assertThat(r.publishDelayLatency()).isEqualTo(two.publishDelayLatency());
                    assertThat(r.endToEndLatency()).isEqualTo(two.endToEndLatency());
                });
    }

    @Test
    void histogramSnapshotsAreDefensiveAndPreserveValues() {
        Histogram publish = new Histogram(5);
        Histogram delay = new Histogram(5);
        Histogram endToEnd = new Histogram(5);
        Histogram schedule = new Histogram(5);
        publish.recordValue(1_000);
        delay.recordValue(200);
        endToEnd.recordValue(3_000);
        schedule.recordValue(1_200);
        PeriodStats stats = new PeriodStats(1, 0, 0, 1, 0, 1, 0, 1, publish, delay, endToEnd, schedule);

        publish.recordValue(5_000);
        delay.recordValue(500);
        endToEnd.recordValue(6_000);
        stats.publishLatency().recordValue(7_000);
        stats.publishDelayLatency().recordValue(700);
        stats.endToEndLatency().recordValue(8_000);
        schedule.recordValue(9_000);
        stats.publishScheduleLatency().reset();

        assertThat(stats.publishLatency().getTotalCount()).isEqualTo(1);
        assertThat(stats.publishLatency().getValueAtPercentile(99)).isEqualTo(1_000);
        assertThat(stats.publishDelayLatency().getTotalCount()).isEqualTo(1);
        assertThat(stats.publishDelayLatency().getValueAtPercentile(99)).isEqualTo(200);
        assertThat(stats.endToEndLatency().getTotalCount()).isEqualTo(1);
        assertThat(stats.endToEndLatency().getValueAtPercentile(99)).isEqualTo(3_000);
        assertThat(stats.publishScheduleLatency().getTotalCount()).isEqualTo(1);
        assertThat(stats.publishScheduleLatency().getValueAtPercentile(99)).isEqualTo(1_200);
    }

    @Test
    void aggregatePreservesEveryCounterAndHistogramWithoutMutatingInputs() {
        Histogram histogram = new Histogram(5);
        histogram.recordValue(10);
        histogram.recordValue(60_000_000);
        PeriodStats one =
                new PeriodStats(1, 2, 3, 4, 5, 6, 7, 8, histogram, histogram, histogram, histogram);
        histogram.recordValue(40_000_000_000L);
        PeriodStats two =
                new PeriodStats(10, 20, 30, 40, 50, 60, 70, 80, histogram, histogram, histogram, histogram);
        PeriodStats result = PeriodStats.aggregate(List.of(one, two, one));

        assertThat(result).isEqualTo(one.plus(two).plus(one));
        assertThat(result.publishLatency().getTotalCount()).isEqualTo(7);
        assertThat(result.publishScheduleLatency().getTotalCount()).isEqualTo(7);
        result.publishScheduleLatency().reset();
        assertThat(one.publishScheduleLatency().getTotalCount()).isEqualTo(2);
        result.publishLatency().recordValue(100);
        result.publishDelayLatency().reset();
        result.endToEndLatency().reset();
        assertThat(result.publishLatency().getTotalCount()).isEqualTo(7);
        assertThat(one.publishLatency().getTotalCount()).isEqualTo(2);
        assertThat(two.endToEndLatency().getTotalCount()).isEqualTo(3);
    }

    @Test
    void aggregateHandlesEmptyAndSingletonSnapshots() {
        assertThat(PeriodStats.aggregate(List.of())).isEqualTo(new PeriodStats());
        PeriodStats snapshot = new PeriodStats();
        assertThat(PeriodStats.aggregate(List.of(snapshot))).isSameAs(snapshot);
        snapshot.publishLatency().recordValue(10);
        assertThat(PeriodStats.aggregate(List.of(snapshot)).publishLatency().getTotalCount()).isZero();
    }
}
