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
                .satisfies(
                        r -> {
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
                .satisfies(
                        r -> {
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
        publish.recordValue(1_000);
        delay.recordValue(200);
        endToEnd.recordValue(3_000);
        PeriodStats stats = new PeriodStats(1, 0, 0, 1, 0, 1, 0, 1, publish, delay, endToEnd);

        publish.recordValue(5_000);
        delay.recordValue(500);
        endToEnd.recordValue(6_000);
        stats.publishLatency().recordValue(7_000);
        stats.publishDelayLatency().recordValue(700);
        stats.endToEndLatency().recordValue(8_000);

        assertThat(stats.publishLatency().getTotalCount()).isEqualTo(1);
        assertThat(stats.publishLatency().getValueAtPercentile(99)).isEqualTo(1_000);
        assertThat(stats.publishDelayLatency().getTotalCount()).isEqualTo(1);
        assertThat(stats.publishDelayLatency().getValueAtPercentile(99)).isEqualTo(200);
        assertThat(stats.endToEndLatency().getTotalCount()).isEqualTo(1);
        assertThat(stats.endToEndLatency().getValueAtPercentile(99)).isEqualTo(3_000);
    }
}
