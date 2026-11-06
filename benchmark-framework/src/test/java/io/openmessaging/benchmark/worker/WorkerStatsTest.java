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

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class WorkerStatsTest {

    @Test
    void stalledScheduleAppearsInCorrectedTailButNotServiceTime() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(key -> null);
        try {
            WorkerStats stats = new WorkerStats(registry);
            RunCallbacks callbacks =
                    new RunCallbacks(
                            stats,
                            failure -> {
                                throw new AssertionError(failure);
                            });
            AtomicLong clock = new AtomicLong();
            for (int i = 0; i < 100; i++) {
                long intended = clock.get();
                clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(i < 98 ? 0 : 100));
                long sent = clock.get();
                assertThat(callbacks.beginSend()).isTrue();
                clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
                callbacks.completeSend(128, intended, sent, clock::get, null);
            }

            var period = stats.toPeriodStats();
            assertThat(period.publishLatency().getValueAtPercentile(99)).isEqualTo(1_000);
            assertThat(period.publishDelayLatency().getValueAtPercentile(99)).isEqualTo(100_000);
            assertThat(period.publishScheduleLatency().getValueAtPercentile(99)).isEqualTo(101_000);
            assertThat(period.publishScheduleLatency().getTotalCount()).isEqualTo(100);
            assertThat(stats.toPeriodStats().publishScheduleLatency().getTotalCount()).isZero();
            var cumulative = stats.toCumulativeLatencies();
            assertThat(cumulative.publishScheduleLatency).isEqualTo(period.publishScheduleLatency());
            assertThat(stats.toCumulativeLatencies().publishScheduleLatency.getTotalCount()).isZero();
            var timer = registry.get("produce_schedule_latency").timer();
            assertThat(timer.count()).isEqualTo(100);
            assertThat(timer.max(TimeUnit.MICROSECONDS)).isEqualTo(101_000);
            assertThat(registry.scrape()).contains("produce_schedule_latency_seconds_count 100");
        } finally {
            registry.close();
        }
    }

    @Test
    void noScheduleLagEqualsServiceTimeAndResetClearsCorrectedLatency() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(key -> null);
        try {
            WorkerStats stats = new WorkerStats(registry);
            stats.recordProducerSuccess(128, 1_000_000, 1_000_000, 2_000_000);
            var period = stats.toPeriodStats();
            assertThat(period.publishScheduleLatency()).isEqualTo(period.publishLatency());
            var cumulative = stats.toCumulativeLatencies();
            assertThat(cumulative.publishScheduleLatency).isEqualTo(cumulative.publishLatency);

            stats.recordProducerSuccess(128, 0, 0, TimeUnit.SECONDS.toNanos(90));
            assertThat(stats.toPeriodStats().publishScheduleLatency().getValueAtPercentile(99))
                    .isBetween(60_000_000L, 60_000_511L);
            stats.resetLatencies();
            assertThat(stats.toPeriodStats().publishScheduleLatency().getTotalCount()).isZero();
            assertThat(stats.toCumulativeLatencies().publishScheduleLatency.getTotalCount()).isZero();
        } finally {
            registry.close();
        }
    }

    @Test
    void resetClearsErrorTotalsForTheNextRun() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(key -> null);
        try {
            WorkerStats stats = new WorkerStats(registry);
            stats.recordMessageSent();
            stats.recordMessageReceived(128, 1);
            stats.recordProducerFailure();
            stats.recordProducerFailure();

            assertThat(stats.toCountersStats()).isEqualTo(new CountersStats(1, 2, 1));
            assertThat(stats.toPeriodStats().totalMessageSendErrors()).isEqualTo(2);
            stats.recordProducerFailure();

            stats.reset();

            assertThat(stats.toCountersStats()).isEqualTo(new CountersStats());
            var period = stats.toPeriodStats();
            assertThat(period.messageSendErrors()).isZero();
            assertThat(period.totalMessageSendErrors()).isZero();
            assertThat(registry.get("message_send_errors").counter().count()).isEqualTo(3);

            stats.recordProducerFailure();
            assertThat(stats.toCountersStats().messageSendErrors()).isEqualTo(1);
        } finally {
            registry.close();
        }
    }
}
