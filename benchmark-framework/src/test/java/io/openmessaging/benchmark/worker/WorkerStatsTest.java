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
import org.junit.jupiter.api.Test;

class WorkerStatsTest {

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
