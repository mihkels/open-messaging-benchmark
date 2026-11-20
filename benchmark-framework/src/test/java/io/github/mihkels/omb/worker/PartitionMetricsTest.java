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

import io.github.mihkels.omb.driver.RebalanceEvent;
import io.github.mihkels.omb.worker.commands.PartitionPeriod;
import java.util.List;
import org.junit.jupiter.api.Test;

class PartitionMetricsTest {

    @Test
    void recordsEachPartitionAndResetsOnTake() {
        PartitionMetrics metrics = new PartitionMetrics();
        metrics.recordPublish("a", 0, 1_000);
        metrics.recordPublish("a", 0, 2_000);
        metrics.recordReceive("a", 1, 5_000);
        metrics.recordReceive("b", 0, 7_000);

        List<PartitionPeriod> first = metrics.takePartitions();

        assertThat(first).hasSize(3);
        PartitionPeriod a0 = find(first, "a", 0);
        assertThat(a0.messagesSent()).isEqualTo(2);
        assertThat(a0.publishLatency().getTotalCount()).isEqualTo(2);
        assertThat(a0.endToEndLatency().getTotalCount()).isZero();
        assertThat(find(first, "a", 1).messagesReceived()).isEqualTo(1);
        assertThat(metrics.takePartitions()).as("idle partitions are not reported").isEmpty();
    }

    @Test
    void clampsLongLatenciesAndCountsSkewedMessagesWithoutASample() {
        PartitionMetrics metrics = new PartitionMetrics();
        metrics.recordPublish("t", 0, Long.MAX_VALUE);
        metrics.recordReceive("t", 0, -5);

        PartitionPeriod p = metrics.takePartitions().getFirst();

        assertThat(p.publishLatency().getMaxValue())
                .isLessThanOrEqualTo(
                        p.publishLatency().highestEquivalentValue(PartitionPeriod.HIGHEST_TRACKABLE_MICROS));
        assertThat(p.messagesReceived()).isEqualTo(1);
        assertThat(p.endToEndLatency().getTotalCount()).isZero();
    }

    @Test
    void capsRebalancesPerPeriodAndCountsTheRest() {
        PartitionMetrics metrics = new PartitionMetrics();
        RebalanceEvent event = new RebalanceEvent(RebalanceEvent.Kind.REVOKED, 1L, List.of(0));
        for (int i = 0; i < PartitionMetrics.MAX_REBALANCES_PER_PERIOD + 3; i++) {
            metrics.recordRebalance("t", event);
        }

        assertThat(metrics.takeRebalances())
                .hasSize(PartitionMetrics.MAX_REBALANCES_PER_PERIOD)
                .allSatisfy(r -> assertThat(r.worker()).isEqualTo(LocalWorker.ID));
        assertThat(metrics.takeDroppedRebalances()).isEqualTo(3);

        metrics.recordRebalance("t", event);
        assertThat(metrics.takeRebalances()).as("the cap is per period").hasSize(1);
        assertThat(metrics.takeDroppedRebalances()).isZero();
    }

    @Test
    void resetDropsEverything() {
        PartitionMetrics metrics = new PartitionMetrics();
        metrics.recordPublish("t", 0, 1);
        metrics.recordRebalance("t", new RebalanceEvent(RebalanceEvent.Kind.LOST, 1L, List.of(0)));

        metrics.reset();

        assertThat(metrics.takePartitions()).isEmpty();
        assertThat(metrics.takeRebalances()).isEmpty();
    }

    private static PartitionPeriod find(
            List<PartitionPeriod> partitions, String topic, int partition) {
        return partitions.stream()
                .filter(p -> p.topic().equals(topic) && p.partition() == partition)
                .findFirst()
                .orElseThrow();
    }
}
