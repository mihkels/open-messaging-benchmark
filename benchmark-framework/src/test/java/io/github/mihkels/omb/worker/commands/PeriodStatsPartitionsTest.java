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
package io.github.mihkels.omb.worker.commands;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mihkels.omb.driver.RebalanceEvent;
import io.github.mihkels.omb.worker.jackson.ObjectMappers;
import java.util.List;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

class PeriodStatsPartitionsTest {

    @Test
    void jsonOmitsPartitionTelemetryWhenAbsentAndRoundTripsIt() throws Exception {
        var mapper = ObjectMappers.DEFAULT.mapper();
        String empty = mapper.writeValueAsString(new PeriodStats());
        assertThat(empty).doesNotContain("partitions", "rebalances", "droppedRebalances");

        PeriodStats stats =
                withTelemetry(
                        List.of(partition("t", 0, 5, 2_000)),
                        List.of(rebalance(null, RebalanceEvent.Kind.ASSIGNED)),
                        4);
        String json = mapper.writeValueAsString(stats);
        assertThat(mapper.readValue(json, PeriodStats.class)).isEqualTo(stats);
    }

    @Test
    void aggregateSumsTheSamePartitionAcrossWorkers() {
        PeriodStats first =
                withTelemetry(
                        List.of(partition("t", 0, 5, 1_000), partition("t", 1, 1, 9_000)),
                        List.of(rebalance("w1", RebalanceEvent.Kind.REVOKED)),
                        1);
        PeriodStats second =
                withTelemetry(
                        List.of(partition("t", 0, 3, 3_000)),
                        List.of(rebalance("w2", RebalanceEvent.Kind.ASSIGNED)),
                        2);

        PeriodStats merged = PeriodStats.aggregate(List.of(first, second, new PeriodStats()));

        assertThat(merged.partitions()).hasSize(2);
        PartitionPeriod t0 = merged.partitions().getFirst();
        assertThat(t0.partition()).isZero();
        assertThat(t0.messagesSent()).isEqualTo(8);
        assertThat(t0.publishLatency().getTotalCount()).isEqualTo(2);
        assertThat(merged.rebalances()).extracting(RebalanceRecord::worker).containsExactly("w1", "w2");
        assertThat(merged.droppedRebalances()).isEqualTo(3);
        assertThat(first.partitions().getFirst().publishLatency().getTotalCount())
                .as("inputs are not modified")
                .isEqualTo(1);
    }

    @Test
    void withWorkerIdTagsRebalances() {
        PeriodStats stats =
                withTelemetry(List.of(), List.of(rebalance("local", RebalanceEvent.Kind.LOST)), 0);

        assertThat(stats.withWorkerId("worker-2").rebalances())
                .extracting(RebalanceRecord::worker)
                .containsExactly("worker-2");
        assertThat(stats.withWorkerId("worker-2").sendErrorReasons()).isEmpty();
    }

    private static PeriodStats withTelemetry(
            List<PartitionPeriod> partitions, List<RebalanceRecord> rebalances, long dropped) {
        return new PeriodStats(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                partitions,
                rebalances,
                dropped);
    }

    private static PartitionPeriod partition(
            String topic, int partition, long sent, long latencyMicros) {
        Histogram publish =
                new Histogram(PartitionPeriod.HIGHEST_TRACKABLE_MICROS, PartitionPeriod.SIGNIFICANT_DIGITS);
        publish.recordValue(latencyMicros);
        return new PartitionPeriod(
                topic,
                partition,
                sent,
                0,
                publish,
                new Histogram(
                        PartitionPeriod.HIGHEST_TRACKABLE_MICROS, PartitionPeriod.SIGNIFICANT_DIGITS));
    }

    private static RebalanceRecord rebalance(String worker, RebalanceEvent.Kind kind) {
        return new RebalanceRecord(null, 1_000L, worker, "t", kind, List.of(0, 1));
    }
}
