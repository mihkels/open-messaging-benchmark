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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/**
 * One partition's traffic during a reporting period, sent by workers when the workload enables
 * {@code partitionTelemetry}.
 *
 * @param topic the topic
 * @param partition the partition
 * @param messagesSent acknowledged sends to the partition
 * @param messagesReceived messages consumed from the partition
 * @param publishLatency publish latency in microseconds, one significant digit
 * @param endToEndLatency end-to-end latency in microseconds, one significant digit
 */
public record PartitionPeriod(
        @JsonProperty("topic") String topic,
        @JsonProperty("partition") int partition,
        @JsonProperty("messagesSent") long messagesSent,
        @JsonProperty("messagesReceived") long messagesReceived,
        @JsonProperty("publishLatency") Histogram publishLatency,
        @JsonProperty("endToEndLatency") Histogram endToEndLatency) {

    /** Highest latency the per-partition histograms track; larger values are clamped to it. */
    public static final long HIGHEST_TRACKABLE_MICROS = TimeUnit.SECONDS.toMicros(60);

    /** Significant digits of the per-partition histograms: enough to rank partitions. */
    public static final int SIGNIFICANT_DIGITS = 1;

    // Defensive copies, as in PeriodStats
    public PartitionPeriod {
        publishLatency = publishLatency.copy();
        endToEndLatency = endToEndLatency.copy();
    }

    @Override
    public Histogram publishLatency() {
        return publishLatency.copy();
    }

    @Override
    public Histogram endToEndLatency() {
        return endToEndLatency.copy();
    }

    /**
     * Merges the partitions reported by several workers, summing the entries for the same topic and
     * partition. The inputs are not modified.
     *
     * @param reports each worker's partitions
     * @return one entry per topic and partition
     */
    static List<PartitionPeriod> merge(List<List<PartitionPeriod>> reports) {
        Map<String, Map<Integer, PartitionPeriod>> merged = new LinkedHashMap<>();
        for (List<PartitionPeriod> report : reports) {
            for (PartitionPeriod p : report) {
                merged
                        .computeIfAbsent(p.topic, t -> new LinkedHashMap<>())
                        .merge(p.partition, p, PartitionPeriod::plus);
            }
        }
        List<PartitionPeriod> out = new ArrayList<>();
        merged.values().forEach(partitions -> out.addAll(partitions.values()));
        return out;
    }

    private static PartitionPeriod plus(PartitionPeriod a, PartitionPeriod b) {
        // Auto-resizing, so a histogram from a worker with another range still adds
        Histogram publish = new Histogram(SIGNIFICANT_DIGITS);
        publish.add(a.publishLatency);
        publish.add(b.publishLatency);
        Histogram endToEnd = new Histogram(SIGNIFICANT_DIGITS);
        endToEnd.add(a.endToEndLatency);
        endToEnd.add(b.endToEndLatency);
        return new PartitionPeriod(
                a.topic,
                a.partition,
                a.messagesSent + b.messagesSent,
                a.messagesReceived + b.messagesReceived,
                publish,
                endToEnd);
    }
}
