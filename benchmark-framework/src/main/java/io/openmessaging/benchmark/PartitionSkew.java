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
package io.openmessaging.benchmark;

import io.openmessaging.benchmark.worker.commands.PartitionPeriod;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.HdrHistogram.Histogram;
import org.jspecify.annotations.Nullable;

/**
 * How unevenly one period's traffic and latency spread over partitions, from the per-partition
 * telemetry of {@code partitionTelemetry} workloads.
 *
 * @param period index of the period in the result's per-period series
 * @param partitions partitions that published or consumed during the period
 * @param publishMaxMeanRatio the busiest partition's publish count divided by the mean
 * @param consumeMaxMeanRatio the busiest partition's consume count divided by the mean
 * @param publishGini Gini coefficient of per-partition publish counts: 0 is even, near 1 is one
 *     partition taking everything
 * @param consumeGini Gini coefficient of per-partition consume counts
 * @param topPublishP99 the partitions with the highest publish p99
 * @param topEndToEndP99 the partitions with the highest end-to-end p99
 */
public record PartitionSkew(
        int period,
        int partitions,
        double publishMaxMeanRatio,
        double consumeMaxMeanRatio,
        double publishGini,
        double consumeGini,
        List<PartitionLatency> topPublishP99,
        List<PartitionLatency> topEndToEndP99) {

    /** Number of partitions listed in each top-p99 list. */
    static final int TOP_N = 5;

    public PartitionSkew {
        topPublishP99 = List.copyOf(topPublishP99);
        topEndToEndP99 = List.copyOf(topEndToEndP99);
    }

    /**
     * One partition's p99 latency. The per-partition histograms keep one significant digit, so the
     * value is accurate to about 10%.
     *
     * @param topic the topic
     * @param partition the partition
     * @param p99Ms the 99th percentile latency in milliseconds
     */
    public record PartitionLatency(String topic, int partition, double p99Ms) {}

    /**
     * Summarizes one period.
     *
     * @param period index of the period
     * @param partitions the merged per-partition traffic of all workers
     * @return the summary, or {@code null} when no partition reported traffic
     */
    static @Nullable PartitionSkew of(int period, List<PartitionPeriod> partitions) {
        if (partitions.isEmpty()) {
            return null;
        }
        long[] sent = partitions.stream().mapToLong(PartitionPeriod::messagesSent).toArray();
        long[] received = partitions.stream().mapToLong(PartitionPeriod::messagesReceived).toArray();
        return new PartitionSkew(
                period,
                partitions.size(),
                maxMeanRatio(sent),
                maxMeanRatio(received),
                gini(sent),
                gini(received),
                top(partitions, PartitionPeriod::publishLatency),
                top(partitions, PartitionPeriod::endToEndLatency));
    }

    // 0 when nothing was counted, so the result never holds NaN
    static double maxMeanRatio(long[] counts) {
        long total = Arrays.stream(counts).sum();
        if (total == 0) {
            return 0;
        }
        long max = Arrays.stream(counts).max().orElse(0);
        return (double) max * counts.length / total;
    }

    // G = 2 * sum(i * x_i) / (n * sum(x)) - (n + 1) / n over ascending x, i from 1
    static double gini(long[] counts) {
        long[] sorted = counts.clone();
        Arrays.sort(sorted);
        int n = sorted.length;
        double total = 0;
        double weighted = 0;
        for (int i = 0; i < n; i++) {
            total += sorted[i];
            weighted += (i + 1) * (double) sorted[i];
        }
        if (total == 0) {
            return 0;
        }
        return 2 * weighted / (n * total) - (n + 1.0) / n;
    }

    private static List<PartitionLatency> top(
            List<PartitionPeriod> partitions, Function<PartitionPeriod, Histogram> latency) {
        List<PartitionLatency> measured = new ArrayList<>();
        for (PartitionPeriod p : partitions) {
            Histogram histogram = latency.apply(p);
            if (histogram.getTotalCount() > 0) {
                measured.add(
                        new PartitionLatency(
                                p.topic(), p.partition(), histogram.getValueAtPercentile(99) / 1000.0));
            }
        }
        return measured.stream()
                .sorted(Comparator.comparingDouble(PartitionLatency::p99Ms).reversed())
                .limit(TOP_N)
                .toList();
    }
}
