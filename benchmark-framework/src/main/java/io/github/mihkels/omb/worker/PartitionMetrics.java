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

import io.github.mihkels.omb.driver.RebalanceEvent;
import io.github.mihkels.omb.worker.commands.PartitionPeriod;
import io.github.mihkels.omb.worker.commands.RebalanceRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Per-partition counters and one-digit latency histograms, plus the rebalance events of the current
 * period. Only recorded into when the workload enables {@code partitionTelemetry}. Thread-safe.
 */
final class PartitionMetrics {
    static final int MAX_REBALANCES_PER_PERIOD = 1_000;

    // shortcut: allocates a Key per lookup; fine for an opt-in feature, switch to per-topic arrays
    // if partition telemetry ever shows up in allocation profiles
    private final Map<Key, Entry> entries = new ConcurrentHashMap<>();
    private final Queue<RebalanceRecord> rebalances = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queuedRebalances = new AtomicInteger();
    private final LongAdder droppedRebalances = new LongAdder();

    void recordPublish(String topic, int partition, long publishLatencyMicros) {
        Entry entry = entry(topic, partition);
        entry.sent.increment();
        entry.publish.recordValue(clamp(publishLatencyMicros));
    }

    void recordReceive(String topic, int partition, long endToEndLatencyMicros) {
        Entry entry = entry(topic, partition);
        entry.received.increment();
        // Clock skew can make end-to-end latency negative; count the message, skip the sample
        if (endToEndLatencyMicros >= 0) {
            entry.endToEnd.recordValue(clamp(endToEndLatencyMicros));
        }
    }

    void recordRebalance(String topic, RebalanceEvent event) {
        if (queuedRebalances.incrementAndGet() > MAX_REBALANCES_PER_PERIOD) {
            queuedRebalances.decrementAndGet();
            droppedRebalances.increment();
            return;
        }
        rebalances.add(RebalanceRecord.of(LocalWorker.ID, topic, event));
    }

    // The partitions with traffic since the previous call, resetting their counters
    List<PartitionPeriod> takePartitions() {
        List<PartitionPeriod> out = new ArrayList<>();
        entries.forEach((key, entry) -> {
            long sent = entry.sent.sumThenReset();
            long received = entry.received.sumThenReset();
            Histogram publish = entry.publish.getIntervalHistogram();
            Histogram endToEnd = entry.endToEnd.getIntervalHistogram();
            if (sent > 0 || received > 0) {
                out.add(new PartitionPeriod(key.topic, key.partition, sent, received, publish, endToEnd));
            }
        });
        return out;
    }

    // The rebalance events since the previous call
    List<RebalanceRecord> takeRebalances() {
        List<RebalanceRecord> out = new ArrayList<>();
        for (RebalanceRecord r = rebalances.poll(); r != null; r = rebalances.poll()) {
            queuedRebalances.decrementAndGet();
            out.add(r);
        }
        return out;
    }

    // The number of rebalance events dropped since the previous call
    long takeDroppedRebalances() {
        return droppedRebalances.sumThenReset();
    }

    void reset() {
        entries.clear();
        takeRebalances();
        droppedRebalances.reset();
    }

    private Entry entry(String topic, int partition) {
        return entries.computeIfAbsent(new Key(topic, partition), k -> new Entry());
    }

    private static long clamp(long micros) {
        return Math.clamp(micros, 0, PartitionPeriod.HIGHEST_TRACKABLE_MICROS);
    }

    private record Key(String topic, int partition) {}

    private static final class Entry {
        final LongAdder sent = new LongAdder();
        final LongAdder received = new LongAdder();
        final Recorder publish =
                new Recorder(PartitionPeriod.HIGHEST_TRACKABLE_MICROS, PartitionPeriod.SIGNIFICANT_DIGITS);
        final Recorder endToEnd =
                new Recorder(PartitionPeriod.HIGHEST_TRACKABLE_MICROS, PartitionPeriod.SIGNIFICANT_DIGITS);
    }
}
