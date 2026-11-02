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

import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.HdrHistogram.Histogram;

/**
 * The JSON result of one workload and driver, documented in docs/reference/results-schema.md.
 * Publish and end-to-end latencies are in milliseconds; publish delay is in microseconds.
 */
public class TestResult {
    private static final double[] PERCENTILES = {50, 75, 95, 99, 99.9, 99.99};

    public String workload;
    public String driver;
    public long messageSize;
    public int topics;
    public int partitions;
    public int producersPerTopic;
    public int consumersPerTopic;

    public List<Double> publishRate = new ArrayList<>();
    public List<Double> publishErrorRate = new ArrayList<>();
    public List<Double> consumeRate = new ArrayList<>();
    public List<Long> backlog = new ArrayList<>();

    public List<Double> publishLatencyAvg = new ArrayList<>();
    public List<Double> publishLatency50pct = new ArrayList<>();
    public List<Double> publishLatency75pct = new ArrayList<>();
    public List<Double> publishLatency95pct = new ArrayList<>();
    public List<Double> publishLatency99pct = new ArrayList<>();
    public List<Double> publishLatency999pct = new ArrayList<>();
    public List<Double> publishLatency9999pct = new ArrayList<>();
    public List<Double> publishLatencyMax = new ArrayList<>();

    public List<Double> publishDelayLatencyAvg = new ArrayList<>();
    public List<Long> publishDelayLatency50pct = new ArrayList<>();
    public List<Long> publishDelayLatency75pct = new ArrayList<>();
    public List<Long> publishDelayLatency95pct = new ArrayList<>();
    public List<Long> publishDelayLatency99pct = new ArrayList<>();
    public List<Long> publishDelayLatency999pct = new ArrayList<>();
    public List<Long> publishDelayLatency9999pct = new ArrayList<>();
    public List<Long> publishDelayLatencyMax = new ArrayList<>();

    public double aggregatedPublishLatencyAvg;
    public double aggregatedPublishLatency50pct;
    public double aggregatedPublishLatency75pct;
    public double aggregatedPublishLatency95pct;
    public double aggregatedPublishLatency99pct;
    public double aggregatedPublishLatency999pct;
    public double aggregatedPublishLatency9999pct;
    public double aggregatedPublishLatencyMax;

    public double aggregatedPublishDelayLatencyAvg;
    public long aggregatedPublishDelayLatency50pct;
    public long aggregatedPublishDelayLatency75pct;
    public long aggregatedPublishDelayLatency95pct;
    public long aggregatedPublishDelayLatency99pct;
    public long aggregatedPublishDelayLatency999pct;
    public long aggregatedPublishDelayLatency9999pct;
    public long aggregatedPublishDelayLatencyMax;

    public Map<Double, Double> aggregatedPublishLatencyQuantiles = new TreeMap<>();

    public Map<Double, Long> aggregatedPublishDelayLatencyQuantiles = new TreeMap<>();

    // End-to-end latencies, from publish to consumption

    public List<Double> endToEndLatencyAvg = new ArrayList<>();
    public List<Double> endToEndLatency50pct = new ArrayList<>();
    public List<Double> endToEndLatency75pct = new ArrayList<>();
    public List<Double> endToEndLatency95pct = new ArrayList<>();
    public List<Double> endToEndLatency99pct = new ArrayList<>();
    public List<Double> endToEndLatency999pct = new ArrayList<>();
    public List<Double> endToEndLatency9999pct = new ArrayList<>();
    public List<Double> endToEndLatencyMax = new ArrayList<>();

    public Map<Double, Double> aggregatedEndToEndLatencyQuantiles = new TreeMap<>();

    public double aggregatedEndToEndLatencyAvg;
    public double aggregatedEndToEndLatency50pct;
    public double aggregatedEndToEndLatency75pct;
    public double aggregatedEndToEndLatency95pct;
    public double aggregatedEndToEndLatency99pct;
    public double aggregatedEndToEndLatency999pct;
    public double aggregatedEndToEndLatency9999pct;
    public double aggregatedEndToEndLatencyMax;

    /**
     * Appends the values of one report interval.
     *
     * @param period the report interval, with latencies in microseconds
     */
    void recordPeriod(PeriodReport period) {
        publishRate.add(period.publishRate());
        publishErrorRate.add(period.publishErrorRate());
        consumeRate.add(period.consumeRate());
        backlog.add(period.backlog());
        appendMillis(
                period.publishLatency(),
                publishLatencyAvg,
                publishLatencyMax,
                publishLatency50pct,
                publishLatency75pct,
                publishLatency95pct,
                publishLatency99pct,
                publishLatency999pct,
                publishLatency9999pct);
        appendMicros(
                period.publishDelayLatency(),
                publishDelayLatencyAvg,
                publishDelayLatencyMax,
                publishDelayLatency50pct,
                publishDelayLatency75pct,
                publishDelayLatency95pct,
                publishDelayLatency99pct,
                publishDelayLatency999pct,
                publishDelayLatency9999pct);
        appendMillis(
                period.endToEndLatency(),
                endToEndLatencyAvg,
                endToEndLatencyMax,
                endToEndLatency50pct,
                endToEndLatency75pct,
                endToEndLatency95pct,
                endToEndLatency99pct,
                endToEndLatency999pct,
                endToEndLatency9999pct);
    }

    /**
     * Sets the latencies aggregated over the whole test.
     *
     * @param aggregate the cumulative latencies, in microseconds
     */
    void recordAggregate(CumulativeLatencies aggregate) {
        Histogram publish = aggregate.publishLatency;
        aggregatedPublishLatencyAvg = millis(publish.getMean());
        aggregatedPublishLatency50pct = millisAt(publish, 50);
        aggregatedPublishLatency75pct = millisAt(publish, 75);
        aggregatedPublishLatency95pct = millisAt(publish, 95);
        aggregatedPublishLatency99pct = millisAt(publish, 99);
        aggregatedPublishLatency999pct = millisAt(publish, 99.9);
        aggregatedPublishLatency9999pct = millisAt(publish, 99.99);
        aggregatedPublishLatencyMax = millis(publish.getMaxValue());
        publish
                .percentiles(100)
                .forEach(
                        value ->
                                aggregatedPublishLatencyQuantiles.put(
                                        value.getPercentile(), millis(value.getValueIteratedTo())));

        Histogram delay = aggregate.publishDelayLatency;
        aggregatedPublishDelayLatencyAvg = delay.getMean();
        aggregatedPublishDelayLatency50pct = delay.getValueAtPercentile(50);
        aggregatedPublishDelayLatency75pct = delay.getValueAtPercentile(75);
        aggregatedPublishDelayLatency95pct = delay.getValueAtPercentile(95);
        aggregatedPublishDelayLatency99pct = delay.getValueAtPercentile(99);
        aggregatedPublishDelayLatency999pct = delay.getValueAtPercentile(99.9);
        aggregatedPublishDelayLatency9999pct = delay.getValueAtPercentile(99.99);
        aggregatedPublishDelayLatencyMax = delay.getMaxValue();
        delay
                .percentiles(100)
                .forEach(
                        value ->
                                aggregatedPublishDelayLatencyQuantiles.put(
                                        value.getPercentile(), value.getValueIteratedTo()));

        Histogram endToEnd = aggregate.endToEndLatency;
        aggregatedEndToEndLatencyAvg = millis(endToEnd.getMean());
        aggregatedEndToEndLatency50pct = millisAt(endToEnd, 50);
        aggregatedEndToEndLatency75pct = millisAt(endToEnd, 75);
        aggregatedEndToEndLatency95pct = millisAt(endToEnd, 95);
        aggregatedEndToEndLatency99pct = millisAt(endToEnd, 99);
        aggregatedEndToEndLatency999pct = millisAt(endToEnd, 99.9);
        aggregatedEndToEndLatency9999pct = millisAt(endToEnd, 99.99);
        aggregatedEndToEndLatencyMax = millis(endToEnd.getMaxValue());
        endToEnd
                .percentiles(100)
                .forEach(
                        value ->
                                aggregatedEndToEndLatencyQuantiles.put(
                                        value.getPercentile(), millis(value.getValueIteratedTo())));
    }

    // Appends the average, maximum and PERCENTILES of a microsecond histogram in milliseconds.
    @SafeVarargs
    private static void appendMillis(
            Histogram micros, List<Double> avg, List<Double> max, List<Double>... percentiles) {
        avg.add(millis(micros.getMean()));
        for (int i = 0; i < PERCENTILES.length; i++) {
            percentiles[i].add(millisAt(micros, PERCENTILES[i]));
        }
        max.add(millis(micros.getMaxValue()));
    }

    // Appends the average, maximum and PERCENTILES of a microsecond histogram in microseconds.
    @SafeVarargs
    private static void appendMicros(
            Histogram micros, List<Double> avg, List<Long> max, List<Long>... percentiles) {
        avg.add(micros.getMean());
        for (int i = 0; i < PERCENTILES.length; i++) {
            percentiles[i].add(micros.getValueAtPercentile(PERCENTILES[i]));
        }
        max.add(micros.getMaxValue());
    }

    private static double millisAt(Histogram micros, double percentile) {
        return millis(micros.getValueAtPercentile(percentile));
    }

    private static double millis(double micros) {
        return micros / 1000.0;
    }

    public int getTopics() {
        return topics;
    }

    public int getPartitions() {
        return partitions;
    }

    public long getMessageSize() {
        return messageSize;
    }
}
