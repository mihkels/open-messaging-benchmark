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

import com.fasterxml.jackson.annotation.JsonInclude;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.RunPhase;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.DataFormatException;
import org.HdrHistogram.Histogram;
import org.jspecify.annotations.Nullable;

/**
 * The JSON result of one workload and driver, documented in docs/reference/results-schema.md.
 * Publish and end-to-end latencies are in milliseconds; publish delay is in microseconds.
 */
public class TestResult {
    /** The {@link #histograms} key of the publish latency histogram. */
    public static final String PUBLISH_HISTOGRAM = "publish";

    /** The {@link #histograms} key of the publish delay histogram. */
    public static final String PUBLISH_DELAY_HISTOGRAM = "publishDelay";

    /** The {@link #histograms} key of the end-to-end latency histogram. */
    public static final String END_TO_END_HISTOGRAM = "endToEnd";

    private static final double[] PERCENTILES = {50, 75, 95, 99, 99.9, 99.99};

    public int schemaVersion = 2;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public @Nullable ResultMetadata metadata;

    public ResultSummary summary = ResultSummary.empty();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Thresholds.@Nullable Result thresholds;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public @Nullable Map<String, String> histograms;

    public @Nullable String workload;
    public @Nullable String driver;
    public long messageSize;

    /**
     * zstd-compressed size divided by original size of the generated payloads, weighted by how often
     * each is sent; only for workloads with a payload generator.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public @Nullable Double payloadCompressionRatio;

    public int topics;
    public int partitions;
    public int producersPerTopic;
    public int consumersPerTopic;

    public List<Double> publishRate = new ArrayList<>();
    public List<Double> publishErrorRate = new ArrayList<>();
    public List<Double> consumeRate = new ArrayList<>();
    public List<Long> backlog = new ArrayList<>();

    /** The run phase of each period; warm-up periods come first and are left out of the summary. */
    public List<RunPhase> phase = new ArrayList<>();

    /** The target publish rate at the end of each period; absent in older files. */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<Double> targetRate = new ArrayList<>();

    /** The index of the workload stage running at the end of each period; only for staged runs. */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<Integer> stage = new ArrayList<>();

    public List<Double> publishLatencyAvg = new ArrayList<>();
    public List<Double> publishLatency50pct = new ArrayList<>();
    public List<Double> publishLatency75pct = new ArrayList<>();
    public List<Double> publishLatency95pct = new ArrayList<>();
    public List<Double> publishLatency99pct = new ArrayList<>();
    public List<Double> publishLatency999pct = new ArrayList<>();
    public List<Double> publishLatency9999pct = new ArrayList<>();
    public List<Double> publishLatencyMax = new ArrayList<>();

    public List<Double> publishScheduleLatencyAvg = new ArrayList<>();
    public List<Double> publishScheduleLatency50pct = new ArrayList<>();
    public List<Double> publishScheduleLatency75pct = new ArrayList<>();
    public List<Double> publishScheduleLatency95pct = new ArrayList<>();
    public List<Double> publishScheduleLatency99pct = new ArrayList<>();
    public List<Double> publishScheduleLatency999pct = new ArrayList<>();
    public List<Double> publishScheduleLatency9999pct = new ArrayList<>();
    public List<Double> publishScheduleLatencyMax = new ArrayList<>();

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

    public double aggregatedPublishScheduleLatencyAvg;
    public double aggregatedPublishScheduleLatency50pct;
    public double aggregatedPublishScheduleLatency75pct;
    public double aggregatedPublishScheduleLatency95pct;
    public double aggregatedPublishScheduleLatency99pct;
    public double aggregatedPublishScheduleLatency999pct;
    public double aggregatedPublishScheduleLatency9999pct;
    public double aggregatedPublishScheduleLatencyMax;

    public double aggregatedPublishDelayLatencyAvg;
    public long aggregatedPublishDelayLatency50pct;
    public long aggregatedPublishDelayLatency75pct;
    public long aggregatedPublishDelayLatency95pct;
    public long aggregatedPublishDelayLatency99pct;
    public long aggregatedPublishDelayLatency999pct;
    public long aggregatedPublishDelayLatency9999pct;
    public long aggregatedPublishDelayLatencyMax;

    public Map<Double, Double> aggregatedPublishLatencyQuantiles = new TreeMap<>();

    public Map<Double, Double> aggregatedPublishScheduleLatencyQuantiles = new TreeMap<>();

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
     * @param periodPhase the run phase the interval was recorded in
     * @param periodTargetRate the target publish rate at the end of the interval
     * @param periodStage the running stage index, or null for a workload without stages
     */
    void recordPeriod(
            PeriodReport period,
            RunPhase periodPhase,
            double periodTargetRate,
            @Nullable Integer periodStage) {
        phase.add(periodPhase);
        targetRate.add(periodTargetRate);
        if (periodStage != null) {
            stage.add(periodStage);
        }
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
        appendMillis(
                period.publishScheduleLatency(),
                publishScheduleLatencyAvg,
                publishScheduleLatencyMax,
                publishScheduleLatency50pct,
                publishScheduleLatency75pct,
                publishScheduleLatency95pct,
                publishScheduleLatency99pct,
                publishScheduleLatency999pct,
                publishScheduleLatency9999pct);
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
                .forEach(value ->
                        aggregatedPublishLatencyQuantiles.put(
                                value.getPercentile(), millis(value.getValueIteratedTo())));

        Histogram schedule = aggregate.publishScheduleLatency;
        aggregatedPublishScheduleLatencyAvg = millis(schedule.getMean());
        aggregatedPublishScheduleLatency50pct = millisAt(schedule, 50);
        aggregatedPublishScheduleLatency75pct = millisAt(schedule, 75);
        aggregatedPublishScheduleLatency95pct = millisAt(schedule, 95);
        aggregatedPublishScheduleLatency99pct = millisAt(schedule, 99);
        aggregatedPublishScheduleLatency999pct = millisAt(schedule, 99.9);
        aggregatedPublishScheduleLatency9999pct = millisAt(schedule, 99.99);
        aggregatedPublishScheduleLatencyMax = millis(schedule.getMaxValue());
        schedule
                .percentiles(100)
                .forEach(value ->
                        aggregatedPublishScheduleLatencyQuantiles.put(
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
                .forEach(value ->
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
                .forEach(value ->
                        aggregatedEndToEndLatencyQuantiles.put(
                                value.getPercentile(), millis(value.getValueIteratedTo())));
    }

    void recordSummary(CountersStats counters, double targetRate, long maxBacklog) {
        double publishRateAvg = average(measured(phase, publishRate));
        double consumeRateAvg = average(measured(phase, consumeRate));
        summary =
                new ResultSummary(
                        publishRateAvg,
                        consumeRateAvg,
                        consumeRateAvg * messageSize / 1_000_000.0,
                        counters.messagesSent(),
                        counters.messageSendErrors(),
                        maxBacklog,
                        targetRate);
    }

    void recordHistogramData(CumulativeLatencies aggregate) {
        histograms =
                Map.of(
                        PUBLISH_HISTOGRAM, encode(aggregate.publishLatency),
                        PUBLISH_DELAY_HISTOGRAM, encode(aggregate.publishDelayLatency),
                        END_TO_END_HISTOGRAM, encode(aggregate.endToEndLatency));
    }

    /**
     * Averages a per-period series, such as {@link #publishRate}.
     *
     * @param values the series
     * @return the mean, or 0 for an empty series
     */
    public static double average(List<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
    }

    /**
     * The measured part of a per-period series: the periods after the warm-up. Results written before
     * phases were recorded have no {@link #phase} and are measured throughout.
     *
     * @param <T> the series element type
     * @param phases the {@link #phase} series
     * @param values a per-period series of the same result
     * @return the values recorded after the warm-up
     */
    public static <T> List<T> measured(List<RunPhase> phases, List<T> values) {
        int warmup = (int) phases.stream().takeWhile(RunPhase.WARMUP::equals).count();
        return values.subList(Math.min(warmup, values.size()), values.size());
    }

    /**
     * Decodes one of the {@link #histograms}, the inverse of how they are written.
     *
     * @param encoded the Base64 of the compressed histogram
     * @return the histogram, in microseconds
     * @throws IllegalArgumentException if the value is not a valid encoded histogram
     */
    public static Histogram decodeHistogram(String encoded) {
        try {
            return Histogram.decodeFromCompressedByteBuffer(
                    ByteBuffer.wrap(Base64.getDecoder().decode(encoded)), Long.MIN_VALUE);
        } catch (DataFormatException | RuntimeException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private static String encode(Histogram histogram) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        while (true) {
            int written = histogram.encodeIntoCompressedByteBuffer(buffer);
            if (written < buffer.capacity()) {
                buffer.flip();
                byte[] encoded = new byte[buffer.remaining()];
                buffer.get(encoded);
                return Base64.getEncoder().encodeToString(encoded);
            }
            buffer = ByteBuffer.allocate(buffer.capacity() * 2);
        }
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

    /**
     * Reads a percentile of a microsecond histogram in milliseconds.
     *
     * @param micros the histogram, in microseconds
     * @param percentile the percentile; 100 or more reads the maximum
     * @return the value, in milliseconds
     */
    public static double millisAt(Histogram micros, double percentile) {
        return millis(
                percentile >= 100 ? micros.getMaxValue() : micros.getValueAtPercentile(percentile));
    }

    private static double millis(double micros) {
        return micros / 1000.0;
    }

    // Histogram values are long microseconds; the double result may lose precision above 2^53
    private static double millis(long micros) {
        return millis((double) micros);
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
