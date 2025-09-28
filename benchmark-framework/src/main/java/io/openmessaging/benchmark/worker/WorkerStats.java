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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Recorder;

public class WorkerStats {

    // Micrometer registry
    private final PrometheusMeterRegistry registry;

    // HDR histograms (kept for detailed latency exports)
    private static final long highestTrackableValue = TimeUnit.SECONDS.toMicros(60);
    private final Recorder publishLatencyRecorder = new Recorder(highestTrackableValue, 5);
    private final Recorder cumulativePublishLatencyRecorder = new Recorder(highestTrackableValue, 5);
    private final Recorder publishDelayLatencyRecorder = new Recorder(highestTrackableValue, 5);
    private final Recorder cumulativePublishDelayLatencyRecorder =
            new Recorder(highestTrackableValue, 5);

    private final Recorder endToEndLatencyRecorder = new Recorder(TimeUnit.HOURS.toMicros(12), 5);
    private final Recorder endToEndCumulativeLatencyRecorder =
            new Recorder(TimeUnit.HOURS.toMicros(12), 5);

    // Micrometer counters
    private final Counter messagesSentCounter;
    private final Counter messageSendErrorCounter;
    private final Counter bytesSentCounter;
    private final Counter messagesReceivedCounter;
    private final Counter bytesReceivedCounter;

    // Micrometer timers
    private final Timer publishLatencyTimer;
    private final Timer publishDelayLatencyTimer;
    private final Timer endToEndLatencyTimer;

    // Local accumulators used for periodic snapshots
    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder messageSendErrors = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();

    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();

    private final LongAdder totalMessagesSent = new LongAdder();
    private final LongAdder totalMessageSendErrors = new LongAdder();
    private final LongAdder totalMessagesReceived = new LongAdder();

    WorkerStats(PrometheusMeterRegistry registry) {
        this.registry = registry;

        // Counters
        this.messagesSentCounter =
                Counter.builder("messages_sent").description("Producer messages sent").register(registry);
        this.messageSendErrorCounter =
                Counter.builder("message_send_errors")
                        .description("Producer send errors")
                        .register(registry);
        this.bytesSentCounter =
                Counter.builder("bytes_sent").description("Producer bytes sent").register(registry);
        this.messagesReceivedCounter =
                Counter.builder("messages_recv")
                        .description("Consumer messages received")
                        .register(registry);
        this.bytesReceivedCounter =
                Counter.builder("bytes_recv").description("Consumer bytes received").register(registry);

        // Timers
        this.publishLatencyTimer =
                Timer.builder("produce_latency").description("Publish latency (micros)").register(registry);
        this.publishDelayLatencyTimer =
                Timer.builder("producer_delay_latency")
                        .description("Delay between intended and actual send (micros)")
                        .register(registry);
        this.endToEndLatencyTimer =
                Timer.builder("e2e_latency").description("End to end latency (micros)").register(registry);
    }

    public PrometheusMeterRegistry getMeterRegistry() {
        return registry;
    }

    public void recordMessageSent() {
        totalMessagesSent.increment();
    }

    public void recordMessageReceived(long payloadLength, long endToEndLatencyMicros) {
        messagesReceived.increment();
        totalMessagesReceived.increment();
        messagesReceivedCounter.increment();
        bytesReceived.add(payloadLength);
        bytesReceivedCounter.increment(payloadLength);

        if (endToEndLatencyMicros > 0) {
            endToEndCumulativeLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyTimer.record(endToEndLatencyMicros, TimeUnit.MICROSECONDS);
        }
    }

    public PeriodStats toPeriodStats() {
        return new PeriodStats(
                messagesSent.sumThenReset(),
                messageSendErrors.sumThenReset(),
                bytesSent.sumThenReset(),
                messagesReceived.sumThenReset(),
                bytesReceived.sumThenReset(),
                totalMessagesSent.sum(),
                totalMessageSendErrors.sum(),
                totalMessagesReceived.sum(),
                publishLatencyRecorder.getIntervalHistogram(),
                publishDelayLatencyRecorder.getIntervalHistogram(),
                endToEndLatencyRecorder.getIntervalHistogram());
    }

    public CumulativeLatencies toCumulativeLatencies() {
        CumulativeLatencies latencies = new CumulativeLatencies();
        latencies.publishLatency = cumulativePublishLatencyRecorder.getIntervalHistogram();
        latencies.publishDelayLatency = cumulativePublishDelayLatencyRecorder.getIntervalHistogram();
        latencies.endToEndLatency = endToEndCumulativeLatencyRecorder.getIntervalHistogram();
        return latencies;
    }

    public CountersStats toCountersStats() {
        return new CountersStats(
                totalMessagesSent.sum(), totalMessageSendErrors.sum(), totalMessagesReceived.sum());
    }

    public void resetLatencies() {
        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        publishDelayLatencyRecorder.reset();
        cumulativePublishDelayLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();
    }

    public void reset() {
        resetLatencies();
        messagesSent.reset();
        messageSendErrors.reset();
        bytesSent.reset();
        messagesReceived.reset();
        bytesReceived.reset();
        totalMessagesSent.reset();
        totalMessagesReceived.reset();
    }

    public void recordProducerFailure() {
        messageSendErrors.increment();
        messageSendErrorCounter.increment();
        totalMessageSendErrors.increment();
    }

    public void recordProducerSuccess(
            long payloadLength, long intendedSendTimeNs, long sendTimeNs, long nowNs) {
        messagesSent.increment();
        totalMessagesSent.increment();
        messagesSentCounter.increment();
        bytesSent.add(payloadLength);
        bytesSentCounter.increment(payloadLength);

        final long latencyMicros =
                Math.min(highestTrackableValue, TimeUnit.NANOSECONDS.toMicros(nowNs - sendTimeNs));
        publishLatencyRecorder.recordValue(latencyMicros);
        cumulativePublishLatencyRecorder.recordValue(latencyMicros);
        publishLatencyTimer.record(latencyMicros, TimeUnit.MICROSECONDS);

        final long sendDelayMicros =
                Math.min(
                        highestTrackableValue, TimeUnit.NANOSECONDS.toMicros(sendTimeNs - intendedSendTimeNs));
        publishDelayLatencyRecorder.recordValue(sendDelayMicros);
        cumulativePublishDelayLatencyRecorder.recordValue(sendDelayMicros);
        publishDelayLatencyTimer.record(sendDelayMicros, TimeUnit.MICROSECONDS);
    }
}
