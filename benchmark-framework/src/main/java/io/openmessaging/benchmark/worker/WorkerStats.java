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
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class WorkerStats {

    // Micrometer registry
    private final PrometheusMeterRegistry registry;

    private final LatencyMetric publishLatency;
    private final LatencyMetric publishDelayLatency;
    private final LatencyMetric publishScheduleLatency;
    private final LatencyMetric endToEndLatency;

    // Micrometer counters
    private final Counter messagesSentCounter;
    private final Counter messageSendErrorCounter;
    private final Counter bytesSentCounter;
    private final Counter messagesReceivedCounter;
    private final Counter bytesReceivedCounter;

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

        this.publishLatency =
                LatencyMetric.clamped(
                        TimeUnit.SECONDS.toMicros(60), "produce_latency", "Publish latency (micros)", registry);
        this.publishDelayLatency =
                LatencyMetric.clamped(
                        TimeUnit.SECONDS.toMicros(60),
                        "producer_delay_latency",
                        "Delay between intended and actual send (micros)",
                        registry);
        this.publishScheduleLatency =
                LatencyMetric.clamped(
                        TimeUnit.SECONDS.toMicros(60),
                        "produce_schedule_latency",
                        "Publish latency from intended send to acknowledgement (micros)",
                        registry);
        this.endToEndLatency =
                LatencyMetric.positiveOnly(
                        TimeUnit.HOURS.toMicros(12), "e2e_latency", "End to end latency (micros)", registry);
    }

    public PrometheusMeterRegistry getMeterRegistry() {
        return registry;
    }

    public void setRunId(String runId) {
        registry.config().commonTags("run_id", runId);
    }

    public void recordMessageSent() {
        totalMessagesSent.increment();
    }

    public void recordMessageReceived(long payloadLength, long endToEndLatencyMicros) {
        messagesReceived.increment();
        totalMessagesReceived.increment();
        messagesReceivedCounter.increment();
        bytesReceived.add(payloadLength);
        bytesReceivedCounter.increment((double) payloadLength);
        endToEndLatency.record(endToEndLatencyMicros);
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
                publishLatency.takeInterval(),
                publishDelayLatency.takeInterval(),
                endToEndLatency.takeInterval(),
                publishScheduleLatency.takeInterval());
    }

    public CumulativeLatencies toCumulativeLatencies() {
        CumulativeLatencies latencies = new CumulativeLatencies();
        latencies.publishLatency = publishLatency.takeCumulative();
        latencies.publishDelayLatency = publishDelayLatency.takeCumulative();
        latencies.publishScheduleLatency = publishScheduleLatency.takeCumulative();
        latencies.endToEndLatency = endToEndLatency.takeCumulative();
        return latencies;
    }

    public CountersStats toCountersStats() {
        return new CountersStats(
                totalMessagesSent.sum(), totalMessageSendErrors.sum(), totalMessagesReceived.sum());
    }

    public void resetLatencies() {
        publishLatency.reset();
        publishDelayLatency.reset();
        publishScheduleLatency.reset();
        endToEndLatency.reset();
    }

    public void reset() {
        resetLatencies();
        messagesSent.reset();
        messageSendErrors.reset();
        bytesSent.reset();
        messagesReceived.reset();
        bytesReceived.reset();
        totalMessagesSent.reset();
        totalMessageSendErrors.reset();
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
        bytesSentCounter.increment((double) payloadLength);
        publishLatency.record(TimeUnit.NANOSECONDS.toMicros(nowNs - sendTimeNs));
        publishDelayLatency.record(TimeUnit.NANOSECONDS.toMicros(sendTimeNs - intendedSendTimeNs));
        publishScheduleLatency.record(TimeUnit.NANOSECONDS.toMicros(nowNs - intendedSendTimeNs));
    }
}
