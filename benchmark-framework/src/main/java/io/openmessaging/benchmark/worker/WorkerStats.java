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
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.Failures;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.RunPhase;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.jspecify.annotations.Nullable;

public class WorkerStats {

    // Micrometer registry
    private final MeterRegistry registry;

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
    private final Counter verifiedSentCounter;
    private final Counter deliveredCounter;
    private final Counter duplicatedCounter;
    private final Counter outOfOrderCounter;

    // Local accumulators used for periodic snapshots
    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder messageSendErrors = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();

    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();

    private final LongAdder totalMessagesSent = new LongAdder();
    private final LongAdder totalMessageSendErrors = new LongAdder();
    private final LongAdder totalMessagesReceived = new LongAdder();

    // verifyDelivery totals for the whole run, warm-up included; resetLatencies keeps them
    private final LongAdder verifiedSent = new LongAdder();
    private final LongAdder delivered = new LongAdder();
    private final LongAdder duplicated = new LongAdder();
    private final LongAdder outOfOrder = new LongAdder();

    private volatile @Nullable RunPhase phase;
    // Last send failure as "class: message"; kept until reset so every period reports it.
    private volatile @Nullable String lastSendError;

    WorkerStats(MeterRegistry registry) {
        this.registry = registry;
        for (RunPhase p : RunPhase.values()) {
            Gauge.builder("benchmark_phase", () -> phase == p ? 1 : 0)
                    .description("1 while the run is in this phase, else 0")
                    .tag("phase", p.name().toLowerCase(Locale.ROOT))
                    .register(registry);
        }

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
        this.verifiedSentCounter =
                Counter.builder("delivery_sent")
                        .description("Acknowledged messages stamped by verifyDelivery")
                        .register(registry);
        this.deliveredCounter =
                Counter.builder("delivery_received")
                        .description("Stamped messages received for the first time per subscription")
                        .register(registry);
        this.duplicatedCounter =
                Counter.builder("delivery_duplicated")
                        .description("Stamped messages received again on the same subscription")
                        .register(registry);
        this.outOfOrderCounter =
                Counter.builder("delivery_out_of_order")
                        .description("Stamped messages received after a later one from the same producer")
                        .register(registry);

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
                LatencyMetric.skewTolerant(
                        TimeUnit.HOURS.toMicros(12), "e2e_latency", "End to end latency (micros)", registry);
    }

    public MeterRegistry getMeterRegistry() {
        return registry;
    }

    public void setRunId(String runId) {
        registry.config().commonTags("run_id", runId);
    }

    public void setPhase(RunPhase phase) {
        this.phase = phase;
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
                publishScheduleLatency.takeInterval(),
                lastSendError == null ? Map.of() : Map.of(LocalWorker.ID, lastSendError));
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
                totalMessagesSent.sum(),
                totalMessageSendErrors.sum(),
                totalMessagesReceived.sum(),
                verifiedSent.sum(),
                delivered.sum(),
                duplicated.sum(),
                outOfOrder.sum());
    }

    void recordVerifiedSend() {
        verifiedSent.increment();
        verifiedSentCounter.increment();
    }

    void recordDelivery(DeliveryVerifier.Outcome outcome) {
        if (outcome == DeliveryVerifier.Outcome.DUPLICATE) {
            duplicated.increment();
            duplicatedCounter.increment();
            return;
        }
        delivered.increment();
        deliveredCounter.increment();
        if (outcome == DeliveryVerifier.Outcome.NEW_OUT_OF_ORDER) {
            outOfOrder.increment();
            outOfOrderCounter.increment();
        }
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
        verifiedSent.reset();
        delivered.reset();
        duplicated.reset();
        outOfOrder.reset();
        lastSendError = null;
    }

    public void recordProducerFailure(Throwable failure) {
        lastSendError = Failures.unwrap(failure).toString();
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
