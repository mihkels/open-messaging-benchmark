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
package io.openmessaging.benchmark.worker.commands;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.HdrHistogram.Histogram;
import org.jspecify.annotations.Nullable;

public record PeriodStats(
        @JsonProperty("messagesSent") long messagesSent,
        @JsonProperty("messageSendErrors") long messageSendErrors,
        @JsonProperty("bytesSent") long bytesSent,
        @JsonProperty("messagesReceived") long messagesReceived,
        @JsonProperty("bytesReceived") long bytesReceived,
        @JsonProperty("totalMessagesSent") long totalMessagesSent,
        @JsonProperty("totalMessageSendErrors") long totalMessageSendErrors,
        @JsonProperty("totalMessagesReceived") long totalMessagesReceived,
        @JsonProperty("publishLatency") Histogram publishLatency,
        @JsonProperty("publishDelayLatency") Histogram publishDelayLatency,
        @JsonProperty("endToEndLatency") Histogram endToEndLatency,
        @JsonProperty("publishScheduleLatency") Histogram publishScheduleLatency,
        // Last send failure per worker id; omitted when empty so older readers see the old shape
        @JsonProperty("sendErrorReasons") @JsonInclude(JsonInclude.Include.NON_EMPTY)
                Map<String, String> sendErrorReasons,
        // Worker CPU and GC load; absent from older workers
        @JsonProperty("load") @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable WorkerLoad load) {

    // Defensive copy constructor
    @SuppressWarnings("checkstyle:ParameterNumber")
    public PeriodStats(
            long messagesSent,
            long messageSendErrors,
            long bytesSent,
            long messagesReceived,
            long bytesReceived,
            long totalMessagesSent,
            long totalMessageSendErrors,
            long totalMessagesReceived,
            @Nullable Histogram publishLatency,
            @Nullable Histogram publishDelayLatency,
            @Nullable Histogram endToEndLatency,
            @Nullable Histogram publishScheduleLatency,
            @Nullable Map<String, String> sendErrorReasons,
            @Nullable WorkerLoad load) {
        this.messagesSent = messagesSent;
        this.messageSendErrors = messageSendErrors;
        this.bytesSent = bytesSent;
        this.messagesReceived = messagesReceived;
        this.bytesReceived = bytesReceived;
        this.totalMessagesSent = totalMessagesSent;
        this.totalMessageSendErrors = totalMessageSendErrors;
        this.totalMessagesReceived = totalMessagesReceived;
        this.publishLatency = publishLatency != null ? publishLatency.copy() : new Histogram(5);
        this.publishDelayLatency =
                publishDelayLatency != null ? publishDelayLatency.copy() : new Histogram(5);
        this.endToEndLatency = endToEndLatency != null ? endToEndLatency.copy() : new Histogram(5);
        this.publishScheduleLatency =
                publishScheduleLatency != null ? publishScheduleLatency.copy() : new Histogram(5);
        this.sendErrorReasons = sendErrorReasons != null ? Map.copyOf(sendErrorReasons) : Map.of();
        this.load = load;
    }

    public PeriodStats() {
        this(0, 0, 0, 0, 0, 0, 0, 0, null, null, null, null, null, null);
    }

    // Return defensive copies in getters
    @Override
    public Histogram publishLatency() {
        return publishLatency.copy();
    }

    @Override
    public Histogram publishDelayLatency() {
        return publishDelayLatency.copy();
    }

    @Override
    public Histogram publishScheduleLatency() {
        return publishScheduleLatency.copy();
    }

    @Override
    public Histogram endToEndLatency() {
        return endToEndLatency.copy();
    }

    /** Returns a copy whose send error reasons are all tagged with the given worker id. */
    public PeriodStats withWorkerId(String workerId) {
        if (sendErrorReasons.isEmpty()) {
            return this;
        }
        return new PeriodStats(
                messagesSent,
                messageSendErrors,
                bytesSent,
                messagesReceived,
                bytesReceived,
                totalMessagesSent,
                totalMessageSendErrors,
                totalMessagesReceived,
                publishLatency,
                publishDelayLatency,
                endToEndLatency,
                publishScheduleLatency,
                Map.of(workerId, String.join("; ", sendErrorReasons.values())),
                load);
    }

    /**
     * Combines this PeriodStats with another, returning a new record with the summed values.
     *
     * @param other The other PeriodStats to add.
     * @return A new PeriodStats instance with the combined values.
     */
    public PeriodStats plus(PeriodStats other) {
        return aggregate(List.of(this, other));
    }

    private static Map<String, String> merge(List<PeriodStats> snapshots) {
        Map<String, String> reasons = new HashMap<>();
        snapshots.forEach(snapshot -> reasons.putAll(snapshot.sendErrorReasons));
        return reasons;
    }

    /** Combines immutable snapshots without allocating intermediate combined snapshots. */
    public static PeriodStats aggregate(List<PeriodStats> snapshots) {
        if (snapshots.size() == 1) {
            return Objects.requireNonNull(snapshots.getFirst());
        }
        long sent = 0;
        long errors = 0;
        long sentBytes = 0;
        long received = 0;
        long receivedBytes = 0;
        long totalSent = 0;
        long totalErrors = 0;
        long totalReceived = 0;
        Histogram publish = new Histogram(5);
        Histogram delay = new Histogram(5);
        Histogram schedule = new Histogram(5);
        Histogram endToEnd = new Histogram(5);
        WorkerLoad load = null;
        for (PeriodStats snapshot : snapshots) {
            sent += snapshot.messagesSent;
            errors += snapshot.messageSendErrors;
            sentBytes += snapshot.bytesSent;
            received += snapshot.messagesReceived;
            receivedBytes += snapshot.bytesReceived;
            totalSent += snapshot.totalMessagesSent;
            totalErrors += snapshot.totalMessageSendErrors;
            totalReceived += snapshot.totalMessagesReceived;
            publish.add(snapshot.publishLatency);
            delay.add(snapshot.publishDelayLatency);
            schedule.add(snapshot.publishScheduleLatency);
            endToEnd.add(snapshot.endToEndLatency);
            load = WorkerLoad.max(load, snapshot.load);
        }
        return new PeriodStats(
                sent,
                errors,
                sentBytes,
                received,
                receivedBytes,
                totalSent,
                totalErrors,
                totalReceived,
                publish,
                delay,
                endToEnd,
                schedule,
                merge(snapshots),
                load);
    }
}
