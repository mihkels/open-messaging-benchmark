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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.mihkels.omb.driver.RebalanceEvent;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A consumer's partition assignment change, as reported by a worker and listed in the result.
 *
 * @param period index of the reporting period it was collected in; set by the coordinator
 * @param timestampMillis wall-clock time of the change, in epoch milliseconds
 * @param worker the worker whose consumer saw the change
 * @param topic the consumer's topic
 * @param kind what happened to the partitions
 * @param partitions the partitions concerned
 */
public record RebalanceRecord(
        @JsonProperty("period") @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable Integer period,
        @JsonProperty("timestampMillis") long timestampMillis,
        @JsonProperty("worker") @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable String worker,
        @JsonProperty("topic") String topic,
        @JsonProperty("kind") RebalanceEvent.Kind kind,
        @JsonProperty("partitions") List<Integer> partitions) {

    public RebalanceRecord {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(kind, "kind");
        partitions = partitions == null ? List.of() : List.copyOf(partitions);
    }

    /**
     * A worker-side record of a driver's event.
     *
     * @param worker the worker id
     * @param topic the consumer's topic
     * @param event the driver's event
     * @return the record
     */
    public static RebalanceRecord of(String worker, String topic, RebalanceEvent event) {
        return new RebalanceRecord(
                null, event.timestampMillis(), worker, topic, event.kind(), event.partitions());
    }

    public RebalanceRecord withWorker(String workerId) {
        return new RebalanceRecord(period, timestampMillis, workerId, topic, kind, partitions);
    }

    public RebalanceRecord withPeriod(int periodIndex) {
        return new RebalanceRecord(periodIndex, timestampMillis, worker, topic, kind, partitions);
    }
}
