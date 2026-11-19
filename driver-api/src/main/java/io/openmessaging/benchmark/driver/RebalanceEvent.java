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
package io.openmessaging.benchmark.driver;

import java.util.List;
import java.util.Objects;

/**
 * A change to a consumer's partition assignment, reported through {@link
 * ConsumerCallback#partitionsChanged(RebalanceEvent)}. The topic is the consumer's own.
 *
 * @param kind what happened to the partitions
 * @param timestampMillis wall-clock time of the change, in epoch milliseconds
 * @param partitions the partitions concerned
 */
public record RebalanceEvent(Kind kind, long timestampMillis, List<Integer> partitions) {

    /** What happened to the partitions. */
    public enum Kind {
        /** The partitions were assigned to the consumer. */
        ASSIGNED,
        /** The partitions were revoked from the consumer in an orderly rebalance. */
        REVOKED,
        /** The partitions were lost without an orderly revocation, for example after a timeout. */
        LOST
    }

    public RebalanceEvent {
        Objects.requireNonNull(kind, "kind");
        partitions = List.copyOf(partitions);
    }
}
