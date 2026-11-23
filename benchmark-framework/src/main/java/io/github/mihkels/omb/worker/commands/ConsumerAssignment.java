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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConsumerAssignment {
    public List<TopicSubscription> topicsSubscriptions = new ArrayList<>();

    /** Whether consumers check the delivery stamp of each message; see {@code verifyDelivery}. */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean verifyDelivery;

    /** Whether consumers report partitions and rebalances; see {@code partitionTelemetry}. */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean partitionTelemetry;

    /**
     * Whether the workload assigns more than one consumer to the same subscription, possibly across
     * different workers; see {@code consumerPerSubscription}.
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean sharedSubscriptions;

    /**
     * Returns whether any subscription in this assignment is shared across multiple consumers, either
     * according to {@link #sharedSubscriptions} or via duplicate entries in {@link
     * #topicsSubscriptions}.
     *
     * @return {@code true} if multiple consumers share a subscription
     */
    public boolean hasSharedSubscriptions() {
        if (sharedSubscriptions) {
            return true;
        }
        Set<List<String>> seen = new HashSet<>(topicsSubscriptions.size());
        for (TopicSubscription subscription : topicsSubscriptions) {
            if (!seen.add(List.of(subscription.topic, subscription.subscription))) {
                return true;
            }
        }
        return false;
    }
}
