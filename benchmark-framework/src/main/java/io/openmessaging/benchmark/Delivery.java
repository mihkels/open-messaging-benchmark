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

import io.openmessaging.benchmark.worker.commands.CountersStats;

/**
 * What {@code verifyDelivery} found once the consumers caught up after the run. Counts cover the
 * whole run, warm-up included.
 *
 * @param sent acknowledged messages that carried a delivery stamp
 * @param received stamped messages received for the first time, summed over subscriptions
 * @param lost copies acknowledged but never received: {@code sent} times the subscriptions per
 *     topic, less {@code received}
 * @param duplicated stamped messages received again on the same subscription
 * @param outOfOrder stamped messages received after a later one from the same producer
 */
public record Delivery(long sent, long received, long lost, long duplicated, long outOfOrder) {

    /**
     * Derives the delivery counts from the workers' run totals.
     *
     * @param stats the run totals, read after the drain
     * @param subscriptionsPerTopic the subscriptions that each receive every message
     * @return the delivery counts
     */
    static Delivery of(CountersStats stats, int subscriptionsPerTopic) {
        // A failed send that still reached the broker can make received exceed the expected copies
        long lost = Math.max(0, stats.verifiedSent() * subscriptionsPerTopic - stats.delivered());
        return new Delivery(
                stats.verifiedSent(), stats.delivered(), lost, stats.duplicated(), stats.outOfOrder());
    }
}
