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
package io.github.mihkels.omb.worker;

import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.RebalanceEvent;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Checks the {@link DeliveryStamp} of every received message against the sequence numbers already
 * seen for its producer on the same subscription, and counts new, duplicated and reordered messages
 * in {@link WorkerStats}. Consumers of one subscription on this worker share the state, so a
 * message delivered to two of them counts as a duplicate. Thread-safe.
 */
final class DeliveryVerifier {
    private final WorkerStats stats;
    // topic + subscription -> producer id -> the sequence numbers seen
    private final Map<String, Map<Long, SequenceWindow>> subscriptions = new ConcurrentHashMap<>();

    DeliveryVerifier(WorkerStats stats) {
        this.stats = stats;
    }

    /**
     * Returns the verification handle for one consumer's subscription, sharing sequence state across
     * all consumers of the same {@code (topic, subscription)} on this worker.
     *
     * @param topic the consumer's topic
     * @param subscription the consumer's subscription
     * @return the subscription verifier
     */
    Subscription subscription(String topic, String subscription) {
        return new Subscription(
                this,
                subscriptions.computeIfAbsent(topic + '\0' + subscription, k -> new ConcurrentHashMap<>()));
    }

    /**
     * Wraps a consumer's callback so each stamped message it receives is checked after the delegate
     * has recorded it.
     *
     * @param topic the consumer's topic
     * @param subscription the consumer's subscription
     * @param delegate the callback that records the message
     * @return the verifying callback
     */
    ConsumerCallback callback(String topic, String subscription, ConsumerCallback delegate) {
        Subscription sub = subscription(topic, subscription);
        return new ConsumerCallback() {
            @Override
            public void messageReceived(byte[] payload, long publishTimestamp) {
                delegate.messageReceived(payload, publishTimestamp);
                sub.verify(payload);
            }

            @Override
            public void messageReceived(byte[] payload, long publishTimestamp, int partition) {
                delegate.messageReceived(payload, publishTimestamp, partition);
                sub.verify(payload);
            }

            @Override
            public void messageReceived(ByteBuffer payload, long publishTimestamp) {
                // Read before the delegate, which may consume the buffer
                boolean stamped = DeliveryStamp.isStamped(payload);
                long producerId = stamped ? DeliveryStamp.producerId(payload) : 0;
                long sequence = stamped ? DeliveryStamp.sequence(payload) : 0;
                delegate.messageReceived(payload, publishTimestamp);
                if (stamped) {
                    sub.record(producerId, sequence);
                }
            }

            @Override
            public void messageReceived(ByteBuffer payload, long publishTimestamp, int partition) {
                boolean stamped = DeliveryStamp.isStamped(payload);
                long producerId = stamped ? DeliveryStamp.producerId(payload) : 0;
                long sequence = stamped ? DeliveryStamp.sequence(payload) : 0;
                delegate.messageReceived(payload, publishTimestamp, partition);
                if (stamped) {
                    sub.record(producerId, sequence);
                }
            }

            @Override
            public void partitionsChanged(RebalanceEvent event) {
                delegate.partitionsChanged(event);
            }
        };
    }

    /** Per-subscription verification handle invoked directly inside {@link RunCallbacks}. */
    static final class Subscription {
        private final DeliveryVerifier verifier;
        private final Map<Long, SequenceWindow> producers;

        private Subscription(DeliveryVerifier verifier, Map<Long, SequenceWindow> producers) {
            this.verifier = verifier;
            this.producers = producers;
        }

        void verify(byte[] payload) {
            if (DeliveryStamp.isStamped(payload)) {
                record(DeliveryStamp.producerId(payload), DeliveryStamp.sequence(payload));
            }
        }

        void verify(ByteBuffer payload) {
            if (DeliveryStamp.isStamped(payload)) {
                record(DeliveryStamp.producerId(payload), DeliveryStamp.sequence(payload));
            }
        }

        private void record(long producerId, long sequence) {
            verifier.record(producers, producerId, sequence);
        }
    }

    private void record(Map<Long, SequenceWindow> producers, long producerId, long sequence) {
        stats.recordDelivery(
                producers.computeIfAbsent(producerId, k -> new SequenceWindow()).record(sequence));
    }

    /** What one received message turned out to be. */
    enum Outcome {
        NEW,
        NEW_OUT_OF_ORDER,
        DUPLICATE
    }

    /**
     * The sequence numbers seen from one producer: every number below {@code base} counts as seen,
     * and a bit set records the {@link #SIZE} numbers from {@code base}. Memory stays at {@code SIZE
     * / 8} bytes; a number that arrives more than {@code SIZE} behind the highest one counts as a
     * duplicate.
     */
    static final class SequenceWindow {
        static final int SIZE = 1 << 16;
        private final long[] seen = new long[SIZE / Long.SIZE];
        private long base;
        private long highest = -1;

        synchronized Outcome record(long sequence) {
            if (sequence < base) {
                return Outcome.DUPLICATE;
            }
            if (sequence - base >= SIZE) {
                advance(sequence - SIZE + 1);
            }
            int bit = (int) (sequence & (SIZE - 1));
            long mask = 1L << bit;
            if ((seen[bit >>> 6] & mask) != 0) {
                return Outcome.DUPLICATE;
            }
            seen[bit >>> 6] |= mask;
            if (sequence < highest) {
                return Outcome.NEW_OUT_OF_ORDER;
            }
            highest = sequence;
            return Outcome.NEW;
        }

        // Moves base up, clearing the bits of the numbers that leave the window
        private void advance(long newBase) {
            if (newBase - base >= SIZE) {
                Arrays.fill(seen, 0);
            } else {
                for (long s = base; s < newBase; s++) {
                    int bit = (int) (s & (SIZE - 1));
                    seen[bit >>> 6] &= ~(1L << bit);
                }
            }
            base = newBase;
        }
    }
}
