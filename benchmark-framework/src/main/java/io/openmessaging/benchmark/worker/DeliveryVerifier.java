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

import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.RebalanceEvent;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

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
     * Wraps a consumer's callback so each stamped message it receives is checked after the delegate
     * has recorded it.
     *
     * @param topic the consumer's topic
     * @param subscription the consumer's subscription
     * @param delegate the callback that records the message
     * @return the verifying callback
     */
    ConsumerCallback callback(String topic, String subscription, ConsumerCallback delegate) {
        Map<Long, SequenceWindow> producers =
                subscriptions.computeIfAbsent(topic + '\0' + subscription, k -> new ConcurrentHashMap<>());
        return new ConsumerCallback() {
            @Override
            public void messageReceived(byte[] payload, long publishTimestamp) {
                delegate.messageReceived(payload, publishTimestamp);
                verify(producers, payload);
            }

            @Override
            public void messageReceived(byte[] payload, long publishTimestamp, int partition) {
                delegate.messageReceived(payload, publishTimestamp, partition);
                verify(producers, payload);
            }

            @Override
            public void messageReceived(ByteBuffer payload, long publishTimestamp) {
                // Read before the delegate, which may consume the buffer
                long[] stamp = stamp(payload);
                delegate.messageReceived(payload, publishTimestamp);
                verify(producers, stamp);
            }

            @Override
            public void messageReceived(ByteBuffer payload, long publishTimestamp, int partition) {
                long[] stamp = stamp(payload);
                delegate.messageReceived(payload, publishTimestamp, partition);
                verify(producers, stamp);
            }

            @Override
            public void partitionsChanged(RebalanceEvent event) {
                delegate.partitionsChanged(event);
            }
        };
    }

    private void verify(Map<Long, SequenceWindow> producers, byte[] payload) {
        if (DeliveryStamp.isStamped(payload)) {
            record(producers, DeliveryStamp.producerId(payload), DeliveryStamp.sequence(payload));
        }
    }

    // The producer id and sequence of a stamped buffer, or null for an unstamped one
    private static long @Nullable [] stamp(ByteBuffer payload) {
        return DeliveryStamp.isStamped(payload)
                ? new long[] {DeliveryStamp.producerId(payload), DeliveryStamp.sequence(payload)}
                : null;
    }

    private void verify(Map<Long, SequenceWindow> producers, long @Nullable [] stamp) {
        if (stamp != null) {
            record(producers, stamp[0], stamp[1]);
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
