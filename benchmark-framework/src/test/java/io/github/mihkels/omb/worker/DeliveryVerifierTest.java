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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.RebalanceEvent;
import io.github.mihkels.omb.worker.DeliveryVerifier.Outcome;
import io.github.mihkels.omb.worker.DeliveryVerifier.SequenceWindow;
import io.github.mihkels.omb.worker.commands.CountersStats;
import io.github.mihkels.omb.worker.commands.PartitionPeriod;
import io.github.mihkels.omb.worker.commands.PeriodStats;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DeliveryVerifierTest {

    @Test
    void stampsACopyAndReadsItBackFromArraysAndBuffers() {
        byte[] template = new byte[32];
        template[31] = 9;
        DeliveryStamp stamp = new DeliveryStamp(42);

        byte[] first = stamp.stamp(template);
        byte[] second = stamp.stamp(template);

        assertThat(DeliveryStamp.isStamped(template)).isFalse();
        assertThat(first).hasSize(32);
        assertThat(first[31]).isEqualTo((byte) 9);
        assertThat(DeliveryStamp.isStamped(first)).isTrue();
        assertThat(DeliveryStamp.producerId(first)).isEqualTo(42);
        assertThat(DeliveryStamp.sequence(first)).isZero();
        assertThat(DeliveryStamp.sequence(second)).isEqualTo(1);

        // A buffer at an offset and in little-endian order reads the same, and is left as it was
        ByteBuffer buffer = ByteBuffer.allocate(40).order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(8);
        buffer.put(second);
        buffer.position(8);
        assertThat(DeliveryStamp.isStamped(buffer)).isTrue();
        assertThat(DeliveryStamp.producerId(buffer)).isEqualTo(42);
        assertThat(DeliveryStamp.sequence(buffer)).isEqualTo(1);
        assertThat(buffer.position()).isEqualTo(8);
        assertThat(buffer.order()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    void unstampedAndShortPayloadsAreNotStamped() {
        assertThat(DeliveryStamp.isStamped(new byte[10])).isFalse();
        assertThat(DeliveryStamp.isStamped(new byte[32])).isFalse();
        assertThat(DeliveryStamp.isStamped(ByteBuffer.allocate(10))).isFalse();
        assertThat(DeliveryStamp.isStamped(ByteBuffer.allocate(32))).isFalse();
        assertThat(new DeliveryStamp().stamp(new byte[DeliveryStamp.LENGTH]))
                .hasSize(DeliveryStamp.LENGTH);
    }

    @Test
    void windowTellsNewReorderedAndDuplicateNumbers() {
        SequenceWindow window = new SequenceWindow();

        assertThat(window.record(0)).isEqualTo(Outcome.NEW);
        assertThat(window.record(2)).isEqualTo(Outcome.NEW);
        assertThat(window.record(1)).isEqualTo(Outcome.NEW_OUT_OF_ORDER);
        assertThat(window.record(2)).isEqualTo(Outcome.DUPLICATE);
        assertThat(window.record(0)).isEqualTo(Outcome.DUPLICATE);
    }

    @Test
    void windowSlidesAndCountsNumbersBehindItAsDuplicates() {
        SequenceWindow window = new SequenceWindow();
        int size = SequenceWindow.SIZE;
        window.record(0);
        window.record(5);

        // A small slide (base 0 -> 3) drops only the numbers that leave the window
        assertThat(window.record(size + 2)).isEqualTo(Outcome.NEW);
        assertThat(window.record(5)).isEqualTo(Outcome.DUPLICATE);
        assertThat(window.record(4)).isEqualTo(Outcome.NEW_OUT_OF_ORDER);
        assertThat(window.record(2)).isEqualTo(Outcome.DUPLICATE);
        // A jump past the whole window clears it
        assertThat(window.record(10L * size)).isEqualTo(Outcome.NEW);
        assertThat(window.record(10L * size - 1)).isEqualTo(Outcome.NEW_OUT_OF_ORDER);
        assertThat(window.record(size + 2)).isEqualTo(Outcome.DUPLICATE);
    }

    @Test
    void sharesStatePerSubscriptionAndIgnoresUnstampedMessages() {
        WorkerStats stats = new WorkerStats(new SimpleMeterRegistry());
        DeliveryVerifier verifier = new DeliveryVerifier(stats);
        ConsumerCallback delegate = mock(ConsumerCallback.class);
        ConsumerCallback first = verifier.callback("topic", "sub", delegate);
        ConsumerCallback sameSubscription = verifier.callback("topic", "sub", delegate);
        ConsumerCallback otherSubscription = verifier.callback("topic", "other", delegate);
        byte[] message = new DeliveryStamp(7).stamp(new byte[24]);

        first.messageReceived(message, 1);
        sameSubscription.messageReceived(ByteBuffer.wrap(message), 1);
        otherSubscription.messageReceived(message, 1);
        first.messageReceived(new byte[24], 1);
        first.messageReceived(ByteBuffer.allocate(4), 1);

        verify(delegate).messageReceived(new byte[24], 1);
        assertThat(stats.toCountersStats()).isEqualTo(new CountersStats(0, 0, 0, 0, 2, 1, 0));
        stats.reset();
        assertThat(stats.toCountersStats()).isEqualTo(new CountersStats());
    }

    @Test
    void dropsStampedDeliveriesAfterRunCallbacksAreSealed() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            WorkerStats stats = new WorkerStats(registry);
            DeliveryVerifier verifier = new DeliveryVerifier(stats);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            RunCallbacks callbacks = new RunCallbacks(stats, failure::set);
            ConsumerCallback callback =
                    callbacks.forConsumer(null, verifier.subscription("topic", "sub"));
            DeliveryStamp stamp = new DeliveryStamp(7);
            byte[] first = stamp.stamp(new byte[32]);
            byte[] duplicate = first.clone();
            long now = System.currentTimeMillis();

            callback.messageReceived(first, now);
            assertThat(stats.toCountersStats()).isEqualTo(new CountersStats(0, 0, 1, 0, 1, 0, 0));

            assertThat(callbacks.seal(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))).isTrue();
            stats.reset();

            callback.messageReceived(duplicate, now);
            callback.messageReceived(ByteBuffer.wrap(duplicate), now);
            assertThat(stats.toCountersStats()).isEqualTo(new CountersStats());
            assertThat(failure.get()).isNull();
        } finally {
            registry.close();
        }
    }

    @Test
    void verifiesDeliveryInsideGateBeforePauseBlocks() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            WorkerStats stats = new WorkerStats(registry);
            DeliveryVerifier verifier = new DeliveryVerifier(stats);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            RunCallbacks callbacks = new RunCallbacks(stats, failure::set);
            ConsumerCallback callback =
                    callbacks.forConsumer(null, verifier.subscription("topic", "sub"));
            byte[] message = new DeliveryStamp(9).stamp(new byte[32]);
            long now = System.currentTimeMillis();

            callbacks.pause();
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch finished = new CountDownLatch(1);
            Thread delivery =
                    Thread.ofVirtual()
                            .start(() -> {
                                started.countDown();
                                callback.messageReceived(ByteBuffer.wrap(message), now);
                                finished.countDown();
                            });

            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (stats.toCountersStats().delivered() == 0) {
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("Delivery was not verified before blocking on pause");
                }
                Thread.onSpinWait();
            }

            assertThat(finished.getCount()).isEqualTo(1);
            assertThat(stats.toCountersStats()).isEqualTo(new CountersStats(0, 0, 1, 0, 1, 0, 0));

            callbacks.resume();
            assertThat(finished.await(2, TimeUnit.SECONDS)).isTrue();
            delivery.join(TimeUnit.SECONDS.toMillis(2));
            assertThat(callbacks.awaitQuiescence(System.nanoTime() + TimeUnit.SECONDS.toNanos(1)))
                    .isTrue();
            assertThat(callbacks.seal(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))).isTrue();
            assertThat(failure.get()).isNull();
        } finally {
            registry.close();
        }
    }

    @Test
    void combinesDeliveryVerificationWithPartitionTelemetry() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            WorkerStats stats = new WorkerStats(registry);
            DeliveryVerifier verifier = new DeliveryVerifier(stats);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            RunCallbacks callbacks = new RunCallbacks(stats, failure::set);
            ConsumerCallback first =
                    callbacks.forConsumer("orders", verifier.subscription("orders", "sub"));
            ConsumerCallback second =
                    callbacks.forConsumer("orders", verifier.subscription("orders", "sub"));
            DeliveryStamp stamp = new DeliveryStamp(11);
            byte[] msg1 = stamp.stamp(new byte[32]);
            byte[] msg2 = stamp.stamp(new byte[32]);
            long now = System.currentTimeMillis();

            first.messageReceived(msg1, now, 3);
            second.messageReceived(ByteBuffer.wrap(msg1), now, 3);
            second.messageReceived(ByteBuffer.wrap(msg2), now, 1);
            first.partitionsChanged(new RebalanceEvent(RebalanceEvent.Kind.ASSIGNED, 5L, List.of(1, 3)));

            PeriodStats period = stats.toPeriodStats();
            assertThat(stats.toCountersStats()).isEqualTo(new CountersStats(0, 0, 3, 0, 2, 1, 0));
            assertThat(period.partitions())
                    .extracting(PartitionPeriod::partition, PartitionPeriod::messagesReceived)
                    .containsExactlyInAnyOrder(
                            org.assertj.core.groups.Tuple.tuple(3, 2L),
                            org.assertj.core.groups.Tuple.tuple(1, 1L));
            assertThat(period.rebalances()).hasSize(1);

            assertThat(callbacks.seal(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))).isTrue();
            stats.reset();

            first.messageReceived(msg2, now, 3);
            assertThat(stats.toCountersStats()).isEqualTo(new CountersStats());
            assertThat(stats.toPeriodStats().partitions()).isEmpty();
            assertThat(failure.get()).isNull();
        } finally {
            registry.close();
        }
    }
}
