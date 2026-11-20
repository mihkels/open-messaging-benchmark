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
import io.github.mihkels.omb.worker.DeliveryVerifier.Outcome;
import io.github.mihkels.omb.worker.DeliveryVerifier.SequenceWindow;
import io.github.mihkels.omb.worker.commands.CountersStats;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
}
