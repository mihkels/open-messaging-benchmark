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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.RebalanceEvent;
import io.openmessaging.benchmark.worker.commands.PartitionPeriod;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PartitionTelemetryCallbacksTest {
    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(key -> null);
    private final WorkerStats stats = new WorkerStats(registry);
    private final RunCallbacks callbacks =
            new RunCallbacks(
                    stats,
                    failure -> {
                        throw new AssertionError(failure);
                    });

    @AfterEach
    void close() {
        registry.close();
    }

    @Test
    void topicCallbackRecordsPartitionsAndRebalances() {
        ConsumerCallback callback = callbacks.forTopic("orders");
        long now = System.currentTimeMillis();
        callback.messageReceived(new byte[4], now, 2);
        callback.messageReceived(ByteBuffer.allocate(4), now, 2);
        callback.messageReceived(new byte[4], now);
        callback.partitionsChanged(new RebalanceEvent(RebalanceEvent.Kind.ASSIGNED, 9L, List.of(2)));

        PeriodStats period = stats.toPeriodStats();

        assertThat(period.messagesReceived()).as("unknown partitions still count overall").isEqualTo(3);
        assertThat(period.partitions())
                .singleElement()
                .satisfies(p -> {
                    assertThat(p.topic()).isEqualTo("orders");
                    assertThat(p.partition()).isEqualTo(2);
                    assertThat(p.messagesReceived()).isEqualTo(2);
                });
        assertThat(period.rebalances())
                .singleElement()
                .satisfies(r -> {
                    assertThat(r.timestampMillis()).isEqualTo(9L);
                    assertThat(r.topic()).isEqualTo("orders");
                });
    }

    @Test
    void completedSendWithAPartitionRecordsItsPublishLatency() {
        long start = 0;
        assertThat(callbacks.beginSend()).isTrue();
        callbacks.completeSend(
                8, start, start, () -> TimeUnit.MILLISECONDS.toNanos(3), null, false, "orders", 1);
        assertThat(callbacks.beginSend()).isTrue();
        callbacks.completeSend(
                8, start, start, () -> 1L, null, false, "orders", ConsumerCallback.UNKNOWN_PARTITION);

        PeriodStats period = stats.toPeriodStats();

        assertThat(period.messagesSent()).isEqualTo(2);
        PartitionPeriod p = period.partitions().getFirst();
        assertThat(period.partitions()).hasSize(1);
        assertThat(p.messagesSent()).isEqualTo(1);
        assertThat(p.publishLatency().getValueAtPercentile(99)).isBetween(2_000L, 4_000L);
    }

    @Test
    void deliveryVerifierForwardsPartitionsAndRebalances() {
        ConsumerCallback delegate = mock(ConsumerCallback.class);
        ConsumerCallback verifying = new DeliveryVerifier(stats).callback("t", "s", delegate);
        byte[] payload = new byte[4];
        ByteBuffer buffer = ByteBuffer.allocate(4);
        RebalanceEvent event = new RebalanceEvent(RebalanceEvent.Kind.REVOKED, 1L, List.of(0));

        verifying.messageReceived(payload, 1L, 4);
        verifying.messageReceived(buffer, 2L, 5);
        verifying.partitionsChanged(event);

        verify(delegate).messageReceived(payload, 1L, 4);
        verify(delegate).messageReceived(buffer, 2L, 5);
        verify(delegate).partitionsChanged(event);
    }

    @Test
    void sealedCallbacksDropRebalances() throws Exception {
        callbacks.seal(System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
        callbacks
                .forTopic("t")
                .partitionsChanged(new RebalanceEvent(RebalanceEvent.Kind.LOST, 1L, List.of()));

        assertThat(stats.toPeriodStats().rebalances()).isEmpty();
    }
}
