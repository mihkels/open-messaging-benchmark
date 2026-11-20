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
package io.github.mihkels.omb.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class PartitionTelemetryDefaultsTest {

    @Test
    void defaultSendWithResultDelegatesAndReportsUnknownPartition() {
        List<String> calls = new ArrayList<>();
        BenchmarkProducer producer = producer(calls);

        assertSame(
                SendResult.UNKNOWN,
                producer.sendAsyncWithResult(Optional.empty(), new byte[] {1}, null).join());
        assertSame(
                SendResult.UNKNOWN,
                producer
                        .sendAsyncWithResult(Optional.empty(), new byte[] {1}, Map.of("h", new byte[] {2}))
                        .join());
        assertEquals(List.of("plain", "headers"), calls);
        assertEquals(ConsumerCallback.UNKNOWN_PARTITION, SendResult.UNKNOWN.partition());
    }

    @Test
    void defaultPartitionAwareReceiveDropsThePartitionAndDelegates() {
        List<String> calls = new ArrayList<>();
        ConsumerCallback callback =
                new ConsumerCallback() {
                    @Override
                    public void messageReceived(byte[] payload, long publishTimestamp) {
                        calls.add("bytes@" + publishTimestamp);
                    }

                    @Override
                    public void messageReceived(ByteBuffer payload, long publishTimestamp) {
                        calls.add("buffer@" + publishTimestamp);
                    }
                };

        callback.messageReceived(new byte[] {1}, 5L, 3);
        callback.messageReceived(ByteBuffer.allocate(1), 6L, 3);
        callback.partitionsChanged(new RebalanceEvent(RebalanceEvent.Kind.ASSIGNED, 7L, List.of(3)));

        assertEquals(List.of("bytes@5", "buffer@6"), calls);
    }

    @Test
    void rebalanceEventCopiesItsPartitions() {
        List<Integer> partitions = new ArrayList<>(List.of(1, 2));
        RebalanceEvent event = new RebalanceEvent(RebalanceEvent.Kind.REVOKED, 1L, partitions);
        partitions.add(3);

        assertEquals(List.of(1, 2), event.partitions());
        assertThrows(UnsupportedOperationException.class, () -> event.partitions().add(4));
    }

    @Test
    void driversDoNotSupportPartitionTelemetryByDefault() throws Exception {
        try (BenchmarkDriver driver =
                (BenchmarkDriver)
                        Proxy.newProxyInstance(
                                BenchmarkDriver.class.getClassLoader(),
                                new Class<?>[] {BenchmarkDriver.class},
                                (proxy, method, args) ->
                                        method.isDefault()
                                                ? InvocationHandler.invokeDefault(proxy, method, args)
                                                : null)) {
            assertFalse(driver.supportsPartitionTelemetry());
        }
    }

    private static BenchmarkProducer producer(List<String> calls) {
        return new BenchmarkProducer() {
            @Override
            public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
                calls.add("plain");
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> sendAsync(
                    Optional<String> key, byte[] payload, Map<String, byte[]> headers) {
                calls.add("headers");
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void close() {
                // Nothing to release
            }
        };
    }
}
