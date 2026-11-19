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

import java.nio.ByteBuffer;

/** Callback that the driver implementation calls when a message is received. */
public interface ConsumerCallback {
    /**
     * Publish timestamp for a message that carries none: the message is counted but gives no
     * end-to-end latency sample, because the latency comes out negative and the framework drops it.
     */
    long NO_PUBLISH_TIMESTAMP = Long.MAX_VALUE / 1000;

    /** Partition value for a message whose partition the driver does not know. */
    int UNKNOWN_PARTITION = -1;

    /**
     * Driver should invoke this method (or the ByteBuffer variant) once for each message received.
     *
     * @param payload the received message payload
     * @param publishTimestamp the publish timestamp in milliseconds
     */
    void messageReceived(byte[] payload, long publishTimestamp);

    /**
     * Driver should invoke this method (or the byte[] variant) once for each message received.
     *
     * @param payload the received message payload
     * @param publishTimestamp the publish timestamp in milliseconds
     */
    void messageReceived(ByteBuffer payload, long publishTimestamp);

    /**
     * Partition-aware variant of {@link #messageReceived(byte[], long)} for drivers that override
     * {@link BenchmarkDriver#supportsPartitionTelemetry()}. The default drops the partition.
     *
     * @param payload the received message payload
     * @param publishTimestamp the publish timestamp in milliseconds
     * @param partition the partition the message was consumed from, or {@link #UNKNOWN_PARTITION}
     */
    default void messageReceived(byte[] payload, long publishTimestamp, int partition) {
        messageReceived(payload, publishTimestamp);
    }

    /**
     * Partition-aware variant of {@link #messageReceived(ByteBuffer, long)} for drivers that override
     * {@link BenchmarkDriver#supportsPartitionTelemetry()}. The default drops the partition.
     *
     * @param payload the received message payload
     * @param publishTimestamp the publish timestamp in milliseconds
     * @param partition the partition the message was consumed from, or {@link #UNKNOWN_PARTITION}
     */
    default void messageReceived(ByteBuffer payload, long publishTimestamp, int partition) {
        messageReceived(payload, publishTimestamp);
    }

    /**
     * Drivers with group membership call this when the consumer's partition assignment changes. It
     * may be called from the client's own thread and must not block; the default ignores the event.
     *
     * @param event the assignment change
     */
    default void partitionsChanged(RebalanceEvent event) {}
}
