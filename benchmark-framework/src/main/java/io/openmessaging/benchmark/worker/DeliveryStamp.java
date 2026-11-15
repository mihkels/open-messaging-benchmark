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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * The {@code (producerId, sequence)} stamp that {@code verifyDelivery} writes over the first {@link
 * #LENGTH} bytes of each payload: a 4-byte magic number, the 8-byte producer id and the 8-byte
 * sequence number, big-endian. Overwriting keeps the message size unchanged. One instance belongs
 * to one producer task and is not thread-safe.
 */
public final class DeliveryStamp {
    /** The bytes the stamp takes at the start of each payload. */
    public static final int LENGTH = 20;

    // "OMBV"; tells stamped payloads from readiness probes and other unstamped messages
    static final int MAGIC = 0x4F4D4256;
    private static final VarHandle INT =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private final long producerId;
    private long nextSequence;

    DeliveryStamp() {
        // Random, so producers on different workers do not collide
        this(UUID.randomUUID().getMostSignificantBits());
    }

    DeliveryStamp(long producerId) {
        this.producerId = producerId;
    }

    /**
     * Copies a payload template and stamps it with the next sequence number.
     *
     * @param template the shared payload template, which is not modified
     * @return the stamped copy
     */
    byte[] stamp(byte[] template) {
        // LocalWorker.startLoad rejects templates shorter than LENGTH
        byte[] payload = template.clone();
        INT.set(payload, 0, MAGIC);
        LONG.set(payload, 4, producerId);
        LONG.set(payload, 12, nextSequence++);
        return payload;
    }

    static boolean isStamped(byte[] payload) {
        return payload.length >= LENGTH && (int) INT.get(payload, 0) == MAGIC;
    }

    static long producerId(byte[] payload) {
        return (long) LONG.get(payload, 4);
    }

    static long sequence(byte[] payload) {
        return (long) LONG.get(payload, 12);
    }

    // The ByteBuffer variants read at absolute offsets from the buffer's position and leave its
    // position and byte order as they are
    static boolean isStamped(ByteBuffer payload) {
        if (payload.remaining() < LENGTH) {
            return false;
        }
        int magic = payload.getInt(payload.position());
        return (payload.order() == ByteOrder.BIG_ENDIAN ? magic : Integer.reverseBytes(magic)) == MAGIC;
    }

    static long producerId(ByteBuffer payload) {
        return bigEndianLong(payload, payload.position() + 4);
    }

    static long sequence(ByteBuffer payload) {
        return bigEndianLong(payload, payload.position() + 12);
    }

    private static long bigEndianLong(ByteBuffer payload, int at) {
        long value = payload.getLong(at);
        return payload.order() == ByteOrder.BIG_ENDIAN ? value : Long.reverseBytes(value);
    }
}
