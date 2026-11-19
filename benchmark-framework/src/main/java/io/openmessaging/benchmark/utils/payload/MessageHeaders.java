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
package io.openmessaging.benchmark.utils.payload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.random.RandomGenerator;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;

/**
 * Generated message headers sent with every message.
 *
 * @param count headers per message
 * @param valueSize bytes per header value
 */
public record MessageHeaders(int count, int valueSize) {

    /** Distinct header sets per producer, so batch compression cannot collapse repeated values. */
    public static final int POOL_SIZE = 64;

    private static final byte[] ALPHANUMERIC =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                    .getBytes(StandardCharsets.US_ASCII);

    public MessageHeaders {
        if (count < 1) {
            throw new IllegalArgumentException("headers count must be positive");
        }
        if (valueSize < 0) {
            throw new IllegalArgumentException("headers valueSize must not be negative");
        }
    }

    @JsonCreator
    public static MessageHeaders fromJson(
            @JsonProperty("count") @Nullable Integer count,
            @JsonProperty("valueSize") @Nullable Integer valueSize) {
        if (count == null || valueSize == null) {
            throw new IllegalArgumentException("headers requires count and valueSize");
        }
        return new MessageHeaders(count, valueSize);
    }

    /**
     * Header name at an index, a valid identifier for every broker, JMS included.
     *
     * @param index the header index
     * @return the header name
     */
    public static String name(int index) {
        return "omb_h" + index;
    }

    /**
     * Pre-generates {@link #POOL_SIZE} immutable header sets with random ASCII alphanumeric values,
     * so string-only brokers send exactly {@code valueSize} bytes per value.
     *
     * @param random the source of the values
     * @return the header sets
     */
    public List<Map<String, byte[]>> pool(RandomGenerator random) {
        return Stream.generate(() -> headerSet(random)).limit(POOL_SIZE).toList();
    }

    private Map<String, byte[]> headerSet(RandomGenerator random) {
        Map<String, byte[]> headers = HashMap.newHashMap(count);
        for (int i = 0; i < count; i++) {
            byte[] value = new byte[valueSize];
            for (int j = 0; j < valueSize; j++) {
                value[j] = ALPHANUMERIC[random.nextInt(ALPHANUMERIC.length)];
            }
            headers.put(name(i), value);
        }
        return Map.copyOf(headers);
    }
}
