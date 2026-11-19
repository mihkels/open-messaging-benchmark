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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class MessageHeadersTest {

    @Test
    void poolHoldsDistinctAsciiHeaderSetsOfTheConfiguredShape() {
        List<Map<String, byte[]>> pool = new MessageHeaders(3, 16).pool(new SplittableRandom(1));

        assertThat(pool).hasSize(MessageHeaders.POOL_SIZE);
        for (Map<String, byte[]> headers : pool) {
            assertThat(headers).containsOnlyKeys("omb_h0", "omb_h1", "omb_h2");
            assertThat(headers.values())
                    .allSatisfy(value ->
                            assertThat(new String(value, java.nio.charset.StandardCharsets.US_ASCII))
                                    .hasSize(16)
                                    .matches("[0-9A-Za-z]*"));
        }
        assertThat(pool.get(0).get("omb_h0")).isNotEqualTo(pool.get(1).get("omb_h0"));
        assertThatThrownBy(() -> pool.get(0).put("x", new byte[0]))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsInvalidShapes() {
        assertThatThrownBy(() -> new MessageHeaders(0, 1)).hasMessage("headers count must be positive");
        assertThatThrownBy(() -> new MessageHeaders(1, -1))
                .hasMessage("headers valueSize must not be negative");
        assertThatThrownBy(() -> MessageHeaders.fromJson(1, null))
                .hasMessage("headers requires count and valueSize");
    }
}
