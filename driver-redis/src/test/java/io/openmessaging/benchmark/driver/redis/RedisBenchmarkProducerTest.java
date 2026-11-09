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
package io.openmessaging.benchmark.driver.redis;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.XAddParams;

class RedisBenchmarkProducerTest {
    @Test
    void closeLeavesTheSharedPoolOpenAndRejectsLaterSends() {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        RedisBenchmarkProducer producer = new RedisBenchmarkProducer(pool, "topic", 1000);
        producer.sendAsync(Optional.empty(), new byte[] {1}).join();

        producer.close();

        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {2}).join())
                .hasCauseInstanceOf(IllegalStateException.class);
        verify(pool, never()).close();
        verify(jedis).close();
    }

    @Test
    void sendFailureCompletesExceptionally() {
        JedisPool pool = mock();
        RuntimeException cause = new RuntimeException("connection failed");
        when(pool.getResource()).thenThrow(cause);
        RedisBenchmarkProducer producer = new RedisBenchmarkProducer(pool, "topic", 1000);

        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(cause);
    }

    @Test
    void keyedSendAddsTheKeyField() {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        RedisBenchmarkProducer producer = new RedisBenchmarkProducer(pool, "topic", 1000);

        producer.sendAsync(Optional.of("k"), new byte[] {1}).join();

        verify(jedis)
                .xadd(
                        any(byte[].class),
                        org.mockito.ArgumentMatchers.<Map<byte[], byte[]>>argThat(
                                map -> map.size() == 2 && "k".equals(field(map, "key"))),
                        any(XAddParams.class));
        verify(jedis).close();
    }

    // The entry map uses identity keys, so look the field up by name
    private static String field(Map<byte[], byte[]> entry, String name) {
        return entry.entrySet().stream()
                .filter(e -> name.equals(new String(e.getKey(), StandardCharsets.UTF_8)))
                .map(e -> new String(e.getValue(), StandardCharsets.UTF_8))
                .findFirst()
                .orElse(null);
    }

    @Test
    void sendTrimsTheStreamToTheConfiguredMaxLen() {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        RedisBenchmarkProducer producer = new RedisBenchmarkProducer(pool, "topic", 42);

        producer.sendAsync(Optional.empty(), new byte[] {1}).join();

        verify(jedis)
                .xadd(
                        any(byte[].class), any(), eq(XAddParams.xAddParams().maxLen(42).approximateTrimming()));
    }
}
