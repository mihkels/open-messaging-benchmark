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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PublicationLifecycle;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.XAddParams;

public class RedisBenchmarkProducer implements BenchmarkProducer {
    private static final byte[] PAYLOAD_FIELD = "payload".getBytes(UTF_8);
    private static final byte[] KEY_FIELD = "key".getBytes(UTF_8);

    private final JedisPool pool;
    private final byte[] topic;
    private final XAddParams xaddParams;
    private final PublicationLifecycle lifecycle = new PublicationLifecycle("Redis producer");

    public RedisBenchmarkProducer(final JedisPool pool, final String rmqTopic, final long maxLen) {
        this.pool = pool;
        this.topic = rmqTopic.getBytes(UTF_8);
        this.xaddParams = XAddParams.xAddParams().maxLen(maxLen).approximateTrimming();
    }

    @Override
    public CompletableFuture<Void> sendAsync(final Optional<String> key, final byte[] payload) {
        // xadd returns once Redis has acknowledged the entry
        return lifecycle.publishSync(() -> {
            // byte[] keys have identity equality; the two field names are distinct arrays
            Map<byte[], byte[]> map1 = new IdentityHashMap<>();
            map1.put(PAYLOAD_FIELD, payload);
            if (key.isPresent()) {
                map1.put(KEY_FIELD, key.get().getBytes(UTF_8));
            }
            try (Jedis jedis = this.pool.getResource()) {
                jedis.xadd(this.topic, map1, this.xaddParams);
            }
        });
    }

    /** Waits for sends in progress to return; the shared pool is owned by the driver. */
    @Override
    public void close() {
        lifecycle.close();
    }
}
