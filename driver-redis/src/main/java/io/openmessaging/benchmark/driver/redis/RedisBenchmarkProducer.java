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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.XAddParams;

public class RedisBenchmarkProducer implements BenchmarkProducer {
    private final JedisPool pool;
    private final String rmqTopic;
    private final XAddParams xaddParams;
    private final Object lifecycleLock = new Object();
    private final ThreadLocal<Boolean> inSend = new ThreadLocal<>();
    private boolean closed;
    private int activeSends;

    public RedisBenchmarkProducer(final JedisPool pool, final String rmqTopic) {
        this.pool = pool;
        this.rmqTopic = rmqTopic;
        this.xaddParams = redis.clients.jedis.params.XAddParams.xAddParams();
    }

    @Override
    public CompletableFuture<Void> sendAsync(final Optional<String> key, final byte[] payload) {
        synchronized (lifecycleLock) {
            if (closed) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Redis producer is closed"));
            }
            activeSends++;
        }
        Boolean previous = inSend.get();
        inSend.set(true);
        try {
            Map<byte[], byte[]> map1 = new HashMap<>();
            map1.put("payload".getBytes(UTF_8), payload);
            if (key.isPresent()) {
                map1.put("key".getBytes(UTF_8), key.toString().getBytes(UTF_8));
            }
            try (Jedis jedis = this.pool.getResource()) {
                jedis.xadd(this.rmqTopic.getBytes(UTF_8), map1, this.xaddParams);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        } finally {
            if (previous == null) {
                inSend.remove();
            } else {
                inSend.set(previous);
            }
            synchronized (lifecycleLock) {
                activeSends--;
                lifecycleLock.notifyAll();
            }
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (Boolean.TRUE.equals(inSend.get())) {
            throw new IllegalStateException("Cannot close Redis producer during its send");
        }
        synchronized (lifecycleLock) {
            closed = true;
            while (activeSends != 0) {
                try {
                    lifecycleLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }
}
