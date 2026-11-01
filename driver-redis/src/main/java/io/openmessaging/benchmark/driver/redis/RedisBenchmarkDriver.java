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

import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.redis.client.RedisClientConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.yaml.YAMLFactory;

@SuppressWarnings("unused")
public class RedisBenchmarkDriver implements BenchmarkDriver {
    JedisPool jedisPool;
    private RedisClientConfig clientConfig;
    private boolean closed;

    @Override
    public void initialize(final Path configurationFile, final MeterRegistry statsLogger)
            throws IOException {
        this.clientConfig = readConfig(configurationFile);
    }

    @Override
    public String getTopicNamePrefix() {
        return "redis-openmessaging-benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(final String topic, final int partitions) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(final String topic) {
        try {
            return CompletableFuture.completedFuture(new RedisBenchmarkProducer(pool(), topic));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            final String topic, final String subscriptionName, final ConsumerCallback consumerCallback) {
        String consumerId = "consumer-" + getRandomString();
        try {
            JedisPool sharedPool = pool();
            try (Jedis jedis = sharedPool.getResource()) {
                try {
                    jedis.xgroupCreate(topic, subscriptionName, null, true);
                    log.info("Created consumer group '{}' for topic '{}'", subscriptionName, topic);
                } catch (redis.clients.jedis.exceptions.JedisDataException e) {
                    if (e.getMessage() != null && e.getMessage().contains("BUSYGROUP")) {
                        log.info(
                                "Consumer group '{}' already exists for topic '{}', reusing it",
                                subscriptionName,
                                topic);
                    } else {
                        log.error(
                                "Failed to create consumer group '{}' for topic '{}'", subscriptionName, topic, e);
                        throw e;
                    }
                }
            }
            return CompletableFuture.completedFuture(
                    new RedisBenchmarkConsumer(
                            consumerId, topic, subscriptionName, sharedPool, consumerCallback));
        } catch (Exception e) {
            log.error(
                    "Failed to create consumer instance for topic '{}' and subscription '{}'",
                    topic,
                    subscriptionName,
                    e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private synchronized JedisPool pool() {
        if (closed) {
            throw new IllegalStateException("Redis driver is closed");
        }
        if (jedisPool == null) {
            setupJedisConn();
        }
        return jedisPool;
    }

    private synchronized void setupJedisConn() {
        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(this.clientConfig.jedisPoolMaxTotal);
        poolConfig.setMaxIdle(this.clientConfig.jedisPoolMaxIdle);
        if (this.clientConfig.redisPass != null) {
            if (this.clientConfig.redisUser != null) {
                jedisPool =
                        new JedisPool(
                                poolConfig,
                                this.clientConfig.redisHost,
                                this.clientConfig.redisPort,
                                2000,
                                this.clientConfig.redisPass,
                                this.clientConfig.redisUser);
            } else {
                jedisPool =
                        new JedisPool(
                                poolConfig,
                                this.clientConfig.redisHost,
                                this.clientConfig.redisPort,
                                2000,
                                this.clientConfig.redisPass);
            }
        } else {
            jedisPool =
                    new JedisPool(poolConfig, this.clientConfig.redisHost, this.clientConfig.redisPort, 2000);
        }
    }

    @Override
    public void close() {
        JedisPool toClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toClose = jedisPool;
            jedisPool = null;
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectReader reader =
            mapper
                    .readerFor(RedisClientConfig.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static RedisClientConfig readConfig(Path configurationFile) throws IOException {
        try (var input = Files.newInputStream(configurationFile)) {
            return reader.readValue(input);
        }
    }

    private static final Random random = new Random();

    private static String getRandomString() {
        byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buffer);
    }

    private static final Logger log = LoggerFactory.getLogger(RedisBenchmarkDriver.class);
}
