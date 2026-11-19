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
import io.openmessaging.benchmark.driver.DriverConfigs;
import io.openmessaging.benchmark.driver.ResourceNames;
import io.openmessaging.benchmark.driver.redis.client.RedisClientConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@SuppressWarnings("unused")
public class RedisBenchmarkDriver implements BenchmarkDriver {
    @Nullable JedisPool jedisPool;
    // Consumers hold a connection inside XREADGROUP BLOCK, so they get their own unbounded pool
    @Nullable JedisPool consumerPool;
    private RedisClientConfig clientConfig;
    private String redisHost;
    private int redisPort;
    private boolean closed;

    @Override
    public void initialize(final Path configurationFile, final MeterRegistry statsLogger)
            throws IOException {
        this.clientConfig = readConfig(configurationFile);
        if (clientConfig.redisHost == null) {
            throw new IllegalArgumentException("Redis config needs redisHost");
        }
        if (clientConfig.redisPort == null) {
            throw new IllegalArgumentException("Redis config needs redisPort");
        }
        this.redisHost = clientConfig.redisHost;
        this.redisPort = clientConfig.redisPort;
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
            return CompletableFuture.completedFuture(
                    new RedisBenchmarkProducer(pool(), topic, clientConfig.redisStreamMaxLen));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            final String topic, final String subscriptionName, final ConsumerCallback consumerCallback) {
        String consumerId = "consumer-" + ResourceNames.randomSuffix();
        try {
            try (Jedis jedis = pool().getResource()) {
                createGroup(jedis, topic, subscriptionName);
            }
            return CompletableFuture.completedFuture(
                    new RedisBenchmarkConsumer(
                            consumerId, topic, subscriptionName, consumerPool(), consumerCallback));
        } catch (Exception e) {
            log.error(
                    "Failed to create consumer instance for topic '{}' and subscription '{}'",
                    topic,
                    subscriptionName,
                    e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private static void createGroup(Jedis jedis, String topic, String subscriptionName) {
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

    private synchronized JedisPool pool() {
        if (closed) {
            throw new IllegalStateException("Redis driver is closed");
        }
        JedisPool current = jedisPool;
        if (current == null) {
            current = setupJedisConn();
        }
        return current;
    }

    private synchronized JedisPool consumerPool() {
        pool();
        JedisPool current = consumerPool;
        if (current == null) {
            throw new IllegalStateException("Redis consumer pool is not set up");
        }
        return current;
    }

    private synchronized JedisPool setupJedisConn() {
        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(clientConfig.jedisPoolMaxTotal);
        poolConfig.setMaxIdle(clientConfig.jedisPoolMaxIdle);
        GenericObjectPoolConfig<Jedis> consumerPoolConfig = new GenericObjectPoolConfig<>();
        consumerPoolConfig.setMaxTotal(-1);
        consumerPoolConfig.setMaxIdle(-1);
        JedisPool producers = newPool(poolConfig);
        jedisPool = producers;
        consumerPool = newPool(consumerPoolConfig);
        return producers;
    }

    private JedisPool newPool(GenericObjectPoolConfig<Jedis> poolConfig) {
        String redisUser = clientConfig.redisUser;
        String redisPass = clientConfig.redisPass;
        if (redisPass == null) {
            return new JedisPool(poolConfig, redisHost, redisPort, 2000);
        }
        if (redisUser == null) {
            return new JedisPool(poolConfig, redisHost, redisPort, 2000, redisPass);
        }
        return new JedisPool(poolConfig, redisHost, redisPort, 2000, redisUser, redisPass);
    }

    @Override
    public void close() {
        JedisPool toClose;
        JedisPool consumersToClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toClose = jedisPool;
            consumersToClose = consumerPool;
            jedisPool = null;
            consumerPool = null;
        }
        if (toClose != null) {
            toClose.close();
        }
        if (consumersToClose != null) {
            consumersToClose.close();
        }
    }

    private static RedisClientConfig readConfig(Path configurationFile) throws IOException {
        return DriverConfigs.read(configurationFile, RedisClientConfig.class);
    }

    private static final Logger log = LoggerFactory.getLogger(RedisBenchmarkDriver.class);
}
