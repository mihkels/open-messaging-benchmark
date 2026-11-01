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
package io.openmessaging.benchmark.e2e;

import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.driver.redis.RedisBenchmarkDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.StreamGroupInfo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for Redis. Testing is done by creating a simple workload and running the benchmark.
 */
@Testcontainers
class RedisE2eIT extends BaseE2eIT {
    private static final Logger log = LoggerFactory.getLogger(RedisE2eIT.class);

    private static final String REDIS_IMAGE = "redis:7.4-alpine";
    private static final int REDIS_PORT = 6379;

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE))
            .withExposedPorts(REDIS_PORT);

    private Path driverConfigFile;

    @BeforeAll
    static void verifyRedisConnection() {
        log.info("Redis running at {}:{}", redis.getHost(), redis.getMappedPort(REDIS_PORT));
        try (Jedis jedis = new Jedis(redis.getHost(), redis.getMappedPort(REDIS_PORT))) {
            assertThat(jedis.ping()).isEqualTo("PONG");
        }
    }

    @BeforeEach
    void setupDriver() throws Exception {
        String driverConfig = String.format("""
                name: Redis
                driverClass: io.openmessaging.benchmark.driver.redis.RedisBenchmarkDriver

                # Redis connection settings
                redisHost: %s
                redisPort: %d

                # Jedis pool configuration
                jedisPoolMaxTotal: 16
                jedisPoolMaxIdle: 16
                """, redis.getHost(), redis.getMappedPort(REDIS_PORT));

        driverConfigFile = tempDir.resolve("redis-driver.yaml");
        Files.writeString(driverConfigFile, driverConfig);
    }

    @AfterEach
    void cleanupRedis() {
        try (Jedis jedis = new Jedis(redis.getHost(), redis.getMappedPort(REDIS_PORT))) {
            jedis.flushDB();
        }
    }

    @Test
    @DriverContract("redis")
    void testSimpleProduceConsume() throws Exception {
        // Create workload
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 1; // Redis uses streams but we keep it simple
        workload.producersPerTopic = 2;
        workload.consumerPerSubscription = 2;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;

        // Run benchmark
        SmokeResult result = runBenchmark(workload);

        // Validate results
        validateResults(result, workload);
    }

    @Test
    void testMultipleTopics() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 3;
        workload.partitionsPerTopic = 1;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;

        SmokeResult result = runBenchmark(workload);

        validateResults(result, workload);
        try (Jedis jedis = new Jedis(redis.getHost(), redis.getMappedPort(REDIS_PORT))) {
            assertThat(jedis.keys("redis-openmessaging-benchmark*")).hasSize(workload.topics);
        }
    }

    @Test
    void testBinaryPayloadAndIndependentConsumerGroups() throws Exception {
        byte[] payload = {0, 1, (byte) 0x80, (byte) 0xff, 42, 0};
        MessageCapture first = new MessageCapture();
        MessageCapture second = new MessageCapture();
        try (RedisBenchmarkDriver driver = new RedisBenchmarkDriver()) {
            driver.initialize(driverConfigFile, null);
            String topic = driver.getTopicNamePrefix() + "-" + UUID.randomUUID();
            driver.createTopic(topic, 1).get(10, TimeUnit.SECONDS);
            try (var firstConsumer = driver.createConsumer(topic, "group-a", first).get(10, TimeUnit.SECONDS);
                    var secondConsumer =
                            driver.createConsumer(topic, "group-b", second).get(10, TimeUnit.SECONDS);
                    var producer = driver.createProducer(topic).get(10, TimeUnit.SECONDS)) {
                producer.sendAsync(Optional.empty(), payload).get(10, TimeUnit.SECONDS);
                for (MessageCapture capture : List.of(first, second)) {
                    MessageCapture.Delivery received = capture.await();
                    assertThat(received.payload()).isEqualTo(payload);
                    assertThat(received.timestamp()).isPositive();
                }
                try (Jedis jedis = new Jedis(redis.getHost(), redis.getMappedPort(REDIS_PORT))) {
                    assertThat(jedis.xinfoGroups(topic))
                            .extracting(StreamGroupInfo::getName)
                            .containsExactlyInAnyOrder("group-a", "group-b");
                }
            }
        }
    }

    @Test
    void testMultipleProducersAndConsumers() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.messageSize = 100;
        workload.producersPerTopic = 4;
        workload.consumerPerSubscription = 4;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;

        SmokeResult result = runBenchmark(workload);

        validateResults(result, workload);

    }

    @Test
    void testSmallMessages() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.messageSize = 64; // Small messages
        workload.producersPerTopic = 2;
        workload.consumerPerSubscription = 2;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;

        SmokeResult result = runBenchmark(workload);

        validateResults(result, workload);
    }

    @Test
    void testLargePoolConfiguration() throws Exception {
        // Create a custom driver config with larger pool
        String driverConfig = String.format("""
                name: Redis
                driverClass: io.openmessaging.benchmark.driver.redis.RedisBenchmarkDriver

                redisHost: %s
                redisPort: %d

                # Larger pool for higher concurrency
                jedisPoolMaxTotal: 32
                jedisPoolMaxIdle: 16
                """, redis.getHost(), redis.getMappedPort(REDIS_PORT));

        Path largePoolConfigFile = tempDir.resolve("redis-driver-large-pool.yaml");
        Files.writeString(largePoolConfigFile, driverConfig);

        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.producersPerTopic = 4;
        workload.consumerPerSubscription = 4;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;

        // Run benchmark with larger pool configuration
        SmokeResult result = runBenchmarkWithConfig(workload, largePoolConfigFile);

        validateResults(result, workload);
    }

    private SmokeResult runBenchmark(Workload workload) throws Exception {
        return runBenchmarkWithConfig(workload, driverConfigFile);
    }

    private SmokeResult runBenchmarkWithConfig(Workload workload, Path configFile) throws Exception {
        return runBenchmark("Redis", configFile, workload);
    }

}
