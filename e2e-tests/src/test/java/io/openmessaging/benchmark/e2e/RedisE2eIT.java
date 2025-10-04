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

import io.openmessaging.benchmark.TestResult;
import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.WorkloadGenerator;
import io.openmessaging.benchmark.worker.LocalWorker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for Redis. Testing is done by creating a simple workload and running the benchmark.
 */
@Testcontainers
class RedisE2eIT extends BaseE2eIT {
    private static final Logger log = LoggerFactory.getLogger(RedisE2eIT.class);

    private static final String REDIS_IMAGE = "redis:7-alpine";
    private static final int REDIS_PORT = 6379;

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE))
            .withExposedPorts(REDIS_PORT);

    private static File driverConfigFile;

    @BeforeAll
    static void setupDriver() throws Exception {
        // Wait for Redis to be fully ready
        redis.start();

        // Give Redis some additional time to be ready
        Thread.sleep(1000);

        log.info("Redis running at {}:{}", redis.getHost(), redis.getMappedPort(REDIS_PORT));

        // Create driver configuration
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

        Path configPath = Files.createTempFile("redis-driver-", ".yaml");
        Files.writeString(configPath, driverConfig);
        driverConfigFile = configPath.toFile();
        driverConfigFile.deleteOnExit();

        log.info("Created driver config at: {}", driverConfigFile.getAbsolutePath());

        // Verify Redis is responsive
        verifyRedisConnection();
    }

    private static void verifyRedisConnection() throws Exception {
        try (Jedis jedis = new Jedis(redis.getHost(), redis.getMappedPort(REDIS_PORT))) {
            // Try to ping Redis to verify connection
            String response = jedis.ping();
            assertThat(response).isEqualTo("PONG");
            log.info("Redis connection verified successfully");
        }
    }

    @AfterAll
    static void tearDownDriver() {
        if (driverConfigFile != null && driverConfigFile.exists()) {
            driverConfigFile.delete();
        }
    }

    @AfterEach
    void cleanupRedis() {
        // Flush Redis database between tests to avoid consumer group conflicts
        // Redis Streams consumer groups persist, causing BUSYGROUP errors on subsequent tests
        try (Jedis jedis = new Jedis(redis.getHost(), redis.getMappedPort(REDIS_PORT))) {
            jedis.flushAll();  // Changed from flushDB to flushAll for thoroughness
            log.info("Flushed Redis database after test");
        } catch (Exception e) {
            log.warn("Failed to flush Redis database", e);
        }

        // Give Redis a moment to complete the flush
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testSimpleProduceConsume() throws Exception {
        // Create workload
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 1; // Redis uses streams but we keep it simple
        workload.producersPerTopic = 2;
        workload.consumerPerSubscription = 2;
        workload.producerRate = 500;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;
        workload.testDurationMinutes = 1;

        // Run benchmark
        TestResult result = runBenchmark(workload);

        // Validate results
        validateResults(result, workload);
        var total = result.publishRate.stream().reduce(0.0, Double::sum);
        assertThat(total)
                .as("Should publish approximately expected messages")
                .isGreaterThan((long) (workload.producerRate * workload.testDurationMinutes * 0.6));
    }

    @Test
    void testMultipleTopics() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 3;
        workload.partitionsPerTopic = 1;
        workload.testDurationMinutes = 1;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;
        workload.producerRate = 100;

        TestResult result = runBenchmark(workload);

        validateResults(result, workload);
        assertThat(result.topics).isEqualTo(workload.topics);
    }

    @Test
    void testHighThroughput() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.messageSize = 100;
        workload.producersPerTopic = 4;
        workload.consumerPerSubscription = 4;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;
        workload.producerRate = 100;
        workload.testDurationMinutes = 1;

        TestResult result = runBenchmark(workload);

        validateResults(result, workload);

        var total = result.publishRate.stream().reduce(0.0, Double::sum);
        double actualRate = total / workload.testDurationMinutes;
        log.info("Actual publish rate: {} msg/s", actualRate);
        assertThat(actualRate).isGreaterThan(workload.producerRate * 0.4);
    }

    @Test
    void testSmallMessages() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.messageSize = 64; // Small messages
        workload.producersPerTopic = 2;
        workload.consumerPerSubscription = 2;
        workload.producerRate = 500;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;
        workload.testDurationMinutes = 1;

        TestResult result = runBenchmark(workload);

        validateResults(result, workload);
        var total = result.publishRate.stream().reduce(0.0, Double::sum);
        assertThat(total)
                .as("Should publish small messages successfully")
                .isGreaterThan((long) (workload.producerRate * workload.testDurationMinutes * 0.5));
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

        Path configPath = Files.createTempFile("redis-driver-large-pool-", ".yaml");
        Files.writeString(configPath, driverConfig);
        File largePoolConfigFile = configPath.toFile();
        largePoolConfigFile.deleteOnExit();

        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.producersPerTopic = 4;
        workload.consumerPerSubscription = 4;
        workload.producerRate = 200;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;
        workload.testDurationMinutes = 1;

        // Run benchmark with larger pool configuration
        TestResult result = runBenchmarkWithConfig(workload, largePoolConfigFile);

        validateResults(result, workload);
        var total = result.publishRate.stream().reduce(0.0, Double::sum);
        assertThat(total)
                .as("Should handle higher concurrency with larger pool")
                .isGreaterThan((long) (workload.producerRate * workload.testDurationMinutes * 0.6));
    }

    private TestResult runBenchmark(Workload workload) throws Exception {
        return runBenchmarkWithConfig(workload, driverConfigFile);
    }

    private TestResult runBenchmarkWithConfig(Workload workload, File configFile) throws Exception {
        // Create a new LocalWorker for each test
        LocalWorker worker = new LocalWorker(statsLogger);

        try {
            // Initialize the worker with driver configuration
            worker.initializeDriver(configFile);

            // Create WorkloadGenerator with the driver name, workload, and worker
            WorkloadGenerator generator = new WorkloadGenerator("Redis", workload, worker);

            // Run the benchmark
            TestResult result = generator.run();

            // Clean up generator
            generator.close();

            return result;
        } finally {
            // Always close the worker
            worker.stopAll();
            worker.close();
        }
    }
}
