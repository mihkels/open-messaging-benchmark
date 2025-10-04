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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for RabbitMQ. Testing is done by creating a simple workload and running the benchmark.
 */
@Testcontainers
class RabbitMqE2eIT extends BaseE2eIT {
    private static final Logger log = LoggerFactory.getLogger(RabbitMqE2eIT.class);

    private static final String RABBITMQ_IMAGE = "rabbitmq:3.13-management";

    @Container
    static RabbitMQContainer rabbitmq = new RabbitMQContainer(RABBITMQ_IMAGE);

    private static File driverConfigFile;

    @BeforeAll
    static void setupDriver() throws Exception {
        // Wait for RabbitMQ to be fully ready
        rabbitmq.start();

        // Give RabbitMQ some additional time to be ready
        Thread.sleep(2000);

        log.info("RabbitMQ AMQP URL: amqp://{}:{}", rabbitmq.getHost(), rabbitmq.getAmqpPort());

        // Create driver configuration
        var amqpUri = String.format("amqp://guest:guest@%s:%d", rabbitmq.getHost(), rabbitmq.getAmqpPort());
        String driverConfig = String.format("""
                name: RabbitMQ
                driverClass: io.openmessaging.benchmark.driver.rabbitmq.RabbitMqBenchmarkDriver
                
                # RabbitMQ connection URIs
                amqpUris:
                  - %s
                
                # Message persistence (false for faster testing)
                messagePersistence: false
                
                # Queue type: CLASSIC, QUORUM, or STREAM
                queueType: CLASSIC
                
                # Producer creation settings
                producerCreationDelay: 100
                producerCreationBatchSize: 5
                
                # Consumer creation settings
                consumerCreationDelay: 100
                consumerCreationBatchSize: 5
                """, amqpUri);

        Path configPath = Files.createTempFile("rabbitmq-driver-", ".yaml");
        Files.writeString(configPath, driverConfig);
        driverConfigFile = configPath.toFile();
        driverConfigFile.deleteOnExit();

        log.info("Created driver config at: {}", driverConfigFile.getAbsolutePath());

        // Verify RabbitMQ is responsive
        verifyRabbitMqConnection();
    }

    private static void verifyRabbitMqConnection() throws Exception {
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setHost(rabbitmq.getHost());
        factory.setPort(rabbitmq.getAmqpPort());
        factory.setUsername(rabbitmq.getAdminUsername());
        factory.setPassword(rabbitmq.getAdminPassword());

        try (com.rabbitmq.client.Connection connection = factory.newConnection();
             com.rabbitmq.client.Channel channel = connection.createChannel()) {
            // Try to declare a test queue to verify connection
            channel.queueDeclare("test-connection-queue", false, true, true, null);
            log.info("RabbitMQ connection verified successfully");
        }
    }

    @AfterAll
    static void tearDownDriver() {
        if (driverConfigFile != null && driverConfigFile.exists()) {
            driverConfigFile.delete();
        }
    }

    @Test
    void testSimpleProduceConsume() throws Exception {
        // Create workload
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 1; // RabbitMQ doesn't use partitions like Kafka
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
                .isGreaterThan((long) (workload.producerRate * workload.testDurationMinutes * 0.7));
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
        assertThat(actualRate).isGreaterThan(workload.producerRate * 0.5);
    }

    @Test
    void testMessagePersistence() throws Exception {
        // Create a custom driver config with persistence enabled
        String amqpUri = String.format("amqp://guest:guest@%s:%d", rabbitmq.getHost(), rabbitmq.getAmqpPort());
        String driverConfig = String.format("""
                name: RabbitMQ
                driverClass: io.openmessaging.benchmark.driver.rabbitmq.RabbitMqBenchmarkDriver
                
                amqpUris:
                  - %s
                
                messagePersistence: true
                queueType: CLASSIC
                
                producerCreationDelay: 100
                producerCreationBatchSize: 5
                consumerCreationDelay: 100
                consumerCreationBatchSize: 5
                """, amqpUri);

        Path configPath = Files.createTempFile("rabbitmq-driver-persistence-", ".yaml");
        Files.writeString(configPath, driverConfig);
        File persistenceConfigFile = configPath.toFile();
        persistenceConfigFile.deleteOnExit();

        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.producersPerTopic = 1;
        workload.consumerPerSubscription = 1;
        workload.producerRate = 100;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;
        workload.testDurationMinutes = 1;

        // Run benchmark with persistence
        TestResult result = runBenchmarkWithConfig(workload, persistenceConfigFile);

        validateResults(result, workload);
        var total = result.publishRate.stream().reduce(0.0, Double::sum);
        assertThat(total)
                .as("Should publish messages even with persistence")
                .isGreaterThan((long) (workload.producerRate * workload.testDurationMinutes * 0.5));
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
            WorkloadGenerator generator = new WorkloadGenerator("RabbitMQ", workload, worker);

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