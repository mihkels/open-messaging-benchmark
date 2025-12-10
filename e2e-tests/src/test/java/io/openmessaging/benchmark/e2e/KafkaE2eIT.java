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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for Kafka. Testing is done by creating a simple workload and running the benchmark.
 */
@Testcontainers
class KafkaE2eIT extends BaseE2eIT {
    private static final Logger log = LoggerFactory.getLogger(KafkaE2eIT.class);

    private static final String KAFKA_IMAGE = "apache/kafka:3.8.0";

    @Container
    static KafkaContainer kafka = new KafkaContainer(KAFKA_IMAGE);

    private static Path driverConfigFile;

    @BeforeAll
    static void setupDriver() throws Exception {
        // Wait for Kafka to be fully ready
        kafka.start();

        // Give Kafka some additional time to be ready for topic operations
        Thread.sleep(2000);

        log.info("Kafka bootstrap servers: {}", kafka.getBootstrapServers());

        // Create driver configuration
        String driverConfig = String.format("""
                name: Kafka
                driverClass: io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver

                replicationFactor: 1

                topicConfig: |
                  min.insync.replicas=1

                commonConfig: |
                  bootstrap.servers=%s
                  request.timeout.ms=30000
                  default.api.timeout.ms=60000

                producerConfig: |
                  acks=all
                  linger.ms=1
                  batch.size=131072
                  max.in.flight.requests.per.connection=1

                consumerConfig: |
                  auto.offset.reset=earliest
                  enable.auto.commit=false
                  max.partition.fetch.bytes=10485760
                """, kafka.getBootstrapServers());

        Path configPath = Files.createTempFile("kafka-driver-", ".yaml");
        Files.writeString(configPath, driverConfig);
        driverConfigFile = configPath;
        driverConfigFile.toFile().deleteOnExit();

        log.info("Created driver config at: {}", driverConfigFile.toAbsolutePath());

        // Verify Kafka is responsive by creating a test AdminClient
        verifyKafkaConnection();
    }

    private static void verifyKafkaConnection() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("request.timeout.ms", "10000");

        try (var adminClient = org.apache.kafka.clients.admin.AdminClient.create(props)) {
            // Try to list topics to verify connection
            adminClient.listTopics().names().get(10, java.util.concurrent.TimeUnit.SECONDS);
            log.info("Kafka connection verified successfully");
        }
    }

    @AfterAll
    static void tearDownDriver() {
        if (driverConfigFile != null && Files.exists(driverConfigFile)) {
            try {
                Files.deleteIfExists(driverConfigFile);
            } catch (IOException e) {
                log.error("Failed to delete driver config file: {}", driverConfigFile, e);
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void testSimpleProduceConsume() throws Exception {
        // Create workload
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 4;
        workload.producersPerTopic = 2;
        workload.consumerPerSubscription = 2;
        workload.producerRate = 1000;
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
                .isGreaterThan((long) (workload.producerRate * workload.testDurationMinutes * 0.8));
    }

    @Test
    void testMultipleTopics() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 3;
        workload.partitionsPerTopic = 2;
        workload.testDurationMinutes = 1;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;  // Add this
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
        workload.partitionsPerTopic = 8;
        workload.messageSize = 100;
        workload.producersPerTopic = 4;
        workload.consumerPerSubscription = 4;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;  // Add this
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

    private TestResult runBenchmark(Workload workload) throws Exception {
        // Create a new LocalWorker for each test
        LocalWorker worker = new LocalWorker(statsLogger);

        try {
            // Initialize the worker with driver configuration
            worker.initializeDriver(driverConfigFile);

            // Create WorkloadGenerator with the driver name, workload, and worker
            WorkloadGenerator generator = new WorkloadGenerator("Kafka", workload, worker);

            // Run the benchmark
            TestResult result = generator.run();

            // Clean up generator
            generator.close();

            return result;
        } finally {
            // Always close to the worker
            worker.stopAll();
            worker.close();
        }
    }
}
