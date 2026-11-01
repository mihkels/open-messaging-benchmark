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
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for Kafka. Testing is done by creating a simple workload and running the benchmark.
 */
@Testcontainers
class KafkaE2eIT extends BaseE2eIT {
    private static final Logger log = LoggerFactory.getLogger(KafkaE2eIT.class);

    private static final String KAFKA_IMAGE = "apache/kafka:4.4.0";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE));

    private Path driverConfigFile;

    @BeforeEach
    void setupDriver() throws Exception {
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
                  enable.idempotence=true
                  compression.type=lz4

                consumerConfig: |
                  auto.offset.reset=earliest
                  enable.auto.commit=false
                  max.partition.fetch.bytes=10485760
                """, kafka.getBootstrapServers());

        driverConfigFile = tempDir.resolve("kafka-driver.yaml");
        Files.writeString(driverConfigFile, driverConfig);
    }

    @BeforeAll
    static void verifyKafkaConnection() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("request.timeout.ms", "10000");

        try (var adminClient = org.apache.kafka.clients.admin.AdminClient.create(props)) {
            // Try to list topics to verify connection
            adminClient.listTopics().names().get(10, java.util.concurrent.TimeUnit.SECONDS);
            log.info("Kafka connection verified successfully");
        }
    }

    @Test
    @DriverContract("kafka")
    void testSimpleProduceConsume() throws Exception {
        // Create workload
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 4;
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
        workload.partitionsPerTopic = 2;
        workload.subscriptionsPerTopic = 2;
        workload.keyDistributor = KeyDistributorType.KEY_ROUND_ROBIN;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;  // Add this
        workload.randomBytesRatio = 0.5;

        Set<String> topicsBefore = kafkaTopics();
        SmokeResult result = runBenchmark(workload);

        validateResults(result, workload);
        Set<String> createdTopics = new HashSet<>(kafkaTopics());
        createdTopics.removeAll(topicsBefore);
        createdTopics.removeIf(topic -> !topic.startsWith("test-topic"));
        assertThat(createdTopics).hasSize(workload.topics);
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        try (var adminClient = org.apache.kafka.clients.admin.AdminClient.create(props)) {
            var descriptions = adminClient.describeTopics(createdTopics).allTopicNames().get(10, TimeUnit.SECONDS);
            assertThat(descriptions.values())
                    .allSatisfy(topic -> assertThat(topic.partitions()).hasSize(workload.partitionsPerTopic));

            Map<String, Integer> groupsWithDeliveries = new HashMap<>();
            for (var group : adminClient.listConsumerGroups().all().get(10, TimeUnit.SECONDS)) {
                var offsets =
                        adminClient
                                .listConsumerGroupOffsets(group.groupId())
                                .partitionsToOffsetAndMetadata()
                                .get(10, TimeUnit.SECONDS);
                Set<String> deliveredTopics = new HashSet<>();
                offsets.forEach(
                        (partition, offset) -> {
                            if (createdTopics.contains(partition.topic())) {
                                assertThat(offset.offset()).isPositive();
                                deliveredTopics.add(partition.topic());
                            }
                        });
                deliveredTopics.forEach(topic -> groupsWithDeliveries.merge(topic, 1, Integer::sum));
            }
            assertThat(groupsWithDeliveries.keySet()).containsExactlyInAnyOrderElementsOf(createdTopics);
            assertThat(groupsWithDeliveries.values())
                    .allSatisfy(count -> assertThat(count).isEqualTo(workload.subscriptionsPerTopic));
        }
    }

    @Test
    void testMultipleProducersAndConsumers() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 8;
        workload.messageSize = 100;
        workload.producersPerTopic = 4;
        workload.consumerPerSubscription = 4;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;  // Add this
        workload.randomBytesRatio = 0.5;

        SmokeResult result = runBenchmark(workload);

        validateResults(result, workload);

    }

    private SmokeResult runBenchmark(Workload workload) throws Exception {
        return runBenchmark("Kafka", driverConfigFile, workload);
    }

    private static Set<String> kafkaTopics() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        try (var adminClient = org.apache.kafka.clients.admin.AdminClient.create(props)) {
            return adminClient.listTopics().names().get(10, TimeUnit.SECONDS);
        }
    }
}
