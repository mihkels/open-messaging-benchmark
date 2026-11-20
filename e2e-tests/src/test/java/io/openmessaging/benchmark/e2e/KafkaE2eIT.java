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
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.RebalanceEvent;
import io.openmessaging.benchmark.driver.SendResult;
import io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
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
    void headersArriveOnTheConsumerSide() throws Exception {
        Map<String, byte[]> headers = Map.of(
                "omb_h0", "trace".getBytes(StandardCharsets.US_ASCII),
                "omb_h1", "route".getBytes(StandardCharsets.US_ASCII));
        try (KafkaBenchmarkDriver driver = new KafkaBenchmarkDriver()) {
            driver.initialize(driverConfigFile, null);
            String topic = driver.getTopicNamePrefix() + "-" + UUID.randomUUID();
            driver.createTopic(topic, 1).get(30, TimeUnit.SECONDS);
            try (var producer = driver.createProducer(topic).get(30, TimeUnit.SECONDS)) {
                producer.sendAsync(Optional.empty(), new byte[] {1, 2, 3}, headers).get(30, TimeUnit.SECONDS);
            }

            Properties props = new Properties();
            props.put("bootstrap.servers", kafka.getBootstrapServers());
            props.put("group.id", "headers-" + UUID.randomUUID());
            props.put("auto.offset.reset", "earliest");
            try (var consumer =
                    new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                consumer.subscribe(List.of(topic));
                ConsumerRecord<byte[], byte[]> received = null;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (received == null && System.nanoTime() < deadline) {
                    for (ConsumerRecord<byte[], byte[]> polled : consumer.poll(Duration.ofMillis(500))) {
                        received = polled;
                    }
                }
                assertThat(received).isNotNull();
                assertThat(received.value()).containsExactly(1, 2, 3);
                for (var entry : headers.entrySet()) {
                    assertThat(received.headers().lastHeader(entry.getKey()).value()).isEqualTo(entry.getValue());
                }
            }
        }
    }

    @Test
    void partitionTelemetryReportsPartitionsAndAssignments() throws Exception {
        List<Integer> receivedPartitions = new CopyOnWriteArrayList<>();
        List<RebalanceEvent> rebalances = new CopyOnWriteArrayList<>();
        ConsumerCallback callback = new ConsumerCallback() {
            @Override
            public void messageReceived(byte[] payload, long publishTimestamp) {
                receivedPartitions.add(UNKNOWN_PARTITION);
            }

            @Override
            public void messageReceived(ByteBuffer payload, long publishTimestamp) {
                receivedPartitions.add(UNKNOWN_PARTITION);
            }

            @Override
            public void messageReceived(byte[] payload, long publishTimestamp, int partition) {
                receivedPartitions.add(partition);
            }

            @Override
            public void partitionsChanged(RebalanceEvent event) {
                rebalances.add(event);
            }
        };
        try (KafkaBenchmarkDriver driver = new KafkaBenchmarkDriver()) {
            driver.initialize(driverConfigFile, null);
            assertThat(driver.supportsPartitionTelemetry()).isTrue();
            String topic = driver.getTopicNamePrefix() + "-" + UUID.randomUUID();
            driver.createTopic(topic, 3).get(30, TimeUnit.SECONDS);
            try (var consumer = driver.createConsumer(topic, "telemetry-" + UUID.randomUUID(), callback)
                            .get(60, TimeUnit.SECONDS);
                    var producer = driver.createProducer(topic).get(30, TimeUnit.SECONDS)) {
                assertThat(rebalances).first().satisfies(event -> {
                    assertThat(event.kind()).isEqualTo(RebalanceEvent.Kind.ASSIGNED);
                    assertThat(event.partitions()).containsExactly(0, 1, 2);
                });
                Set<Integer> sentPartitions = new HashSet<>();
                for (String key : List.of("a", "b", "c", "d", "e", "f")) {
                    SendResult sent = producer.sendAsyncWithResult(Optional.of(key), new byte[] {1}, null)
                            .get(30, TimeUnit.SECONDS);
                    assertThat(sent.partition()).isBetween(0, 2);
                    sentPartitions.add(sent.partition());
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (receivedPartitions.size() < 6 && System.nanoTime() < deadline) {
                    Thread.sleep(100);
                }
                assertThat(receivedPartitions).hasSize(6).allSatisfy(p -> assertThat(p).isBetween(0, 2));
                assertThat(new HashSet<>(receivedPartitions)).isEqualTo(sentPartitions);
            }
        }
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
                    .isNotEmpty()
                    .allSatisfy(topic -> assertThat(topic.partitions()).hasSize(workload.partitionsPerTopic));

            Map<String, Integer> groupsWithDeliveries = new HashMap<>();
            for (var group
                    : adminClient
                            .listGroups(ListGroupsOptions.forConsumerGroups())
                            .all()
                            .get(10, TimeUnit.SECONDS)) {
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
