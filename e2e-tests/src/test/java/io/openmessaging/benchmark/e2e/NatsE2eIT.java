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

import static org.assertj.core.api.Assertions.assertThat;

import io.nats.client.Nats;
import io.nats.client.api.StorageType;
import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.driver.nats.NatsBenchmarkDriver;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

class NatsE2eIT extends BaseE2eIT {
    @Test
    @DriverContract("nats")
    void testFileAndMemoryStreamsWithFanoutAndTimestampPayload() throws Exception {
        try (Network network = Network.newNetwork();
                GenericContainer<?> nats = BrokerFixtures.nats(network);
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            nats.start();
            runner.start();

            Workload file = createSimpleWorkload();
            validateResults(runner.run(BrokerFixtures.driverConfig("nats-file.yaml"), file), file);

            Workload memory = createSimpleWorkload();
            memory.topics = 2;
            memory.subscriptionsPerTopic = 2;
            validateResults(runner.run(BrokerFixtures.driverConfig("nats-memory.yaml"), memory), memory);

            String server = "nats://" + nats.getHost() + ":" + nats.getMappedPort(4222);
            try (var connection = Nats.connect(server)) {
                var streams = connection.jetStreamManagement().getStreams();
                assertThat(streams)
                        .extracting(info -> info.getConfiguration().getStorageType())
                        .contains(StorageType.File, StorageType.Memory);
            }
            verifyPayloadAndDurableGroups(server);
        }
    }

    private void verifyPayloadAndDurableGroups(String server) throws Exception {
        Path config = tempDir.resolve("nats-probe.yaml");
        Files.writeString(
                config,
                """
                name: NATS probe
                driverClass: io.openmessaging.benchmark.driver.nats.NatsBenchmarkDriver
                natsHostUrl: %s
                replicationFactor: 1
                storageType: memory
                """
                        .formatted(server));
        byte[] expected = new byte[32];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = (byte) (i * 13);
        }
        MessageCapture first = new MessageCapture();
        MessageCapture second = new MessageCapture();
        try (NatsBenchmarkDriver driver = new NatsBenchmarkDriver()) {
            driver.initialize(config, null);
            String topic = driver.getTopicNamePrefix() + "-" + UUID.randomUUID();
            driver.createTopic(topic, 1).get(10, TimeUnit.SECONDS);
            try (var firstConsumer = driver.createConsumer(topic, "group-a", first).get(10, TimeUnit.SECONDS);
                    var secondConsumer =
                            driver.createConsumer(topic, "group-b", second).get(10, TimeUnit.SECONDS);
                    var producer = driver.createProducer(topic).get(10, TimeUnit.SECONDS)) {
                long before = System.currentTimeMillis();
                producer.sendAsync(Optional.empty(), expected.clone()).get(10, TimeUnit.SECONDS);
                for (MessageCapture capture : List.of(first, second)) {
                    MessageCapture.Delivery delivery = capture.await();
                    assertThat(delivery.payload()).hasSize(expected.length);
                    assertThat(Arrays.copyOfRange(delivery.payload(), 8, expected.length))
                            .isEqualTo(Arrays.copyOfRange(expected, 8, expected.length));
                    assertThat(ByteBuffer.wrap(delivery.payload()).getLong()).isEqualTo(delivery.timestamp());
                    assertThat(delivery.timestamp()).isBetween(before, System.currentTimeMillis());
                }
                try (var connection = Nats.connect(server)) {
                    assertThat(connection.jetStreamManagement().getConsumerNames(topic))
                            .containsExactlyInAnyOrder("durable-group-a", "durable-group-b");
                }
            }
        }
    }
}
