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

import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.driver.natsStreaming.NatsStreamingBenchmarkDriver;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

class NatsStreamingE2eIT extends BaseE2eIT {
    @Test
    @DriverContract("nats-streaming")
    void testStreamingQueueGroupsAndFanout() throws Exception {
        try (Network network = Network.newNetwork();
                GenericContainer<?> server = BrokerFixtures.natsStreaming(network);
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            server.start();
            runner.start();
            Workload workload = createSimpleWorkload();
            workload.topics = 2;
            workload.subscriptionsPerTopic = 2;
            validateResults(
                    runner.run(BrokerFixtures.driverConfig("nats-streaming.yaml"), workload), workload);

            URI uri =
                    URI.create(
                            "http://"
                                    + server.getHost()
                                    + ":"
                                    + server.getMappedPort(8222)
                                    + "/streaming/channelsz");
            HttpResponse<String> response =
                    HttpClient.newHttpClient()
                            .send(
                                    HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build(),
                                    HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).contains("benchmark");
            verifyNonDurableSubscriptionAndServerTimestamp(server);
        }
    }

    private void verifyNonDurableSubscriptionAndServerTimestamp(GenericContainer<?> server)
            throws Exception {
        Path config = tempDir.resolve("nats-streaming-probe.yaml");
        Files.writeString(
                config,
                """
                name: NATS Streaming probe
                driverClass: io.openmessaging.benchmark.driver.natsStreaming.NatsStreamingBenchmarkDriver
                natsHostUrl: nats://%s:%d
                clusterId: omb-e2e
                maxPubAcksInFlight: 1024
                maxInFlight: 256
                """
                        .formatted(server.getHost(), server.getMappedPort(4222)));
        byte[] firstPayload = "first".getBytes(StandardCharsets.UTF_8);
        byte[] offlinePayload = "while-offline".getBytes(StandardCharsets.UTF_8);
        byte[] lastPayload = "after-reconnect".getBytes(StandardCharsets.UTF_8);
        MessageCapture first = new MessageCapture();
        MessageCapture reconnected = new MessageCapture();
        try (NatsStreamingBenchmarkDriver driver = new NatsStreamingBenchmarkDriver()) {
            driver.initialize(config, null);
            String topic = driver.getTopicNamePrefix() + "-" + UUID.randomUUID();
            driver.createTopic(topic, 1).get(10, TimeUnit.SECONDS);
            try (var producer = driver.createProducer(topic).get(10, TimeUnit.SECONDS)) {
                try (var consumer = driver.createConsumer(topic, "group", first).get(10, TimeUnit.SECONDS)) {
                    long before = System.currentTimeMillis();
                    producer.sendAsync(Optional.empty(), firstPayload).get(10, TimeUnit.SECONDS);
                    MessageCapture.Delivery delivery = first.await();
                    assertThat(delivery.payload()).isEqualTo(firstPayload);
                    assertThat(delivery.timestamp()).isBetween(before, System.currentTimeMillis());
                }
                producer.sendAsync(Optional.empty(), offlinePayload).get(10, TimeUnit.SECONDS);
                try (var consumer =
                        driver.createConsumer(topic, "group", reconnected).get(10, TimeUnit.SECONDS)) {
                    producer.sendAsync(Optional.empty(), lastPayload).get(10, TimeUnit.SECONDS);
                    assertThat(reconnected.await().payload()).isEqualTo(lastPayload);
                }
            }
        }
    }
}
