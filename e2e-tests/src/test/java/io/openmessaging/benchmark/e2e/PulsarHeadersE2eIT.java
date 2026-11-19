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

import io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkDriver;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Sends with the in-process Pulsar driver and reads the headers back with a native consumer. */
@Testcontainers
class PulsarHeadersE2eIT {

    @Container
    // Unlike BrokerFixtures.pulsar, keeps the default advertised address so the host can connect
    static GenericContainer<?> pulsar = new GenericContainer<>(DockerImageName.parse("apachepulsar/pulsar:4.0.6"))
            .withExposedPorts(6650, 8080)
            .withCommand(
                    "sh",
                    "-c",
                    "bin/apply-config-from-env.py conf/standalone.conf && exec bin/pulsar standalone -nfw -nss")
            .waitingFor(Wait.forHttp("/admin/v2/clusters")
                    .forPort(8080)
                    .forResponsePredicate(body -> body.contains("standalone")));

    private static String serviceUrl() {
        return "pulsar://" + pulsar.getHost() + ":" + pulsar.getMappedPort(6650);
    }

    @TempDir
    Path tempDir;

    @Test
    void headersArriveOnTheConsumerSide() throws Exception {
        Path config = tempDir.resolve("pulsar-headers.yaml");
        Files.writeString(
                config,
                """
                name: Pulsar headers
                driverClass: io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkDriver
                client:
                  serviceUrl: %s
                  httpUrl: %s
                  clusterName: standalone
                  namespacePrefix: benchmark/ns
                  topicType: persistent
                  persistence:
                    ensembleSize: 1
                    writeQuorum: 1
                    ackQuorum: 1
                producer:
                  batchingEnabled: false
                consumer:
                  subscriptionType: Failover
                """
                        .formatted(serviceUrl(), "http://" + pulsar.getHost() + ":" + pulsar.getMappedPort(8080)));
        Map<String, byte[]> headers = Map.of(
                "omb_h0", "trace".getBytes(StandardCharsets.US_ASCII),
                "omb_h1", "route".getBytes(StandardCharsets.US_ASCII));

        try (PulsarBenchmarkDriver driver = new PulsarBenchmarkDriver();
                PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl()).build()) {
            driver.initialize(config, null);
            String topic = driver.getTopicNamePrefix() + "-" + UUID.randomUUID();
            driver.createTopic(topic, 1).get(30, TimeUnit.SECONDS);
            try (var consumer = client.newConsumer(Schema.BYTES)
                            .topic(topic)
                            .subscriptionName("headers")
                            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                            .subscribe();
                    var producer = driver.createProducer(topic).get(30, TimeUnit.SECONDS)) {
                producer.sendAsync(Optional.empty(), new byte[] {1, 2, 3}, headers).get(30, TimeUnit.SECONDS);

                Message<byte[]> received = consumer.receive(30, TimeUnit.SECONDS);
                assertThat(received).isNotNull();
                assertThat(received.getValue()).containsExactly(1, 2, 3);
                assertThat(received.getProperties()).containsEntry("omb_h0", "trace").containsEntry("omb_h1", "route");
            }
        }
    }
}
