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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

class KopE2eIT extends BaseE2eIT {
    @Test
    @DriverContract("kop")
    void testAllProducerAndConsumerProtocolCombinations() throws Exception {
        Path nar = ArtifactResolver.resolve("kop-nar");
        try (Network network = Network.newNetwork();
                GenericContainer<?> broker =
                        new GenericContainer<>(DockerImageName.parse("apachepulsar/pulsar:2.10.4"))
                                .withNetwork(network)
                                .withNetworkAliases("pulsar-kop")
                                .withCreateContainerCmdModifier(
                                        command -> command.withPlatform("linux/amd64").withHostName("pulsar-kop"))
                                .withCopyFileToContainer(MountableFile.forHostPath(nar), "/pulsar/kop.nar")
                                .withEnv("PULSAR_MEM", "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=512m")
                                .withEnv("PULSAR_PREFIX_advertisedAddress", "pulsar-kop")
                                .withEnv("PULSAR_PREFIX_messagingProtocols", "kafka")
                                .withEnv("PULSAR_PREFIX_protocolHandlerDirectory", "/pulsar/protocols")
                                .withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "partitioned")
                                .withEnv("PULSAR_PREFIX_kafkaListeners", "PLAINTEXT://0.0.0.0:9092")
                                .withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners", "PLAINTEXT://pulsar-kop:9092")
                                .withEnv(
                                        "PULSAR_PREFIX_brokerEntryMetadataInterceptors",
                                        "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor")
                                .withEnv("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false")
                                .withEnv("PULSAR_PREFIX_kafkaTransactionCoordinatorEnabled", "true")
                                .withEnv("PULSAR_PREFIX_brokerDeduplicationEnabled", "true")
                                .withCommand(
                                        "sh",
                                        "-c",
                                        "mkdir -p /pulsar/protocols && mv /pulsar/kop.nar /pulsar/protocols/kop.nar"
                                                + " && bin/apply-config-from-env.py conf/standalone.conf"
                                                + " && exec bin/pulsar standalone -nfw -nss")
                                .waitingFor(
                                        Wait.forSuccessfulCommand(
                                                        "curl -fsS http://localhost:8080/admin/v2/brokers/health"
                                                                + " && bash -c 'echo > /dev/tcp/localhost/9092'")
                                                .withStartupTimeout(Duration.ofMinutes(8)));
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            try {
                broker.start();
            } catch (RuntimeException e) {
                throw new IllegalStateException(
                        "KoP requires the linux/amd64 image and working amd64 emulation on ARM hosts", e);
            }
            runner.start();
            List<String> fixtures =
                    List.of(
                            "kop-kafka-to-kafka.yaml", "kop-kafka-to-pulsar.yaml", "kop-pulsar-to-kafka.yaml");
            for (String fixture : fixtures) {
                Path config = BrokerFixtures.driverConfig(fixture);
                Workload workload = createSimpleWorkload();
                validateResults(runner.run(config, workload), workload);
                runner.verifyOrderedPayloads(
                        "kop", "io.openmessaging.benchmark.driver.kop.KopBenchmarkDriver", config);
            }

            String nativeConfig =
                    Files.readString(BrokerFixtures.driverConfig("kop-pulsar-to-kafka.yaml"));
            assertThat(nativeConfig).contains("consumerType: kafka");
            Path nativeFixture = tempDir.resolve("kop-pulsar-to-pulsar.yaml");
            Files.writeString(
                    nativeFixture, nativeConfig.replace("consumerType: kafka", "consumerType: pulsar"));
            Workload workload = createSimpleWorkload();
            validateResults(runner.run(nativeFixture, workload), workload);
            runner.verifyOrderedPayloads(
                    "kop", "io.openmessaging.benchmark.driver.kop.KopBenchmarkDriver", nativeFixture);
        }
    }
}
