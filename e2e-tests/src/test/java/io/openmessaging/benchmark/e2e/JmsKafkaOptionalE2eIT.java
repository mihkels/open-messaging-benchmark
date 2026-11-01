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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.openmessaging.benchmark.Workload;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class JmsKafkaOptionalE2eIT extends BaseE2eIT {
    @Test
    @DriverContract("jms")
    void testLicensedKafkaJmsProvider() throws Exception {
        assumeTrue(
                Boolean.parseBoolean(System.getProperty("omb.e2e.jms.kafka.enabled", "false")),
                "Optional Kafka JMS provider is disabled; supply its licensed jar and enable it explicitly");
        String path = System.getProperty("omb.e2e.jms.kafka.jar", "");
        assertThat(path)
                .as("omb.e2e.jms.kafka.jar must be supplied when the Kafka JMS case is enabled")
                .isNotBlank();
        Path provider = Path.of(path);
        assertThat(provider).as("Kafka JMS provider jar").isRegularFile();
        try (JarFile jar = new JarFile(provider.toFile())) {
            assertThat(jar.getEntry("io/confluent/kafka/jms/KafkaConnectionFactory.class"))
                    .as("Kafka JMS connection factory in the supplied jar")
                    .isNotNull();
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read Kafka JMS provider jar " + provider, e);
        }

        try (Network network = Network.newNetwork();
                KafkaContainer kafka =
                        new KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0"))
                                .withNetwork(network)
                                .withNetworkAliases("kafka")
                                .withListener("kafka:19092");
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            kafka.start();
            runner.start();
            runner.copy(provider, "/benchmark/drivers/driver-jms/ext/provider.jar");
            String fixture = Files.readString(BrokerFixtures.driverConfig("jms-kafka.yaml"));
            assertThat(fixture).contains("kafka:9092");
            Workload workload = createSimpleWorkload();
            validateResults(runner.run(fixture.replace("kafka:9092", "kafka:19092"), workload), workload);
        }
    }
}
