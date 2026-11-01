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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

final class BrokerFixtures {
    private BrokerFixtures() {}

    static Path driverConfig(String file) {
        Path config =
                Path.of(System.getProperty("omb.home")).resolve("e2e-tests/compose/drivers").resolve(file);
        if (!Files.isRegularFile(config)) {
            throw new IllegalStateException("Driver fixture not found: " + config);
        }
        return config;
    }

    static GenericContainer<?> nats(Network network) {
        return new GenericContainer<>(DockerImageName.parse("nats:2.11-alpine"))
                .withNetwork(network)
                .withNetworkAliases("nats")
                .withExposedPorts(4222, 8222)
                .withCommand("-js", "-sd", "/data", "-m", "8222")
                .waitingFor(
                        Wait.forHttp("/healthz?js-enabled-only=true")
                                .forPort(8222)
                                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    static GenericContainer<?> natsStreaming(Network network) {
        return new GenericContainer<>(DockerImageName.parse("nats-streaming:0.25.6"))
                .withNetwork(network)
                .withNetworkAliases("nats-streaming")
                .withExposedPorts(4222, 8222)
                .withCommand("-cid", "omb-e2e", "-m", "8222", "-store", "memory")
                .waitingFor(
                        Wait.forHttp("/streaming/serverz")
                                .forPort(8222)
                                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    static GenericContainer<?> artemis(Network network) {
        return new GenericContainer<>(DockerImageName.parse("apache/artemis:2.57.0"))
                .withNetwork(network)
                .withNetworkAliases("artemis")
                .withEnv("ANONYMOUS_LOGIN", "true")
                .withEnv("JAVA_ARGS_APPEND", "-Xmx768m")
                .waitingFor(
                        Wait.forSuccessfulCommand(
                                        "/var/lib/artemis-instance/bin/artemis check node --silent "
                                                + "--user artemis --password artemis")
                                .withStartupTimeout(Duration.ofMinutes(3)));
    }

    static GenericContainer<?> mqtt(Network network) {
        return new GenericContainer<>(DockerImageName.parse("eclipse-mosquitto:2.0.22"))
                .withNetwork(network)
                .withNetworkAliases("mqtt")
                .withCommand(
                        "sh",
                        "-c",
                        "printf 'listener 1883 0.0.0.0\\nallow_anonymous true\\n' > /tmp/mosquitto.conf"
                                + " && exec mosquitto -c /tmp/mosquitto.conf")
                .waitingFor(
                        Wait.forSuccessfulCommand(
                                        "mosquitto_pub -h localhost -t health -m ok -r -q 1"
                                                + " && mosquitto_sub -h localhost -t health -C 1 -W 5")
                                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    static GenericContainer<?> pulsar(Network network) {
        return new GenericContainer<>(DockerImageName.parse("apachepulsar/pulsar:4.0.6"))
                .withNetwork(network)
                .withNetworkAliases("pulsar")
                .withCreateContainerCmdModifier(command -> command.withHostName("pulsar"))
                .withEnv("PULSAR_MEM", "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=512m")
                .withEnv("PULSAR_PREFIX_advertisedAddress", "pulsar")
                .withEnv("PULSAR_PREFIX_transactionCoordinatorEnabled", "true")
                .withEnv("PULSAR_PREFIX_systemTopicEnabled", "true")
                .withEnv("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false")
                .withCommand(
                        "sh",
                        "-c",
                        "bin/apply-config-from-env.py conf/standalone.conf"
                                + " && exec bin/pulsar standalone -nfw -nss")
                .waitingFor(
                        Wait.forSuccessfulCommand("curl -fsS http://localhost:8080/admin/v2/brokers/health")
                                .withStartupTimeout(Duration.ofMinutes(5)));
    }

    static Container.ExecResult exec(GenericContainer<?> container, String... command)
            throws Exception {
        Container.ExecResult result = container.execInContainer(command);
        assertThat(result.getExitCode())
                .as(
                        "command %s failed: %s\n%s",
                        String.join(" ", command), result.getStdout(), result.getStderr())
                .isZero();
        return result;
    }
}
