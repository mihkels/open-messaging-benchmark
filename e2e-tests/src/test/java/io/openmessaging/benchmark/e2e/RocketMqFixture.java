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

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

final class RocketMqFixture implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RocketMqFixture.class);
    private static final DockerImageName IMAGE = DockerImageName.parse("apache/rocketmq:5.3.2");

    final Network network = Network.newNetwork();
    final GenericContainer<?> namesrv =
            new GenericContainer<>(IMAGE)
                    .withNetwork(network)
                    .withNetworkAliases("namesrv")
                    .withEnv("JAVA_OPT_EXT", "-Xms64m -Xmx256m -Xmn64m")
                    .withExposedPorts(9876)
                    .withCommand("sh", "mqnamesrv")
                    .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
    final GenericContainer<?> broker =
            new GenericContainer<>(IMAGE)
                    .withNetwork(network)
                    .withNetworkAliases("broker")
                    .withEnv("NAMESRV_ADDR", "namesrv:9876")
                    .withEnv("JAVA_OPT_EXT", "-Xms128m -Xmx512m -Xmn128m")
                    .withCommand(
                            "sh",
                            "-c",
                            "printf 'brokerClusterName=DefaultCluster\\nbrokerName=broker-a\\n"
                                    + "brokerId=0\\nbrokerIP1=broker\\nautoCreateTopicEnable=true\\n"
                                    + "autoCreateSubscriptionGroup=true\\n' > /tmp/broker.conf"
                                    + " && exec sh mqbroker -c /tmp/broker.conf")
                    .waitingFor(
                            Wait.forSuccessfulCommand(
                                            "sh mqadmin clusterList -n namesrv:9876 2>/dev/null | grep -q broker-a")
                                    .withStartupTimeout(Duration.ofMinutes(5)));
    private final GenericContainer<?> proxy;

    RocketMqFixture(boolean withProxy) {
        proxy =
                withProxy
                        ? new GenericContainer<>(IMAGE)
                                .withNetwork(network)
                                .withNetworkAliases("proxy")
                                .withEnv("NAMESRV_ADDR", "namesrv:9876")
                                .withEnv("JAVA_OPT_EXT", "-Xms64m -Xmx256m")
                                .withExposedPorts(8081)
                                .withCommand("sh", "mqproxy", "-n", "namesrv:9876")
                                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)))
                        : null;
    }

    void start() {
        namesrv.start();
        broker.start();
        if (proxy != null) {
            proxy.start();
        }
    }

    @Override
    public void close() {
        try (network;
                namesrv;
                broker;
                proxy) {
            log.debug("Stopping RocketMQ fixture");
        }
    }
}
