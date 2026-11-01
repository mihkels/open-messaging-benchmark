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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

class NsqE2eIT extends BaseE2eIT {
    @Test
    @DriverContract("nsq")
    void testLookupRegistrationAndChannelDelivery() throws Exception {
        try (Network network = Network.newNetwork();
                GenericContainer<?> lookup =
                        new GenericContainer<>(DockerImageName.parse("nsqio/nsq:v1.3.0"))
                                .withNetwork(network)
                                .withNetworkAliases("nsqlookupd")
                                .withExposedPorts(4161)
                                .withCommand("/nsqlookupd")
                                .waitingFor(Wait.forHttp("/ping").forPort(4161));
                GenericContainer<?> nsqd =
                        new GenericContainer<>(DockerImageName.parse("nsqio/nsq:v1.3.0"))
                                .withNetwork(network)
                                .withNetworkAliases("nsqd")
                                .withExposedPorts(4151)
                                .withCommand(
                                        "/nsqd",
                                        "--broadcast-address=nsqd",
                                        "--lookupd-tcp-address=nsqlookupd:4160",
                                        "--data-path=/tmp")
                                .waitingFor(Wait.forHttp("/ping").forPort(4151));
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            lookup.start();
            nsqd.start();
            runner.start();

            Workload workload = createSimpleWorkload();
            workload.topics = 2;
            workload.subscriptionsPerTopic = 2;
            validateResults(runner.run(BrokerFixtures.driverConfig("nsq.yaml"), workload), workload);

            URI stats =
                    URI.create(
                            "http://" + nsqd.getHost() + ":" + nsqd.getMappedPort(4151) + "/stats?format=json");
            HttpResponse<String> response =
                    HttpClient.newHttpClient()
                            .send(
                                    HttpRequest.newBuilder(stats).timeout(Duration.ofSeconds(10)).GET().build(),
                                    HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            var topics = new ObjectMapper().readTree(response.body()).get("topics");
            assertThat(topics).isNotNull();
            int matched = 0;
            for (var topic : topics) {
                if (topic.get("topic_name").asString().startsWith("Nsq-Benchmark")) {
                    matched++;
                    assertThat(topic.get("message_count").asLong()).isPositive();
                    assertThat(topic.get("channels").size()).isEqualTo(workload.subscriptionsPerTopic);
                    topic
                            .get("channels")
                            .forEach(channel -> assertThat(channel.get("message_count").asLong()).isPositive());
                }
            }
            assertThat(matched).isEqualTo(workload.topics);
        }
    }
}
