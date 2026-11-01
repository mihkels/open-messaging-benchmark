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
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

/** End-to-end tests for Pravega on a private broker network. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PravegaE2eIT extends BaseE2eIT {
    private static final String SCOPE = "ombscope";

    private Network network;
    private GenericContainer<?> pravega;

    @BeforeAll
    void startPravega() {
        network = Network.newNetwork();
        pravega =
                new GenericContainer<>(DockerImageName.parse("pravega/pravega:0.13.0"))
                        .withNetwork(network)
                        .withNetworkAliases("pravega")
                        .withCreateContainerCmdModifier(command -> command.withPlatform("linux/amd64"))
                        .withEnv("HOST_IP", "pravega")
                        .withEnv("JAVA_OPTS", "-Xmx1g -XX:MaxDirectMemorySize=1g")
                        .withCommand("standalone")
                        .withExposedPorts(9091)
                        .waitingFor(
                                new WaitAllStrategy()
                                        .withStrategy(Wait.forLogMessage(".*Pravega Sandbox is running.*", 1))
                                        .withStrategy(Wait.forHttp("/v1/scopes").forPort(9091))
                                        .withStartupTimeout(Duration.ofMinutes(5)));
        try {
            pravega.start();
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "Pravega requires the linux/amd64 image and working amd64 emulation on ARM hosts",
                    e);
        }
    }

    @AfterAll
    void stopPravega() {
        try {
            if (pravega != null) {
                pravega.close();
            }
        } finally {
            if (network != null) {
                network.close();
            }
        }
    }

    @Test
    @DriverContract("pravega")
    void testMultipleStreamsAndSegments() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 3;
        workload.partitionsPerTopic = 2;
        workload.producersPerTopic = 2;
        workload.consumerPerSubscription = 2;
        workload.keyDistributor = KeyDistributorType.KEY_ROUND_ROBIN;
        Map<String, Integer> before = streams();
        SmokeResult result = runPravega(workload, false);
        validateResults(result, workload);
        Map<String, Integer> created = new HashMap<>(streams());
        before.keySet().forEach(created::remove);
        assertThat(created).hasSize(workload.topics);
        assertThat(created.values())
                .allSatisfy(segments -> assertThat(segments).isEqualTo(workload.partitionsPerTopic));
        Path config = tempDir.resolve("pravega-probe.yaml");
        Files.writeString(config, pravegaConfig(false));
        try (NetworkBenchmark runner = new NetworkBenchmark(network)) {
            runner.start();
            runner.verifyOrderedPayloadsAndTimestamps(
                    "pravega", "io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver", config);
        }
    }

    @Test
    void testTransactionCommitAndTimestamp() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.partitionsPerTopic = 4;
        workload.keyDistributor = KeyDistributorType.KEY_ROUND_ROBIN;
        SmokeResult result = runPravega(workload, true);
        validateResults(result, workload);
    }

    private SmokeResult runPravega(Workload workload, boolean transactional) throws Exception {
        try (NetworkBenchmark runner = new NetworkBenchmark(network)) {
            runner.start();
            return runner.run(pravegaConfig(transactional), workload);
        }
    }

    private String pravegaConfig(boolean transactional) {
        return """
                name: Pravega
                driverClass: io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver
                client:
                  controllerURI: tcp://pravega:9090
                  scopeName: %s
                writer:
                  enableConnectionPooling: true
                includeTimestampInEvent: true
                deleteStreams: false
                enableTransaction: %s
                eventsPerTransaction: 10
                """
                .formatted(SCOPE, transactional);
    }

    private Map<String, Integer> streams() throws Exception {
        URI uri =
                URI.create(
                        "http://"
                                + pravega.getHost()
                                + ":"
                                + pravega.getMappedPort(9091)
                                + "/v1/scopes/"
                                + SCOPE
                                + "/streams");
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build();
        HttpResponse<String> response =
                HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 404) {
            return Map.of();
        }
        assertThat(response.statusCode()).as("Pravega stream list: %s", response.body()).isEqualTo(200);
        var streams = new ObjectMapper().readTree(response.body()).get("streams");
        assertThat(streams).isNotNull();
        Map<String, Integer> segments = new HashMap<>();
        streams.forEach(
                stream -> {
                    String name = stream.get("streamName").asString();
                    var scaling = stream.get("scalingPolicy");
                    assertThat(scaling).as("scaling policy for stream %s", name).isNotNull();
                    var minimum = scaling.get("minSegments");
                    assertThat(minimum).as("minimum segments for stream %s", name).isNotNull();
                    segments.put(name, minimum.asInt());
                });
        return segments;
    }
}
