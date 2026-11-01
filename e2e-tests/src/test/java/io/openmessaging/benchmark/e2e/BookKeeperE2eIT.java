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
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

class BookKeeperE2eIT extends BaseE2eIT {
    private static final DockerImageName IMAGE = DockerImageName.parse("apachepulsar/pulsar:4.0.6");

    @Test
    @DriverContract("bookkeeper")
    void testDistributedLogStreamAndOrderedPayloads() throws Exception {
        try (Network network = Network.newNetwork();
                GenericContainer<?> zookeeper =
                        new GenericContainer<>(IMAGE)
                                .withNetwork(network)
                                .withNetworkAliases("zookeeper")
                                .withEnv("PULSAR_MEM", "-Xmx256m")
                                .withCommand(
                                        "sh",
                                        "-c",
                                        "bin/apply-config-from-env.py conf/zookeeper.conf"
                                                + " && exec bin/pulsar zookeeper")
                                .waitingFor(
                                        Wait.forSuccessfulCommand("bin/pulsar-zookeeper-ruok.sh")
                                                .withStartupTimeout(Duration.ofMinutes(2)));
                GenericContainer<?> bookie =
                        new GenericContainer<>(IMAGE)
                                .withNetwork(network)
                                .withNetworkAliases("bookie")
                                .withCreateContainerCmdModifier(command -> command.withHostName("bookie"))
                                .withEnv("BOOKIE_MEM", "-Xms256m -Xmx512m -XX:MaxDirectMemorySize=512m")
                                .withEnv("metadataServiceUri", "zk+hierarchical://zookeeper:2181/ledgers")
                                .withEnv("zkServers", "zookeeper:2181")
                                .withEnv("advertisedAddress", "bookie")
                                .withEnv("bookiePort", "3181")
                                .withEnv("httpServerEnabled", "true")
                                .withEnv("httpServerPort", "8000")
                                .withEnv("journalDirectories", "/tmp/bk-journal")
                                .withEnv("ledgerDirectories", "/tmp/bk-ledgers")
                                .withEnv("dbStorage_writeCacheMaxSizeMb", "64")
                                .withEnv("dbStorage_readAheadCacheMaxSizeMb", "64")
                                .withEnv("dbStorage_rocksDB_blockCacheSize", "16777216")
                                .withCommand(
                                        "sh",
                                        "-c",
                                        "bin/apply-config-from-env.py conf/bookkeeper.conf"
                                                + " && exec bin/bookkeeper bookie")
                                .waitingFor(
                                        Wait.forSuccessfulCommand(
                                                        "curl -fsS http://localhost:8000/api/v1/bookie/state"
                                                                + " | grep -q '\"running\" : true'")
                                                .withStartupTimeout(Duration.ofMinutes(3)));
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            zookeeper.start();
            BrokerFixtures.exec(
                    zookeeper,
                    "sh",
                    "-c",
                    "metadataServiceUri=zk+hierarchical://zookeeper:2181/ledgers"
                            + " zkServers=zookeeper:2181 bin/apply-config-from-env.py conf/bookkeeper.conf"
                            + " && bin/bookkeeper shell initnewcluster");
            bookie.start();
            runner.start();
            var bind =
                    runner.exec(
                            "sh",
                            "-c",
                            "java -cp '/benchmark/drivers/driver-bookkeeper/lib/*'"
                                    + " dlshade.org.apache.distributedlog.tools.Tool"
                                    + " dlshade.org.apache.distributedlog.admin.DistributedLogAdmin"
                                    + " bind -l /ledgers -s zookeeper:2181"
                                    + " -c distributedlog://zookeeper:2181/streams");
            assertThat(bind.getExitCode())
                    .as("binding DistributedLog: %s\n%s", bind.getStdout(), bind.getStderr())
                    .isZero();

            var config = BrokerFixtures.driverConfig("bookkeeper.yaml");
            Workload workload = createSimpleWorkload();
            validateResults(runner.run(config, workload), workload);
            runner.verifyOrderedPayloads(
                    "bookkeeper", "io.openmessaging.benchmark.driver.bookkeeper.DlogBenchmarkDriver", config);
        }
    }
}
