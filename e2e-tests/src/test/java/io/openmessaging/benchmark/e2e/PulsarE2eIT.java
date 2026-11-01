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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

class PulsarE2eIT extends BaseE2eIT {
    @Test
    @DriverContract("pulsar")
    void testSubscriptionModesBatchingAndDeduplication() throws Exception {
        try (Network network = Network.newNetwork();
                GenericContainer<?> broker = BrokerFixtures.pulsar(network);
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            broker.start();
            runner.start();
            Workload failover = createSimpleWorkload();
            var config = BrokerFixtures.driverConfig("pulsar.yaml");
            validateResults(runner.run(config, failover), failover);
            runner.verifyOrderedPayloadsAndTimestamps(
                    "pulsar", "io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkDriver", config);

            Workload shared = createSimpleWorkload();
            shared.consumerPerSubscription = 2;
            shared.subscriptionsPerTopic = 2;
            validateResults(
                    runner.run(BrokerFixtures.driverConfig("pulsar-shared-nobatch.yaml"), shared), shared);

            Workload keyShared = createSimpleWorkload();
            keyShared.partitionsPerTopic = 4;
            keyShared.producersPerTopic = 2;
            keyShared.consumerPerSubscription = 2;
            keyShared.keyDistributor = KeyDistributorType.KEY_ROUND_ROBIN;
            validateResults(
                    runner.run(BrokerFixtures.driverConfig("pulsar-key-shared-dedup.yaml"), keyShared),
                    keyShared);

            verifyCreatedTopics(broker, "benchmark", 2, 1, false);
            verifyCreatedTopics(broker, "benchmark-dedup", 1, keyShared.partitionsPerTopic, true);
        }
    }

    private static void verifyCreatedTopics(
            GenericContainer<?> broker, String tenant, int minimum, int partitions, boolean deduplication)
            throws Exception {
        var result = BrokerFixtures.exec(broker, "bin/pulsar-admin", "namespaces", "list", tenant);
        var namespaces =
                result
                        .getStdout()
                        .lines()
                        .map(String::trim)
                        .filter(namespace -> namespace.startsWith(tenant + "/ns-"))
                        .toList();
        assertThat(namespaces).hasSizeGreaterThanOrEqualTo(minimum);
        for (String namespace : namespaces) {
            var resultTopics =
                    BrokerFixtures.exec(broker, "bin/pulsar-admin", "topics", "list", namespace);
            var topics =
                    resultTopics
                            .getStdout()
                            .lines()
                            .map(String::trim)
                            .filter(topic -> topic.contains(namespace + "/test-"))
                            .toList();
            assertThat(topics).isNotEmpty();
            if (partitions > 1) {
                assertThat(topics).hasSize(partitions);
                for (int i = 0; i < partitions; i++) {
                    String suffix = "-partition-" + i;
                    assertThat(topics).anySatisfy(topic -> assertThat(topic).endsWith(suffix));
                }
            }
            var policy =
                    BrokerFixtures.exec(
                            broker, "bin/pulsar-admin", "namespaces", "get-deduplication", namespace);
            assertThat(policy.getStdout().trim()).isEqualTo(Boolean.toString(deduplication));
        }
    }
}
