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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

class JmsPulsarE2eIT extends BaseE2eIT {
    @Test
    @DriverContract("jms")
    void testJmsApisSelectorsAndTransactionsWithAdminDelegate() throws Exception {
        var provider = ArtifactResolver.resolve("jms-pulsar");
        try (Network network = Network.newNetwork();
                GenericContainer<?> broker = BrokerFixtures.pulsar(network);
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            broker.start();
            runner.start();
            runner.copy(provider, "/benchmark/drivers/driver-jms/ext/provider.jar");

            String fixture = Files.readString(BrokerFixtures.driverConfig("jms-pulsar.yaml"));
            Workload jms20 = createSimpleWorkload();
            validateResults(runner.run(fixture, jms20), jms20);

            assertThat(fixture).contains("use20api: true");
            Workload jms11 = createSimpleWorkload();
            validateResults(
                    runner.run(fixture.replace("use20api: true", "use20api: false"), jms11), jms11);

            Workload selectors = createSimpleWorkload();
            var selectorConfig = BrokerFixtures.driverConfig("jms-pulsar-selectors.yaml");
            validateResults(runner.run(selectorConfig, selectors), selectors);
            String selector = Files.readString(selectorConfig);
            assertThat(selector).contains("value: bar");
            runner.verifySelector(
                    "jms",
                    "io.openmessaging.benchmark.driver.jms.JMSBenchmarkDriver",
                    "pulsar",
                    selectorConfig,
                    selector.replace("value: bar", "value: unmatched"));

            Workload transactions = createSimpleWorkload();
            validateResults(
                    runner.run(BrokerFixtures.driverConfig("jms-pulsar-transactions.yaml"), transactions),
                    transactions);
            var namespaces =
                    BrokerFixtures.exec(broker, "bin/pulsar-admin", "namespaces", "list", "public");
            assertThat(namespaces.getStdout()).contains("public/default-");
        }
    }
}
