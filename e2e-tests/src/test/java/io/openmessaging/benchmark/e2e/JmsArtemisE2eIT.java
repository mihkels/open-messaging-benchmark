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

import io.openmessaging.benchmark.Workload;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

class JmsArtemisE2eIT extends BaseE2eIT {
    @Test
    @DriverContract("jms")
    void testJms20AndTransactedJms11() throws Exception {
        var provider = ArtifactResolver.resolve("jms-artemis");
        try (Network network = Network.newNetwork();
                GenericContainer<?> broker = BrokerFixtures.artemis(network);
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            broker.start();
            runner.start();
            runner.copy(provider, "/benchmark/drivers/driver-jms/ext/provider.jar");

            var modern = BrokerFixtures.driverConfig("jms-artemis.yaml");
            Workload jms20 = createSimpleWorkload();
            jms20.subscriptionsPerTopic = 2;
            validateResults(runner.run(modern, jms20), jms20);
            runner.verifyOrderedPayloads(
                    "jms", "io.openmessaging.benchmark.driver.jms.JMSBenchmarkDriver", modern);

            String selector =
                    Files.readString(modern)
                            + """
                            properties:
                              - name: foo
                                value: bar
                            messageSelector: foo = 'bar'
                            """;
            var selectorConfig = tempDir.resolve("jms-artemis-selectors.yaml");
            Files.writeString(selectorConfig, selector);
            runner.verifySelector(
                    "jms",
                    "io.openmessaging.benchmark.driver.jms.JMSBenchmarkDriver",
                    selectorConfig,
                    selector.replace("value: bar", "value: unmatched"));

            Workload transacted = createSimpleWorkload();
            transacted.subscriptionsPerTopic = 2;
            validateResults(
                    runner.run(BrokerFixtures.driverConfig("jms-artemis-transactions.yaml"), transacted),
                    transacted);
        }
    }
}
