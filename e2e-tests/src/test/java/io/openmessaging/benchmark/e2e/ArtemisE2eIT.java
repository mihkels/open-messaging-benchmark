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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

class ArtemisE2eIT extends BaseE2eIT {
    @Test
    @DriverContract("artemis")
    void testMulticastAddressesAndIndependentSubscriptions() throws Exception {
        try (Network network = Network.newNetwork();
                GenericContainer<?> broker = BrokerFixtures.artemis(network);
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            broker.start();
            runner.start();
            Workload workload = createSimpleWorkload();
            workload.topics = 2;
            workload.subscriptionsPerTopic = 2;
            validateResults(runner.run(BrokerFixtures.driverConfig("artemis.yaml"), workload), workload);

            var queues =
                    BrokerFixtures.exec(
                            broker,
                            "/var/lib/artemis-instance/bin/artemis",
                            "queue",
                            "stat",
                            "--url",
                            "tcp://localhost:61616",
                            "--user",
                            "artemis",
                            "--password",
                            "artemis");
            assertThat(queues.getStdout()).contains("test-");
        }
    }
}
