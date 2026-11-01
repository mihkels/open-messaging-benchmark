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

class Mqtt5E2eIT extends BaseE2eIT {
    @Test
    @DriverContract("mqtt5")
    void testQosAndSharedSubscriptionFanout() throws Exception {
        try (Network network = Network.newNetwork();
                GenericContainer<?> broker = BrokerFixtures.mqtt(network);
                NetworkBenchmark runner = new NetworkBenchmark(network)) {
            broker.start();
            runner.start();
            String fixture = Files.readString(BrokerFixtures.driverConfig("mqtt5.yaml"));
            assertThat(fixture).contains("qos: 1");
            for (int qos = 0; qos <= 2; qos++) {
                Workload workload = createSimpleWorkload();
                workload.subscriptionsPerTopic = 2;
                workload.consumerPerSubscription = 2;
                String config = fixture.replace("qos: 1", "qos: " + qos);
                // This observes delivery in a healthy local run; QoS 0 has no durability guarantee.
                validateResults(runner.run(config, workload), workload);
            }
        }
    }
}
