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

class RocketMq5E2eIT extends BaseE2eIT {
    @Test
    @DriverContract("rocketmq5")
    void testGrpcAndDelayedDelivery() throws Exception {
        try (RocketMqFixture fixture = new RocketMqFixture(true);
                NetworkBenchmark runner = new NetworkBenchmark(fixture.network)) {
            fixture.start();
            runner.start();
            String config = Files.readString(BrokerFixtures.driverConfig("rocketmq5.yaml"));
            Workload normal = createSimpleWorkload();
            validateResults(runner.run(config, normal), normal);

            int delaySeconds = 2;
            assertThat(config).contains("sendDelayMsg: false");
            String delayed =
                    config.replace(
                            "sendDelayMsg: false", "sendDelayMsg: true\ndelayTimeInSec: " + delaySeconds);
            Workload workload = createSimpleWorkload();
            SmokeResult result = runner.run(delayed, workload);
            validateResults(result, workload);
            assertThat(result.endToEndLatencyMillis()).isGreaterThanOrEqualTo(1500.0);

            var topics =
                    BrokerFixtures.exec(fixture.broker, "sh", "mqadmin", "topicList", "-n", "namesrv:9876");
            assertThat(topics.getStdout()).contains("Benchmark");
        }
    }
}
