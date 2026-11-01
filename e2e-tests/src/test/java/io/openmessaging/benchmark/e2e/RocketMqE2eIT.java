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
import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.databind.ObjectMapper;

class RocketMqE2eIT extends BaseE2eIT {
    @Test
    @DriverContract("rocketmq")
    void testRemotingQueuesAndConsumerGroups() throws Exception {
        try (RocketMqFixture fixture = new RocketMqFixture(false);
                NetworkBenchmark runner = new NetworkBenchmark(fixture.network)) {
            fixture.start();
            runner.start();
            Workload workload = createSimpleWorkload();
            workload.partitionsPerTopic = 4;
            workload.subscriptionsPerTopic = 2;
            workload.consumerPerSubscription = 2;
            workload.keyDistributor = KeyDistributorType.KEY_ROUND_ROBIN;
            validateResults(runner.run(BrokerFixtures.driverConfig("rocketmq.yaml"), workload), workload);

            var topics =
                    BrokerFixtures.exec(fixture.broker, "sh", "mqadmin", "topicList", "-n", "namesrv:9876");
            String topic =
                    topics
                            .getStdout()
                            .lines()
                            .map(String::trim)
                            .filter(name -> name.startsWith("RocketMQ-Benchmark-"))
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("RocketMQ topic was not created"));
            var route =
                    BrokerFixtures.exec(
                            fixture.broker, "sh", "mqadmin", "topicRoute", "-n", "namesrv:9876", "-t", topic);
            var mapper =
                    new ObjectMapper(
                            JsonFactory.builder().enable(JsonReadFeature.ALLOW_UNQUOTED_PROPERTY_NAMES).build());
            var queues = mapper.readTree(route.getStdout()).get("queueDatas");
            assertThat(queues).isNotNull();
            assertThat(queues.isArray()).isTrue();
            assertThat(queues.size()).isEqualTo(1);
            queues.forEach(
                    queue -> {
                        assertThat(queue.get("readQueueNums").asInt()).isEqualTo(workload.partitionsPerTopic);
                        assertThat(queue.get("writeQueueNums").asInt()).isEqualTo(workload.partitionsPerTopic);
                    });
        }
    }
}
