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
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class RabbitMq41E2eIT extends BaseE2eIT {
    @Container
    static RabbitMQContainer rabbitmq =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.1-management"));

    @Test
    @DriverContract("rabbitmq")
    void testRabbitMq41Fanout() throws Exception {
        String config =
                """
                name: RabbitMQ 4.1
                driverClass: io.openmessaging.benchmark.driver.rabbitmq.RabbitMqBenchmarkDriver
                amqpUris:
                  - amqp://%s:%s@%s:%d
                queueType: CLASSIC
                messagePersistence: true
                producerCreationDelay: 100
                producerCreationBatchSize: 5
                consumerCreationDelay: 100
                consumerCreationBatchSize: 5
                """
                        .formatted(
                                rabbitmq.getAdminUsername(),
                                rabbitmq.getAdminPassword(),
                                rabbitmq.getHost(),
                                rabbitmq.getAmqpPort());
        Path driverConfig = tempDir.resolve("rabbitmq-4.1.yaml");
        Files.writeString(driverConfig, config);
        Workload workload = createSimpleWorkload();
        workload.subscriptionsPerTopic = 2;
        validateResults(runBenchmark("RabbitMQ 4.1", driverConfig, workload), workload);
    }
}
