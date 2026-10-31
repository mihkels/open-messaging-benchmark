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
package io.openmessaging.benchmark.driver.kop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.openmessaging.benchmark.driver.kop.config.ClientType;
import io.openmessaging.benchmark.driver.kop.config.Config;
import io.openmessaging.benchmark.driver.kop.config.PulsarConfig;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class KopBenchmarkDriverTest {

    @Test
    public void testLoadDefaultConfig() throws URISyntaxException, IOException {
        final URL url = getClass().getClassLoader().getResource("kop_required.yaml");
        assertThat(url).isNotNull();

        final Config config = KopBenchmarkDriver.loadConfig(Path.of(url.toURI()));
        assertThat(config.producerType()).isEqualTo(ClientType.PULSAR);
        assertThat(config.consumerType()).isEqualTo(ClientType.KAFKA);
        assertThat(config.pollTimeoutMs()).isEqualTo(100);

        final PulsarConfig pulsarConfig = config.pulsarConfig();
        assertThat(pulsarConfig.serviceUrl()).isEqualTo("pulsar://localhost:6650");
        assertThat(pulsarConfig.batchingEnabled()).isTrue();
        assertThat(pulsarConfig.batchingMaxPublishDelayMs()).isEqualTo(1);
        assertThat(pulsarConfig.batchingMaxBytes()).isEqualTo(131072);
        assertThat(pulsarConfig.blockIfQueueFull()).isTrue();
        assertThat(pulsarConfig.pendingQueueSize()).isEqualTo(1000);
        assertThat(pulsarConfig.maxPendingMessagesAcrossPartitions()).isEqualTo(50000);
        assertThat(pulsarConfig.maxTotalReceiverQueueSizeAcrossPartitions()).isEqualTo(50000);
        assertThat(pulsarConfig.receiverQueueSize()).isEqualTo(1000);

        assertThat(config.getKafkaProperties().get("bootstrap.servers")).isEqualTo("localhost:9092");
    }

    @Test
    public void testLoadCustomConfig() throws Exception {
        final URL url = this.getClass().getClassLoader().getResource("kop_custom.yaml");
        assertThat(url).isNotNull();

        final Config config = KopBenchmarkDriver.loadConfig(Path.of(url.toURI()));
        assertThat(config.producerType()).isEqualTo(ClientType.KAFKA);
        assertThat(config.consumerType()).isEqualTo(ClientType.PULSAR);
        assertThat(config.pollTimeoutMs()).isEqualTo(1000);

        final PulsarConfig pulsarConfig = config.pulsarConfig();
        assertThat(pulsarConfig.serviceUrl()).isEqualTo("pulsar+ssl://localhost:6651");
        assertThat(pulsarConfig.batchingEnabled()).isFalse();
        assertThat(pulsarConfig.batchingMaxPublishDelayMs()).isEqualTo(10);
        assertThat(pulsarConfig.batchingMaxBytes()).isEqualTo(1310720);
        assertThat(pulsarConfig.blockIfQueueFull()).isFalse();
        assertThat(pulsarConfig.pendingQueueSize()).isEqualTo(10000);
        assertThat(pulsarConfig.maxPendingMessagesAcrossPartitions()).isEqualTo(500000);
        assertThat(pulsarConfig.maxTotalReceiverQueueSizeAcrossPartitions()).isEqualTo(500000);
        assertThat(pulsarConfig.receiverQueueSize()).isEqualTo(10000);

        final Properties props = config.getKafkaProperties();
        assertThat(props).hasSize(3);
        assertThat(props.get("bootstrap.servers")).isEqualTo("localhost:9092");
        assertThat(props.get("linger.ms")).isEqualTo("1");
        assertThat(props.get("batch.size")).isEqualTo("1048576");
    }

    @Test
    public void testLoadWrongKafkaConfig() throws Exception {
        final URL url = this.getClass().getClassLoader().getResource("kop_wrong_kafka_config.yaml");
        assertThat(url).isNotNull();

        final Config config = KopBenchmarkDriver.loadConfig(Path.of(url.toURI()));
        assertThatThrownBy(config::getKafkaProperties)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bootstrap.servers is not set");
    }
}
