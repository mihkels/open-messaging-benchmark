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
package io.openmessaging.benchmark.driver.mqtt5;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PublicationLifecycle;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttBenchmarkProducer implements BenchmarkProducer {

    private static final Logger log = LoggerFactory.getLogger(MqttBenchmarkProducer.class);

    private final String topic;
    private final MqttQos qos;
    private volatile Mqtt5AsyncClient client;
    private final PublicationLifecycle lifecycle = new PublicationLifecycle("MQTT producer");

    public MqttBenchmarkProducer(String topic, int qosCode) {
        this.topic = topic;
        this.qos = MqttBenchmarkDriver.intToQoS(qosCode);
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        Mqtt5Publish message;
        try {
            Mqtt5UserProperties properties =
                    Mqtt5UserProperties.builder()
                            .add(
                                    MqttBenchmarkDriver.USER_PROPERTY_KEY_PUBLISH_TIMESTAMP,
                                    MqttUtf8String.of(String.valueOf(System.currentTimeMillis())))
                            .build();
            message =
                    Mqtt5Publish.builder()
                            .topic(topic)
                            .payload(payload.clone())
                            .qos(qos)
                            .userProperties(properties)
                            .build();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        return lifecycle.publish(settle ->
                this.client
                        .publish(message)
                        .whenComplete((result, ex) -> {
                            try {
                                if (ex != null) {
                                    settle.complete(ex);
                                    log.error("Failed to publish MQTT message, topic={}", topic, ex);
                                } else if (result == null) {
                                    settle.complete(new IllegalStateException("Missing MQTT publish result"));
                                } else if (result.getError().isPresent()) {
                                    Throwable error = result.getError().get();
                                    settle.complete(error);
                                    log.error("Failed to publish MQTT message, topic={}", topic, error);
                                } else {
                                    settle.complete(null);
                                }
                            } catch (Exception failure) {
                                settle.complete(failure);
                            }
                        }));
    }

    @Override
    public void close() throws Exception {
        lifecycle.close(() -> MqttBenchmarkDriver.closeClient(client));
    }

    public void setClient(Mqtt5AsyncClient client) {
        this.client = client;
    }

    public boolean isClosed() {
        return lifecycle.isClosed();
    }
}
