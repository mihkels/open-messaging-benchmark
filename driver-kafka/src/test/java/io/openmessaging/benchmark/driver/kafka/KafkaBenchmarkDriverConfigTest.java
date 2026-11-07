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
package io.openmessaging.benchmark.driver.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import tools.jackson.databind.exc.MismatchedInputException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class KafkaBenchmarkDriverConfigTest {
    @Test
    void initializationMergesConfigOverridesSerializersAndAppliesZoneId(@TempDir Path temp)
            throws Exception {
        Path configuration = temp.resolve("kafka.yaml");
        Files.writeString(
                configuration,
                """
                replicationFactor: 2
                topicConfig: |
                  retention.ms=123
                commonConfig: |
                  bootstrap.servers=localhost:9092
                  client.id=bench-{zone.id}
                producerConfig: |
                  acks=all
                  value.serializer=wrong
                consumerConfig: |
                  enable.auto.commit=false
                  value.deserializer=wrong
                """);
        AdminClient admin = mock(AdminClient.class);
        KafkaBenchmarkDriver driver = new KafkaBenchmarkDriver();
        String oldZone = System.getProperty("zone.id");
        System.setProperty("zone.id", "zone-a");
        try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
            clients.when(
                            () ->
                                    AdminClient.create(
                                            org.mockito.ArgumentMatchers.any(Properties.class)))
                    .thenReturn(admin);
            driver.initialize(configuration, new SimpleMeterRegistry());

            Properties producer = (Properties) field(driver, "producerProperties");
            Properties consumer = (Properties) field(driver, "consumerProperties");
            Properties topic = (Properties) field(driver, "topicProperties");
            assertEquals("bench-zone-a", producer.getProperty("client.id"));
            assertEquals("all", producer.getProperty("acks"));
            assertEquals(
                    StringSerializer.class.getName(),
                    producer.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
            assertEquals(
                    ByteArraySerializer.class.getName(),
                    producer.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
            assertEquals(
                    StringDeserializer.class.getName(),
                    consumer.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
            assertEquals(
                    ByteArrayDeserializer.class.getName(),
                    consumer.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
            assertEquals("123", topic.getProperty("retention.ms"));
            driver.close();
            verify(admin).close();
        } finally {
            if (oldZone == null) {
                System.clearProperty("zone.id");
            } else {
                System.setProperty("zone.id", oldZone);
            }
        }
    }

    @Test
    void malformedPropertiesFailInitializationBeforeCreatingAdmin(@TempDir Path temp) throws Exception {
        Path configuration = temp.resolve("kafka.yaml");
        Files.writeString(
                configuration,
                """
                commonConfig: [unterminated
                """);
        KafkaBenchmarkDriver driver = new KafkaBenchmarkDriver();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        try (MockedStatic<AdminClient> clients = mockStatic(AdminClient.class)) {
            assertThrows(MismatchedInputException.class, () -> driver.initialize(configuration, registry));
            clients.verifyNoInteractions();
        }
    }

    private static Object field(KafkaBenchmarkDriver driver, String name) throws Exception {
        var field = KafkaBenchmarkDriver.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(driver);
    }
}
