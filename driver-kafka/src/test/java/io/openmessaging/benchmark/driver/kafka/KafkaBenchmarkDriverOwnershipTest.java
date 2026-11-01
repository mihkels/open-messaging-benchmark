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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

class KafkaBenchmarkDriverOwnershipTest {
    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void driverClosesOnlySharedAdminAndCallerClosesHandedOffHandles() throws Exception {
        KafkaBenchmarkDriver driver = new KafkaBenchmarkDriver();
        AdminClient admin = mock(AdminClient.class);
        set(driver, "admin", admin);
        set(driver, "producerProperties", new Properties());
        set(driver, "consumerProperties", new Properties());
        PartitionInfo partition = new PartitionInfo("topic", 0, null, new Node[0], new Node[0]);

        try (MockedConstruction<KafkaProducer> producers = mockConstruction(KafkaProducer.class);
                MockedConstruction<KafkaConsumer> consumers =
                        mockConstruction(
                                KafkaConsumer.class,
                                (consumer, context) -> {
                                    doAnswer(
                                                    invocation -> {
                                                        ConsumerRebalanceListener listener = invocation.getArgument(1);
                                                        listener.onPartitionsAssigned(List.of());
                                                        return null;
                                                    })
                                            .when(consumer)
                                            .subscribe(anyCollection(), any(ConsumerRebalanceListener.class));
                                    when(consumer.poll(any(Duration.class)))
                                            .thenAnswer(
                                                    ignored -> {
                                                        Thread.sleep(2);
                                                        return ConsumerRecords.empty();
                                                    });
                                    when(consumer.partitionsFor(eq("topic"), eq(Duration.ZERO)))
                                            .thenReturn(List.of(partition));
                                })) {
            BenchmarkProducer producer = driver.createProducer("topic").join();
            BenchmarkConsumer consumer = driver.createConsumer("topic", "group", null).join();
            KafkaProducer kafkaProducer = producers.constructed().get(0);
            KafkaConsumer kafkaConsumer = consumers.constructed().get(0);
            try {
                driver.close();
                driver.close();
                verify(admin, times(1)).close();
                verify(kafkaProducer, never()).close();
                verify(kafkaConsumer, never()).close();
            } finally {
                producer.close();
                consumer.close();
            }
            verify(kafkaProducer, times(1)).close();
            verify(kafkaConsumer, times(1)).close();
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void failedSubscriptionClosesPartialConsumerAndPreservesCause() throws Exception {
        KafkaBenchmarkDriver driver = new KafkaBenchmarkDriver();
        set(driver, "consumerProperties", new Properties());
        IllegalStateException cause = new IllegalStateException("subscription failed");
        try (MockedConstruction<KafkaConsumer> consumers =
                mockConstruction(
                        KafkaConsumer.class,
                        (consumer, context) ->
                                doThrow(cause)
                                        .when(consumer)
                                        .subscribe(anyCollection(), any(ConsumerRebalanceListener.class)))) {
            assertSame(
                    cause,
                    assertThrows(
                                    CompletionException.class,
                                    () -> driver.createConsumer("topic", "group", null).join())
                            .getCause());
            verify(consumers.constructed().get(0), times(1)).close();
        }
        driver.close();
    }

    private static void set(Object instance, String name, Object value) throws Exception {
        Field field = instance.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(instance, value);
    }
}
