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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.kop.config.Config;
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
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

class KopBenchmarkDriverOwnershipTest {
    @Test
    void kafkaHandlesBelongToTheirCaller() throws Exception {
        KopBenchmarkDriver driver = kafkaDriver();
        PartitionInfo partition = new PartitionInfo("topic", 0, null, new Node[0], new Node[0]);
        try (MockedConstruction<KafkaProducer> producers = mockConstruction(KafkaProducer.class);
                MockedConstruction<KafkaConsumer> consumers =
                        mockConstruction(
                                KafkaConsumer.class,
                                (mock, context) -> {
                                    doAnswer(
                                                    invocation -> {
                                                        ConsumerRebalanceListener listener = invocation.getArgument(1);
                                                        listener.onPartitionsAssigned(List.of());
                                                        return null;
                                                    })
                                            .when(mock)
                                            .subscribe(anyCollection(), any(ConsumerRebalanceListener.class));
                                    when(mock.poll(any(Duration.class)))
                                            .thenAnswer(
                                                    invocation -> {
                                                        Thread.sleep(1);
                                                        return ConsumerRecords.empty();
                                                    });
                                    when(mock.partitionsFor(eq("topic"), eq(Duration.ZERO)))
                                            .thenReturn(List.of(partition));
                                })) {
            BenchmarkProducer producer = driver.createProducer("topic").join();
            BenchmarkConsumer consumer =
                    driver.createConsumer("topic", "subscription", mock(ConsumerCallback.class)).join();
            try {
                driver.close();
                verify(producers.constructed().get(0), never()).close();
                verify(consumers.constructed().get(0), never()).close();
            } finally {
                producer.close();
                consumer.close();
            }
            verify(producers.constructed().get(0), times(1)).close();
            verify(consumers.constructed().get(0), times(1)).close();
        }
    }

    @Test
    void failedKafkaSubscriptionClosesPartialConsumerAndPreservesCause() throws Exception {
        KopBenchmarkDriver driver = kafkaDriver();
        RuntimeException failure = new RuntimeException("subscription failed");
        RuntimeException cleanup = new RuntimeException("close failed");
        try (MockedConstruction<KafkaConsumer> consumers =
                mockConstruction(
                        KafkaConsumer.class,
                        (mock, context) -> {
                            doThrow(failure)
                                    .when(mock)
                                    .subscribe(anyCollection(), any(ConsumerRebalanceListener.class));
                            doThrow(cleanup).when(mock).close();
                        })) {
            CompletionException result =
                    assertThrows(
                            CompletionException.class,
                            () ->
                                    driver
                                            .createConsumer("topic", "subscription", mock(ConsumerCallback.class))
                                            .join());
            assertSame(failure, result.getCause());
            assertArrayEquals(new Throwable[] {cleanup}, failure.getSuppressed());
            verify(consumers.constructed().get(0), times(1)).close();
        }
    }

    @Test
    void driverClosesBothSharedClientsAndPreservesFailures() throws Exception {
        KopBenchmarkDriver driver = new KopBenchmarkDriver();
        AdminClient admin = mock(AdminClient.class);
        PulsarClient client = mock(PulsarClient.class);
        RuntimeException adminFailure = new RuntimeException("admin close failed");
        RuntimeException clientFailure = new RuntimeException("client close failed");
        doThrow(adminFailure).when(admin).close();
        doThrow(clientFailure).when(client).close();
        setField(driver, "admin", admin);
        setField(driver, "client", client);

        assertSame(adminFailure, assertThrows(RuntimeException.class, driver::close));
        assertArrayEquals(new Throwable[] {clientFailure}, adminFailure.getSuppressed());
        driver.close();
        verify(admin, times(1)).close();
        verify(client, times(1)).close();
    }

    private static KopBenchmarkDriver kafkaDriver() throws Exception {
        KopBenchmarkDriver driver = new KopBenchmarkDriver();
        setField(driver, "config", new Config());
        setField(driver, "producerProperties", new Properties());
        setField(driver, "consumerProperties", new Properties());
        return driver;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
