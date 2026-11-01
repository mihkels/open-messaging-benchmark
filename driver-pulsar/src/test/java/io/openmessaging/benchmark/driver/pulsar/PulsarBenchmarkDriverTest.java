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
package io.openmessaging.benchmark.driver.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.pulsar.config.PulsarConfig;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PulsarBenchmarkDriverTest {
    @Test
    void producerCreationUsesIndependentBuilderClones() throws Exception {
        PulsarBenchmarkDriver driver = new PulsarBenchmarkDriver();
        ProducerBuilder<byte[]> template = mock();
        ProducerBuilder<byte[]> firstBuilder = mock(ProducerBuilder.class, RETURNS_SELF);
        ProducerBuilder<byte[]> secondBuilder = mock(ProducerBuilder.class, RETURNS_SELF);
        Producer<byte[]> first = mock();
        Producer<byte[]> second = mock();
        when(template.clone()).thenReturn(firstBuilder, secondBuilder);
        when(firstBuilder.createAsync()).thenReturn(CompletableFuture.completedFuture(first));
        when(secondBuilder.createAsync()).thenReturn(CompletableFuture.completedFuture(second));
        set(driver, "producerBuilder", template);

        BenchmarkProducer firstHandle = driver.createProducer("first").join();
        BenchmarkProducer secondHandle = driver.createProducer("second").join();
        verify(firstBuilder).topic("first");
        verify(secondBuilder).topic("second");
        verify(template, never()).topic(any());
        firstHandle.close();
        secondHandle.close();
        verify(first).close();
        verify(second).close();
    }

    @Test
    void cancellationClosesLateProducerCreation() throws Exception {
        PulsarBenchmarkDriver driver = new PulsarBenchmarkDriver();
        ProducerBuilder<byte[]> template = mock();
        ProducerBuilder<byte[]> builder = mock(ProducerBuilder.class, RETURNS_SELF);
        CompletableFuture<Producer<byte[]>> pending = new CompletableFuture<>();
        Producer<byte[]> producer = mock();
        when(template.clone()).thenReturn(builder);
        when(builder.createAsync()).thenReturn(pending);
        set(driver, "producerBuilder", template);

        CompletableFuture<BenchmarkProducer> result = driver.createProducer("topic");
        assertThat(result.cancel(true)).isTrue();
        assertThat(pending.complete(producer)).isTrue();
        verify(producer, times(1)).close();
    }

    @Test
    void consumerFailureClosesPartialAndLatePartitionConsumers() throws Exception {
        PulsarBenchmarkDriver driver = new PulsarBenchmarkDriver();
        PulsarClient client = mock();
        ConsumerBuilder<ByteBuffer> firstBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);
        ConsumerBuilder<ByteBuffer> secondBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);
        ConsumerBuilder<ByteBuffer> thirdBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);
        CompletableFuture<Consumer<ByteBuffer>> first = new CompletableFuture<>();
        CompletableFuture<Consumer<ByteBuffer>> second = new CompletableFuture<>();
        CompletableFuture<Consumer<ByteBuffer>> third = new CompletableFuture<>();
        Consumer<ByteBuffer> partial = mock();
        Consumer<ByteBuffer> late = mock();
        when(client.getPartitionsForTopic("topic", true))
                .thenReturn(CompletableFuture.completedFuture(List.of("p0", "p1", "p2")));
        when(client.newConsumer(Schema.BYTEBUFFER))
                .thenReturn(firstBuilder, secondBuilder, thirdBuilder);
        when(firstBuilder.subscribeAsync()).thenReturn(first);
        when(secondBuilder.subscribeAsync()).thenReturn(second);
        when(thirdBuilder.subscribeAsync()).thenReturn(third);
        set(driver, "client", client);
        set(driver, "config", new PulsarConfig());

        CompletableFuture<BenchmarkConsumer> result =
                driver.createConsumer("topic", "subscription", mock(ConsumerCallback.class));
        first.complete(partial);
        RuntimeException failure = new RuntimeException("subscription failed");
        second.completeExceptionally(failure);
        assertThatThrownBy(result::join).hasCause(failure);
        verify(partial, times(1)).close();
        third.complete(late);
        verify(late, times(1)).close();
        verify(firstBuilder).topic("p0");
        verify(secondBuilder).topic("p1");
        verify(thirdBuilder).topic("p2");
    }

    @Test
    void cancelledConsumerDiscoveryDoesNotStartSubscriptions() throws Exception {
        PulsarBenchmarkDriver driver = new PulsarBenchmarkDriver();
        PulsarClient client = mock();
        CompletableFuture<List<String>> discovery = new CompletableFuture<>();
        when(client.getPartitionsForTopic("topic", true)).thenReturn(discovery);
        set(driver, "client", client);
        set(driver, "config", new PulsarConfig());

        CompletableFuture<BenchmarkConsumer> result =
                driver.createConsumer("topic", "subscription", mock(ConsumerCallback.class));
        assertThat(result.cancel(true)).isTrue();
        discovery.complete(List.of("p0"));
        verify(client, never()).newConsumer(any());
    }

    @Test
    void emptyPartitionDiscoveryDoesNotReturnAReadyConsumer() throws Exception {
        PulsarBenchmarkDriver driver = new PulsarBenchmarkDriver();
        PulsarClient client = mock();
        when(client.getPartitionsForTopic("topic", true))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        set(driver, "client", client);
        set(driver, "config", new PulsarConfig());

        CompletableFuture<BenchmarkConsumer> result =
                driver.createConsumer("topic", "subscription", mock(ConsumerCallback.class));
        assertThatThrownBy(result::join)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one partition");
    }

    @Test
    void cancellationClosesPartialAndLatePartitionConsumers() throws Exception {
        PulsarBenchmarkDriver driver = new PulsarBenchmarkDriver();
        PulsarClient client = mock();
        ConsumerBuilder<ByteBuffer> firstBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);
        ConsumerBuilder<ByteBuffer> secondBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);
        CompletableFuture<Consumer<ByteBuffer>> first = new CompletableFuture<>();
        CompletableFuture<Consumer<ByteBuffer>> second = new CompletableFuture<>();
        Consumer<ByteBuffer> partial = mock();
        Consumer<ByteBuffer> late = mock();
        when(client.getPartitionsForTopic("topic", true))
                .thenReturn(CompletableFuture.completedFuture(List.of("p0", "p1")));
        when(client.newConsumer(Schema.BYTEBUFFER)).thenReturn(firstBuilder, secondBuilder);
        when(firstBuilder.subscribeAsync()).thenReturn(first);
        when(secondBuilder.subscribeAsync()).thenReturn(second);
        set(driver, "client", client);
        set(driver, "config", new PulsarConfig());

        CompletableFuture<BenchmarkConsumer> result =
                driver.createConsumer("topic", "subscription", mock(ConsumerCallback.class));
        first.complete(partial);
        assertThat(result.cancel(true)).isTrue();
        verify(partial, times(1)).close();
        second.complete(late);
        verify(late, times(1)).close();
    }

    @Test
    void consumerCloseAttemptsEveryPartition() throws Exception {
        Consumer<ByteBuffer> first = mock();
        Consumer<ByteBuffer> second = mock();
        RuntimeException firstFailure = new RuntimeException("first close");
        RuntimeException secondFailure = new RuntimeException("second close");
        doThrow(firstFailure).when(first).close();
        doThrow(secondFailure).when(second).close();
        PulsarBenchmarkConsumer consumer = new PulsarBenchmarkConsumer(List.of(first, second));

        assertThatThrownBy(consumer::close).isSameAs(firstFailure);
        assertThat(firstFailure.getSuppressed()).containsExactly(secondFailure);
        verify(first).close();
        verify(second).close();
    }

    @Test
    void failedInitializationClosesAllCreatedClients(@TempDir Path temp) throws Exception {
        Path configuration = temp.resolve("pulsar.yaml");
        Files.writeString(
                configuration,
                "client:\n  serviceUrl: pulsar://localhost:6650\n  httpUrl: http://localhost:8080\n");
        ClientBuilder clientBuilder = mock(ClientBuilder.class, RETURNS_SELF);
        PulsarAdminBuilder adminBuilder = mock(PulsarAdminBuilder.class, RETURNS_SELF);
        PulsarClient client = mock();
        PulsarAdmin admin = mock();
        RuntimeException initializationFailure = new RuntimeException("producer builder failed");
        RuntimeException clientCloseFailure = new RuntimeException("client close failed");
        RuntimeException adminCloseFailure = new RuntimeException("admin close failed");
        when(clientBuilder.build()).thenReturn(client);
        when(adminBuilder.build()).thenReturn(admin);
        when(client.newProducer()).thenThrow(initializationFailure);
        doThrow(clientCloseFailure).when(client).close();
        doThrow(adminCloseFailure).when(admin).close();
        PulsarBenchmarkDriver driver = new PulsarBenchmarkDriver();

        try (var clients = mockStatic(PulsarClient.class);
                var admins = mockStatic(PulsarAdmin.class)) {
            clients.when(PulsarClient::builder).thenReturn(clientBuilder);
            admins.when(PulsarAdmin::builder).thenReturn(adminBuilder);
            assertThatThrownBy(() -> driver.initialize(configuration, mock(MeterRegistry.class)))
                    .isSameAs(initializationFailure);
        }
        assertThat(initializationFailure.getSuppressed())
                .containsExactly(clientCloseFailure, adminCloseFailure);
        verify(client).close();
        verify(admin).close();
        driver.close();
        verify(client, times(1)).close();
        verify(admin, times(1)).close();
    }

    @Test
    void driverCloseAttemptsBothClientsAndPreservesFailures() throws Exception {
        PulsarBenchmarkDriver driver = new PulsarBenchmarkDriver();
        PulsarClient client = mock();
        PulsarAdmin admin = mock();
        RuntimeException clientFailure = new RuntimeException("client close failed");
        RuntimeException adminFailure = new RuntimeException("admin close failed");
        doThrow(clientFailure).when(client).close();
        doThrow(adminFailure).when(admin).close();
        set(driver, "client", client);
        set(driver, "adminClient", admin);

        assertThatThrownBy(driver::close).isSameAs(clientFailure);
        assertThat(clientFailure.getSuppressed()).containsExactly(adminFailure);
        driver.close();
        verify(client, times(1)).close();
        verify(admin, times(1)).close();
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
