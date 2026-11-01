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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.Test;

class PulsarBenchmarkProducerTest {

    @Test
    void canceledSendDoesNotRetainCallerPayload() {
        Producer<byte[]> client = mock();
        TypedMessageBuilder<byte[]> builder = mock();
        AtomicReference<byte[]> sent = new AtomicReference<>();
        CompletableFuture<MessageId> acknowledgement = new CompletableFuture<>();
        when(client.newMessage()).thenReturn(builder);
        when(builder.value(any(byte[].class)))
                .thenAnswer(
                        invocation -> {
                            sent.set(invocation.getArgument(0));
                            return builder;
                        });
        when(builder.sendAsync()).thenReturn(acknowledgement);
        PulsarBenchmarkProducer producer = new PulsarBenchmarkProducer(client);
        byte[] payload = {1, 2, 3};

        CompletableFuture<Void> send = producer.sendAsync(Optional.of("key"), payload);
        assertThat(send.cancel(false)).isTrue();
        Arrays.fill(payload, (byte) 0);

        assertThat(sent.get()).isNotSameAs(payload).containsExactly(1, 2, 3);
        verify(builder).key("key");
        acknowledgement.complete(null);
        assertThat(send).isCancelled();
    }

    @Test
    void synchronousClientFailureCompletesExceptionally() {
        Producer<byte[]> client = mock();
        TypedMessageBuilder<byte[]> builder = mock();
        IllegalStateException cause = new IllegalStateException("producer closed");
        when(client.newMessage()).thenReturn(builder);
        when(builder.value(any(byte[].class))).thenReturn(builder);
        when(builder.sendAsync()).thenThrow(cause);
        PulsarBenchmarkProducer producer = new PulsarBenchmarkProducer(client);

        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(cause);
    }

    @Test
    void closeFailsPendingSendsAndIgnoresLateAcknowledgements() throws Exception {
        Producer<byte[]> client = mock();
        TypedMessageBuilder<byte[]> builder = mock();
        CompletableFuture<MessageId> acknowledgement = new CompletableFuture<>();
        when(client.newMessage()).thenReturn(builder);
        when(builder.value(any(byte[].class))).thenReturn(builder);
        when(builder.sendAsync()).thenReturn(acknowledgement);
        PulsarBenchmarkProducer producer = new PulsarBenchmarkProducer(client);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        producer.close();
        acknowledgement.complete(null);
        producer.close();

        assertThat(send).isCompletedExceptionally();
        assertThat(producer.sendAsync(Optional.empty(), new byte[] {2})).isCompletedExceptionally();
        verify(client, times(1)).newMessage();
        verify(client, times(1)).close();
    }

    @Test
    void preservesAcknowledgementDeliveredDuringClose() throws Exception {
        Producer<byte[]> client = mock();
        TypedMessageBuilder<byte[]> builder = mock();
        CompletableFuture<MessageId> acknowledgement = new CompletableFuture<>();
        when(client.newMessage()).thenReturn(builder);
        when(builder.value(any(byte[].class))).thenReturn(builder);
        when(builder.sendAsync()).thenReturn(acknowledgement);
        doAnswer(
                        invocation -> {
                            acknowledgement.complete(null);
                            return null;
                        })
                .when(client)
                .close();
        PulsarBenchmarkProducer producer = new PulsarBenchmarkProducer(client);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        producer.close();

        assertThat(send).isCompletedWithValue(null);
    }

    @Test
    void closeFailureStillSettlesPendingSends() throws Exception {
        Producer<byte[]> client = mock();
        TypedMessageBuilder<byte[]> builder = mock();
        when(client.newMessage()).thenReturn(builder);
        when(builder.value(any(byte[].class))).thenReturn(builder);
        when(builder.sendAsync()).thenReturn(new CompletableFuture<>());
        PulsarClientException failure = new PulsarClientException("close failed");
        doThrow(failure).when(client).close();
        PulsarBenchmarkProducer producer = new PulsarBenchmarkProducer(client);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertThatThrownBy(producer::close).isSameAs(failure);
        assertThatThrownBy(send::join).hasCauseInstanceOf(IllegalStateException.class);
        assertThatThrownBy(send::join).hasRootCause(failure);
    }

    @Test
    void closeFromClientCallbackCanBeRetriedLater() throws Exception {
        Producer<byte[]> client = mock();
        TypedMessageBuilder<byte[]> builder = mock();
        CompletableFuture<MessageId> acknowledgement = new CompletableFuture<>();
        when(client.newMessage()).thenReturn(builder);
        when(builder.value(any(byte[].class))).thenReturn(builder);
        when(builder.sendAsync()).thenReturn(acknowledgement);
        PulsarBenchmarkProducer producer = new PulsarBenchmarkProducer(client);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> callback =
                send.thenRun(
                        () -> assertThatThrownBy(producer::close).isInstanceOf(IllegalStateException.class));

        acknowledgement.complete(null);
        callback.join();
        verify(client, never()).close();
        producer.close();
        verify(client).close();
    }

    @Test
    void closeWaitsForAnInProgressSendBeforeClosingClient() throws Exception {
        Producer<byte[]> client = mock();
        TypedMessageBuilder<byte[]> builder = mock();
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(client.newMessage()).thenReturn(builder);
        when(builder.value(any(byte[].class))).thenReturn(builder);
        when(builder.sendAsync())
                .thenAnswer(
                        invocation -> {
                            publishing.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("send was not released");
                            }
                            return new CompletableFuture<MessageId>();
                        });
        PulsarBenchmarkProducer producer = new PulsarBenchmarkProducer(client);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var sending = executor.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {1}));
            assertThat(publishing.await(5, TimeUnit.SECONDS)).isTrue();
            var closing =
                    executor.submit(
                            () -> {
                                producer.close();
                                return null;
                            });
            assertThatThrownBy(() -> closing.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            verify(client, never()).close();
            release.countDown();
            CompletableFuture<Void> send = sending.get(5, TimeUnit.SECONDS);
            closing.get(5, TimeUnit.SECONDS);
            assertThat(send).isCompletedExceptionally();
            verify(client).close();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
