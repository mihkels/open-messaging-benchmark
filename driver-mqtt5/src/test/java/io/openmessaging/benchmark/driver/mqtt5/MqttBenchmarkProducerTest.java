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

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConfig;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MqttBenchmarkProducerTest {
    @Test
    void cancellationOwnsThePayloadAndCloseFailsOutstandingSends() throws Exception {
        Mqtt5AsyncClient client = client();
        CompletableFuture<Mqtt5PublishResult> firstResult = new CompletableFuture<>();
        CompletableFuture<Mqtt5PublishResult> secondResult = new CompletableFuture<>();
        when(client.publish(any(Mqtt5Publish.class))).thenReturn(firstResult).thenReturn(secondResult);
        MqttBenchmarkProducer producer = new MqttBenchmarkProducer("topic", 1);
        producer.setClient(client);
        byte[] payload = {1, 2, 3};
        CompletableFuture<Void> canceled = producer.sendAsync(Optional.empty(), payload);
        ArgumentCaptor<Mqtt5Publish> publish = ArgumentCaptor.forClass(Mqtt5Publish.class);
        verify(client).publish(publish.capture());
        assertThat(canceled.cancel(false)).isTrue();
        Arrays.fill(payload, (byte) 9);
        assertThat(publish.getValue().getPayloadAsBytes()).containsExactly(1, 2, 3);

        CompletableFuture<Void> pending = producer.sendAsync(Optional.empty(), new byte[] {4});
        producer.close();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IllegalStateException.class);
        Mqtt5PublishResult result = mock(Mqtt5PublishResult.class);
        when(result.getError()).thenReturn(Optional.empty());
        secondResult.complete(result);
        assertThat(pending.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {5}).join())
                .hasCauseInstanceOf(IllegalStateException.class);
        producer.close();
        verify(client, times(1)).disconnect();
    }

    @Test
    void synchronousAndBrokerRejectionsCompleteExceptionally() throws Exception {
        Mqtt5AsyncClient client = client();
        RuntimeException rejection = new RuntimeException("client rejected");
        when(client.publish(any(Mqtt5Publish.class))).thenThrow(rejection);
        MqttBenchmarkProducer producer = new MqttBenchmarkProducer("topic", 1);
        producer.setClient(client);
        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(rejection);

        Mqtt5AsyncClient otherClient = client();
        CompletableFuture<Mqtt5PublishResult> result = new CompletableFuture<>();
        when(otherClient.publish(any(Mqtt5Publish.class))).thenReturn(result);
        MqttBenchmarkProducer other = new MqttBenchmarkProducer("topic", 1);
        other.setClient(otherClient);
        CompletableFuture<Void> pending = other.sendAsync(Optional.empty(), new byte[] {1});
        RuntimeException brokerRejection = new RuntimeException("broker rejected");
        Mqtt5PublishResult rejected = mock(Mqtt5PublishResult.class);
        when(rejected.getError()).thenReturn(Optional.of(brokerRejection));
        result.complete(rejected);
        assertThatThrownBy(pending::join).hasCause(brokerRejection);
        producer.close();
        other.close();
    }

    @Test
    void disconnectFailureDoesNotLeaveSendsUnresolved() {
        Mqtt5AsyncClient client = client();
        when(client.publish(any(Mqtt5Publish.class))).thenReturn(new CompletableFuture<>());
        RuntimeException failure = new RuntimeException("disconnect failed");
        doThrow(failure).when(client).disconnect();
        MqttBenchmarkProducer producer = new MqttBenchmarkProducer("topic", 1);
        producer.setClient(client);
        CompletableFuture<Void> pending = producer.sendAsync(Optional.empty(), new byte[] {1});
        assertThatThrownBy(producer::close).isSameAs(failure);
        assertThatThrownBy(pending::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCause(failure);
    }

    @Test
    void reentrantCloseIsRejectedUntilNestedPublicationsReturn() throws Exception {
        Mqtt5AsyncClient client = client();
        CompletableFuture<Mqtt5PublishResult> firstResult = new CompletableFuture<>();
        CompletableFuture<Mqtt5PublishResult> secondResult = new CompletableFuture<>();
        CompletableFuture<Mqtt5PublishResult> nestedResult = new CompletableFuture<>();
        Mqtt5PublishResult accepted = mock(Mqtt5PublishResult.class);
        when(accepted.getError()).thenReturn(Optional.empty());
        AtomicInteger calls = new AtomicInteger();
        when(client.publish(any(Mqtt5Publish.class)))
                .thenAnswer(
                        invocation -> {
                            int call = calls.incrementAndGet();
                            if (call == 1) {
                                return firstResult;
                            }
                            if (call == 2) {
                                firstResult.complete(accepted);
                                return secondResult;
                            }
                            return nestedResult;
                        });
        MqttBenchmarkProducer producer = new MqttBenchmarkProducer("topic", 1);
        producer.setClient(client);
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        AtomicReference<CompletableFuture<Void>> nested = new AtomicReference<>();
        CompletableFuture<Void> closeAttempt =
                first.thenRun(
                        () -> {
                            nested.set(producer.sendAsync(Optional.empty(), new byte[] {3}));
                            try {
                                producer.close();
                            } catch (Exception failure) {
                                throw new CompletionException(failure);
                            }
                        });
        ExecutorService sender =
                Executors.newSingleThreadExecutor(
                        task -> {
                            Thread thread = new Thread(task, "mqtt-reentrant-test");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<CompletableFuture<Void>> second =
                    sender.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {2}));
            CompletableFuture<Void> secondSend = second.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(closeAttempt::join)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("retry after sendAsync returns");
            assertThat(calls).hasValue(3);
            assertThat(secondSend.isDone()).isFalse();
            assertThat(nested.get().isDone()).isFalse();
            verify(client, never()).disconnect();
            producer.close();
            assertThatThrownBy(secondSend::join).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(nested.get()::join).hasCauseInstanceOf(IllegalStateException.class);
            verify(client, times(1)).disconnect();
        } finally {
            sender.shutdownNow();
        }
    }

    @Test
    void asynchronousAcknowledgementCannotCloseWhileAnotherPublicationIsActive() throws Exception {
        Mqtt5AsyncClient client = client();
        CompletableFuture<Mqtt5PublishResult> firstResult = new CompletableFuture<>();
        CompletableFuture<Mqtt5PublishResult> secondResult = new CompletableFuture<>();
        CompletableFuture<Mqtt5PublishResult> thirdResult = new CompletableFuture<>();
        Mqtt5PublishResult accepted = mock(Mqtt5PublishResult.class);
        when(accepted.getError()).thenReturn(Optional.empty());
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        when(client.publish(any(Mqtt5Publish.class)))
                .thenAnswer(
                        invocation -> {
                            int call = calls.incrementAndGet();
                            if (call == 1) {
                                return firstResult;
                            }
                            if (call == 2) {
                                publishing.countDown();
                                if (!release.await(5, TimeUnit.SECONDS)) {
                                    throw new AssertionError("publication was not released");
                                }
                                return secondResult;
                            }
                            return thirdResult;
                        });
        MqttBenchmarkProducer producer = new MqttBenchmarkProducer("topic", 1);
        producer.setClient(client);
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        AtomicReference<Throwable> rejected = new AtomicReference<>();
        first.thenRun(
                () -> {
                    try {
                        producer.close();
                    } catch (Exception failure) {
                        rejected.set(failure);
                    }
                });
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<CompletableFuture<Void>> second =
                    workers.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {2}));
            assertThat(publishing.await(2, TimeUnit.SECONDS)).isTrue();
            Future<?> ack = workers.submit(() -> firstResult.complete(accepted));
            ack.get(2, TimeUnit.SECONDS);
            assertThat(rejected.get())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("client completion callback");
            assertThat(producer.isClosed()).isFalse();
            verify(client, never()).disconnect();
            release.countDown();
            CompletableFuture<Void> secondSend = second.get(2, TimeUnit.SECONDS);
            CompletableFuture<Void> third = producer.sendAsync(Optional.empty(), new byte[] {3});
            assertThat(third.isDone()).isFalse();
            producer.close();
            assertThatThrownBy(secondSend::join).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(third::join).hasCauseInstanceOf(IllegalStateException.class);
            verify(client, times(1)).disconnect();
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void closeFailureCallbacksRejectReentrantCloseAndRetainTheClientFailure() {
        Mqtt5AsyncClient client = client();
        when(client.publish(any(Mqtt5Publish.class))).thenReturn(new CompletableFuture<>());
        RuntimeException failure = new RuntimeException("disconnect failed");
        doThrow(failure).when(client).disconnect();
        MqttBenchmarkProducer producer = new MqttBenchmarkProducer("topic", 1);
        producer.setClient(client);
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        AtomicReference<Throwable> rejected = new AtomicReference<>();
        first.whenComplete(
                (unused, error) -> {
                    try {
                        producer.close();
                    } catch (Exception closeFailure) {
                        rejected.set(closeFailure);
                    }
                });

        assertThatThrownBy(producer::close).isSameAs(failure);

        assertThat(rejected.get())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("client completion callback");
        assertThatThrownBy(first::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCause(failure);
        assertThatThrownBy(second::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCause(failure);
        verify(client, times(1)).disconnect();
    }

    @Test
    void failedAcknowledgementAlsoGuardsReentrantClose() throws Exception {
        Mqtt5AsyncClient client = client();
        CompletableFuture<Mqtt5PublishResult> result = new CompletableFuture<>();
        when(client.publish(any(Mqtt5Publish.class))).thenReturn(result);
        MqttBenchmarkProducer producer = new MqttBenchmarkProducer("topic", 1);
        producer.setClient(client);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Throwable> rejected =
                send.handle(
                        (unused, error) -> {
                            try {
                                producer.close();
                                return null;
                            } catch (Exception failure) {
                                return failure;
                            }
                        });
        RuntimeException failure = new RuntimeException("acknowledgement failed");
        result.completeExceptionally(failure);
        assertThat(rejected.join()).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(send::join).hasCause(failure);
        assertThat(producer.isClosed()).isFalse();
        producer.close();
    }

    @Test
    void closeWaitsForAnActivePublicationBeforeDisconnecting() throws Exception {
        Mqtt5AsyncClient client = client();
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        when(client.publish(any(Mqtt5Publish.class)))
                .thenAnswer(
                        invocation -> {
                            publishing.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("publication was not released");
                            }
                            return new CompletableFuture<Mqtt5PublishResult>();
                        });
        MqttBenchmarkProducer producer = new MqttBenchmarkProducer("topic", 1);
        producer.setClient(client);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<CompletableFuture<Void>> send =
                    workers.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {1}));
            assertThat(publishing.await(2, TimeUnit.SECONDS)).isTrue();
            Future<?> close =
                    workers.submit(
                            () -> {
                                closing.countDown();
                                try {
                                    producer.close();
                                } catch (Exception failure) {
                                    throw new CompletionException(failure);
                                }
                            });
            assertThat(closing.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> close.get(150, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            verify(client, never()).disconnect();
            release.countDown();
            CompletableFuture<Void> pending = send.get(2, TimeUnit.SECONDS);
            close.get(2, TimeUnit.SECONDS);
            assertThatThrownBy(pending::join).hasCauseInstanceOf(IllegalStateException.class);
            verify(client, times(1)).disconnect();
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void concurrentCloseWaitsForDisconnectAndKeepsItsAsynchronousFailure() throws Exception {
        Mqtt5AsyncClient client = client();
        when(client.publish(any(Mqtt5Publish.class))).thenReturn(new CompletableFuture<>());
        CompletableFuture<Void> disconnect = new CompletableFuture<>();
        CountDownLatch disconnecting = new CountDownLatch(1);
        CountDownLatch secondClosing = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            disconnecting.countDown();
                            return disconnect;
                        })
                .when(client)
                .disconnect();
        MqttBenchmarkProducer producer = new MqttBenchmarkProducer("topic", 1);
        producer.setClient(client);
        CompletableFuture<Void> pending = producer.sendAsync(Optional.empty(), new byte[] {1});
        RuntimeException failure = new RuntimeException("asynchronous disconnect failed");
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<Throwable> first =
                    workers.submit(
                            () -> {
                                try {
                                    producer.close();
                                    return null;
                                } catch (Exception error) {
                                    return error;
                                }
                            });
            assertThat(disconnecting.await(2, TimeUnit.SECONDS)).isTrue();
            Future<?> second =
                    workers.submit(
                            () -> {
                                secondClosing.countDown();
                                producer.close();
                                return null;
                            });
            assertThat(secondClosing.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> second.get(150, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(pending.isDone()).isFalse();
            disconnect.completeExceptionally(failure);
            assertThat(first.get(2, TimeUnit.SECONDS)).isSameAs(failure);
            second.get(2, TimeUnit.SECONDS);
            assertThatThrownBy(pending::join)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCause(failure);
            verify(client, times(1)).disconnect();
        } finally {
            disconnect.completeExceptionally(failure);
            workers.shutdownNow();
        }
    }

    @Test
    void acknowledgedSendStaysSuccessfulWhenDisconnectThenFails() {
        Mqtt5AsyncClient client = client();
        CompletableFuture<Mqtt5PublishResult> result = new CompletableFuture<>();
        Mqtt5PublishResult accepted = mock(Mqtt5PublishResult.class);
        when(accepted.getError()).thenReturn(Optional.empty());
        when(client.publish(any(Mqtt5Publish.class))).thenReturn(result);
        RuntimeException failure = new RuntimeException("disconnect failed");
        doAnswer(
                        invocation -> {
                            result.complete(accepted);
                            return CompletableFuture.failedFuture(failure);
                        })
                .when(client)
                .disconnect();
        MqttBenchmarkProducer producer = new MqttBenchmarkProducer("topic", 1);
        producer.setClient(client);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertThatThrownBy(producer::close).isSameAs(failure);
        assertThat(send.join()).isNull();
    }

    private static Mqtt5AsyncClient client() {
        Mqtt5AsyncClient client = mock(Mqtt5AsyncClient.class);
        when(client.getConfig()).thenReturn(mock(Mqtt5ClientConfig.class));
        when(client.disconnect()).thenReturn(CompletableFuture.completedFuture(null));
        return client;
    }
}
