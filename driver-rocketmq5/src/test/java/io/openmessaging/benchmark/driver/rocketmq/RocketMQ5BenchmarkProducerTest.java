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
package io.openmessaging.benchmark.driver.rocketmq;

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

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RocketMQ5BenchmarkProducerTest {
    @Test
    void cancellationOwnsThePayloadAndCloseFailsOutstandingSends() throws Exception {
        Producer client = mock(Producer.class);
        CompletableFuture<SendReceipt> firstReceipt = new CompletableFuture<>();
        CompletableFuture<SendReceipt> secondReceipt = new CompletableFuture<>();
        when(client.sendAsync(any(Message.class))).thenReturn(firstReceipt).thenReturn(secondReceipt);
        RocketMQ5BenchmarkProducer producer = new RocketMQ5BenchmarkProducer(client, "topic");
        byte[] payload = {1, 2, 3};
        CompletableFuture<Void> canceled = producer.sendAsync(Optional.of("key"), payload);
        ArgumentCaptor<Message> message = ArgumentCaptor.forClass(Message.class);
        verify(client).sendAsync(message.capture());
        assertThat(canceled.cancel(false)).isTrue();
        Arrays.fill(payload, (byte) 9);
        ByteBuffer wire = message.getValue().getBody().duplicate();
        byte[] wirePayload = new byte[wire.remaining()];
        wire.get(wirePayload);
        assertThat(wirePayload).containsExactly(1, 2, 3);
        assertThat(message.getValue().getKeys()).containsExactly("key");

        CompletableFuture<Void> pending = producer.sendAsync(Optional.empty(), new byte[] {4});
        producer.close();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IllegalStateException.class);
        secondReceipt.complete(mock(SendReceipt.class));
        assertThat(pending.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {5}).join())
                .hasCauseInstanceOf(IllegalStateException.class);
        producer.close();
        verify(client, times(1)).close();
    }

    @Test
    void synchronousRejectionAndMissingReceiptAreExceptional() throws Exception {
        Producer client = mock(Producer.class);
        RuntimeException rejection = new RuntimeException("rejected");
        when(client.sendAsync(any(Message.class))).thenThrow(rejection);
        RocketMQ5BenchmarkProducer producer = new RocketMQ5BenchmarkProducer(client, "topic");
        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(rejection);
        producer.close();

        Producer otherClient = mock(Producer.class);
        when(otherClient.sendAsync(any(Message.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        RocketMQ5BenchmarkProducer other = new RocketMQ5BenchmarkProducer(otherClient, "topic");
        assertThatThrownBy(() -> other.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCauseInstanceOf(IllegalStateException.class);
        other.close();
    }

    @Test
    void clientCloseFailureDoesNotLeaveSendsUnresolved() throws Exception {
        Producer client = mock(Producer.class);
        when(client.sendAsync(any(Message.class))).thenReturn(new CompletableFuture<>());
        IOException failure = new IOException("close failed");
        doThrow(failure).when(client).close();
        RocketMQ5BenchmarkProducer producer = new RocketMQ5BenchmarkProducer(client, "topic");
        CompletableFuture<Void> pending = producer.sendAsync(Optional.empty(), new byte[] {1});
        assertThatThrownBy(producer::close).isSameAs(failure);
        assertThatThrownBy(pending::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCause(failure);
    }

    @Test
    void reentrantCloseIsRejectedUntilNestedPublicationsReturn() throws Exception {
        Producer client = mock(Producer.class);
        CompletableFuture<SendReceipt> firstReceipt = new CompletableFuture<>();
        CompletableFuture<SendReceipt> secondReceipt = new CompletableFuture<>();
        CompletableFuture<SendReceipt> nestedReceipt = new CompletableFuture<>();
        SendReceipt receipt = mock(SendReceipt.class);
        AtomicInteger calls = new AtomicInteger();
        when(client.sendAsync(any(Message.class)))
                .thenAnswer(
                        invocation -> {
                            int call = calls.incrementAndGet();
                            if (call == 1) {
                                return firstReceipt;
                            }
                            if (call == 2) {
                                firstReceipt.complete(receipt);
                                return secondReceipt;
                            }
                            return nestedReceipt;
                        });
        RocketMQ5BenchmarkProducer producer = new RocketMQ5BenchmarkProducer(client, "topic");
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
                            Thread thread = new Thread(task, "rocketmq5-reentrant-test");
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
            assertThat(first).isCompleted();
            assertThat(secondSend.isDone()).isFalse();
            assertThat(nested.get().isDone()).isFalse();
            verify(client, never()).close();
            producer.close();
            assertThatThrownBy(secondSend::join).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(nested.get()::join).hasCauseInstanceOf(IllegalStateException.class);
            verify(client, times(1)).close();
        } finally {
            sender.shutdownNow();
        }
    }

    @Test
    void closeOnAnotherThreadWaitsForPublicationAndSettlesTheSend() throws Exception {
        Producer client = mock(Producer.class);
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        when(client.sendAsync(any(Message.class)))
                .thenAnswer(
                        invocation -> {
                            publishing.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("publication was not released");
                            }
                            return new CompletableFuture<SendReceipt>();
                        });
        RocketMQ5BenchmarkProducer producer = new RocketMQ5BenchmarkProducer(client, "topic");
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<CompletableFuture<Void>> send =
                    workers.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {1}));
            assertThat(publishing.await(5, TimeUnit.SECONDS)).isTrue();
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
            assertThat(closing.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> close.get(150, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            release.countDown();
            CompletableFuture<Void> pending = send.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(pending::join).hasCauseInstanceOf(IllegalStateException.class);
            verify(client, times(1)).close();
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void asynchronousAcknowledgementCannotCloseWhileAnotherPublicationIsActive() throws Exception {
        Producer client = mock(Producer.class);
        CompletableFuture<SendReceipt> firstReceipt = new CompletableFuture<>();
        CompletableFuture<SendReceipt> secondReceipt = new CompletableFuture<>();
        CompletableFuture<SendReceipt> thirdReceipt = new CompletableFuture<>();
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        when(client.sendAsync(any(Message.class)))
                .thenAnswer(
                        invocation -> {
                            int call = calls.incrementAndGet();
                            if (call == 1) {
                                return firstReceipt;
                            }
                            if (call == 2) {
                                publishing.countDown();
                                if (!release.await(5, TimeUnit.SECONDS)) {
                                    throw new AssertionError("publication was not released");
                                }
                                return secondReceipt;
                            }
                            return thirdReceipt;
                        });
        RocketMQ5BenchmarkProducer producer = new RocketMQ5BenchmarkProducer(client, "topic");
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
            Future<?> ack = workers.submit(() -> firstReceipt.complete(mock(SendReceipt.class)));
            ack.get(2, TimeUnit.SECONDS);
            assertThat(rejected.get())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("client completion callback");
            verify(client, never()).close();
            release.countDown();
            CompletableFuture<Void> secondSend = second.get(2, TimeUnit.SECONDS);
            CompletableFuture<Void> third = producer.sendAsync(Optional.empty(), new byte[] {3});
            assertThat(third.isDone()).isFalse();
            producer.close();
            assertThatThrownBy(secondSend::join).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(third::join).hasCauseInstanceOf(IllegalStateException.class);
            verify(client, times(1)).close();
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void closeFailureCallbacksRejectReentrantCloseAndRetainTheClientFailure() throws Exception {
        Producer client = mock(Producer.class);
        when(client.sendAsync(any(Message.class))).thenReturn(new CompletableFuture<>());
        IOException failure = new IOException("client close failed");
        doThrow(failure).when(client).close();
        RocketMQ5BenchmarkProducer producer = new RocketMQ5BenchmarkProducer(client, "topic");
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
        verify(client, times(1)).close();
    }

    @Test
    void failedAcknowledgementAlsoGuardsReentrantClose() throws Exception {
        Producer client = mock(Producer.class);
        CompletableFuture<SendReceipt> receipt = new CompletableFuture<>();
        when(client.sendAsync(any(Message.class))).thenReturn(receipt);
        RocketMQ5BenchmarkProducer producer = new RocketMQ5BenchmarkProducer(client, "topic");
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
        receipt.completeExceptionally(failure);
        assertThat(rejected.join()).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(send::join).hasCause(failure);
        CompletableFuture<Void> next = producer.sendAsync(Optional.empty(), new byte[] {2});
        assertThatThrownBy(next::join).hasCause(failure);
        producer.close();
    }

    @Test
    void concurrentCloseWaitsForTheOwnedClientAndKeepsItsFailure() throws Exception {
        Producer client = mock(Producer.class);
        when(client.sendAsync(any(Message.class))).thenReturn(new CompletableFuture<>());
        CountDownLatch closingClient = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch secondClosing = new CountDownLatch(1);
        IOException failure = new IOException("client close failed");
        doAnswer(
                        invocation -> {
                            closingClient.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("client close was not released");
                            }
                            throw failure;
                        })
                .when(client)
                .close();
        RocketMQ5BenchmarkProducer producer = new RocketMQ5BenchmarkProducer(client, "topic");
        CompletableFuture<Void> pending = producer.sendAsync(Optional.empty(), new byte[] {1});
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
            assertThat(closingClient.await(2, TimeUnit.SECONDS)).isTrue();
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
            release.countDown();
            assertThat(first.get(2, TimeUnit.SECONDS)).isSameAs(failure);
            second.get(2, TimeUnit.SECONDS);
            assertThatThrownBy(pending::join)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCause(failure);
            verify(client, times(1)).close();
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void acknowledgedSendStaysSuccessfulWhenClientCloseThenFails() throws Exception {
        Producer client = mock(Producer.class);
        CompletableFuture<SendReceipt> receipt = new CompletableFuture<>();
        when(client.sendAsync(any(Message.class))).thenReturn(receipt);
        IOException failure = new IOException("client close failed");
        doAnswer(
                        invocation -> {
                            receipt.complete(mock(SendReceipt.class));
                            throw failure;
                        })
                .when(client)
                .close();
        RocketMQ5BenchmarkProducer producer = new RocketMQ5BenchmarkProducer(client, "topic");
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertThatThrownBy(producer::close).isSameAs(failure);
        assertThat(send.join()).isNull();
    }
}
