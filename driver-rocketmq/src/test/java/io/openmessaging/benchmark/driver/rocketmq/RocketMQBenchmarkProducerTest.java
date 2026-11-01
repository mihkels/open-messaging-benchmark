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

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RocketMQBenchmarkProducerTest {
    @Test
    void cancellationOwnsThePayloadAndCloseDoesNotCloseTheSharedClient() throws Exception {
        MQProducer client = mock(MQProducer.class);
        RocketMQBenchmarkProducer producer = new RocketMQBenchmarkProducer(client, "topic");
        byte[] payload = {1, 2, 3};
        CompletableFuture<Void> canceled = producer.sendAsync(Optional.of("key"), payload);
        ArgumentCaptor<Message> messages = ArgumentCaptor.forClass(Message.class);
        ArgumentCaptor<SendCallback> callbacks = ArgumentCaptor.forClass(SendCallback.class);
        verify(client).send(messages.capture(), callbacks.capture());
        assertThat(canceled.cancel(false)).isTrue();
        Arrays.fill(payload, (byte) 9);
        assertThat(messages.getValue().getBody()).containsExactly(1, 2, 3);
        assertThat(messages.getValue().getKeys()).contains("key");

        CompletableFuture<Void> pending = producer.sendAsync(Optional.empty(), new byte[] {4});
        producer.close();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IllegalStateException.class);
        verify(client, times(2)).send(messages.capture(), callbacks.capture());
        callbacks.getAllValues().get(callbacks.getAllValues().size() - 1).onSuccess(null);
        assertThat(pending.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {5}).join())
                .hasCauseInstanceOf(IllegalStateException.class);
        producer.close();
        verify(client, never()).shutdown();
    }

    @Test
    void synchronousRejectionAndInterruptionCompleteExceptionally() throws Exception {
        MQProducer client = mock(MQProducer.class);
        MQClientException failure = new MQClientException("send rejected", null);
        doThrow(failure).when(client).send(any(Message.class), any(SendCallback.class));
        RocketMQBenchmarkProducer producer = new RocketMQBenchmarkProducer(client, "topic");
        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(failure);

        MQProducer interruptedClient = mock(MQProducer.class);
        InterruptedException interrupted = new InterruptedException("interrupted");
        doThrow(interrupted).when(interruptedClient).send(any(Message.class), any(SendCallback.class));
        RocketMQBenchmarkProducer other = new RocketMQBenchmarkProducer(interruptedClient, "topic");
        try {
            assertThatThrownBy(() -> other.sendAsync(Optional.empty(), new byte[] {1}).join())
                    .hasCause(interrupted);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
            producer.close();
            other.close();
        }
    }

    @Test
    void closingOneHandleDoesNotAffectAnotherHandleUsingTheSharedClient() throws Exception {
        MQProducer client = mock(MQProducer.class);
        RocketMQBenchmarkProducer first = new RocketMQBenchmarkProducer(client, "topic");
        RocketMQBenchmarkProducer second = new RocketMQBenchmarkProducer(client, "topic");
        CompletableFuture<Void> firstSend = first.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> secondSend = second.sendAsync(Optional.empty(), new byte[] {2});
        ArgumentCaptor<SendCallback> callbacks = ArgumentCaptor.forClass(SendCallback.class);
        verify(client, times(2)).send(any(Message.class), callbacks.capture());
        first.close();
        assertThatThrownBy(firstSend::join).hasCauseInstanceOf(IllegalStateException.class);
        assertThat(secondSend.isDone()).isFalse();
        callbacks.getAllValues().get(1).onSuccess(null);
        assertThat(secondSend.join()).isNull();
        second.close();
        verify(client, never()).shutdown();
    }

    @Test
    void reentrantCloseIsRejectedUntilNestedPublicationsReturn() throws Exception {
        MQProducer client = mock(MQProducer.class);
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<SendCallback> firstCallback = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            int call = calls.incrementAndGet();
                            if (call == 1) {
                                firstCallback.set(invocation.getArgument(1));
                            } else if (call == 2) {
                                firstCallback.get().onSuccess(null);
                            }
                            return null;
                        })
                .when(client)
                .send(any(Message.class), any(SendCallback.class));
        RocketMQBenchmarkProducer producer = new RocketMQBenchmarkProducer(client, "topic");
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        AtomicReference<CompletableFuture<Void>> nested = new AtomicReference<>();
        CompletableFuture<Void> closeAttempt =
                first.thenRun(
                        () -> {
                            nested.set(producer.sendAsync(Optional.empty(), new byte[] {3}));
                            producer.close();
                        });
        ExecutorService sender =
                Executors.newSingleThreadExecutor(
                        task -> {
                            Thread thread = new Thread(task, "rocketmq-reentrant-test");
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
            producer.close();
            assertThatThrownBy(secondSend::join).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(nested.get()::join).hasCauseInstanceOf(IllegalStateException.class);
            verify(client, never()).shutdown();
        } finally {
            sender.shutdownNow();
        }
    }

    @Test
    void closeOnAnotherThreadWaitsForPublicationAndSettlesTheSend() throws Exception {
        MQProducer client = mock(MQProducer.class);
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            publishing.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("publication was not released");
                            }
                            return null;
                        })
                .when(client)
                .send(any(Message.class), any(SendCallback.class));
        RocketMQBenchmarkProducer producer = new RocketMQBenchmarkProducer(client, "topic");
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<CompletableFuture<Void>> send =
                    workers.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {1}));
            assertThat(publishing.await(5, TimeUnit.SECONDS)).isTrue();
            Future<?> close =
                    workers.submit(
                            () -> {
                                closing.countDown();
                                producer.close();
                            });
            assertThat(closing.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> close.get(150, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            release.countDown();
            CompletableFuture<Void> pending = send.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(pending::join).hasCauseInstanceOf(IllegalStateException.class);
            verify(client, never()).shutdown();
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void asynchronousAcknowledgementCannotCloseWhileAnotherPublicationIsActive() throws Exception {
        MQProducer client = mock(MQProducer.class);
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<SendCallback> firstCallback = new AtomicReference<>();
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            int call = calls.incrementAndGet();
                            if (call == 1) {
                                firstCallback.set(invocation.getArgument(1));
                            } else if (call == 2) {
                                publishing.countDown();
                                if (!release.await(5, TimeUnit.SECONDS)) {
                                    throw new AssertionError("publication was not released");
                                }
                            }
                            return null;
                        })
                .when(client)
                .send(any(Message.class), any(SendCallback.class));
        RocketMQBenchmarkProducer producer = new RocketMQBenchmarkProducer(client, "topic");
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        AtomicReference<Throwable> rejected = new AtomicReference<>();
        first.thenRun(
                () -> {
                    try {
                        producer.close();
                    } catch (RuntimeException failure) {
                        rejected.set(failure);
                    }
                });
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<CompletableFuture<Void>> second =
                    workers.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {2}));
            assertThat(publishing.await(2, TimeUnit.SECONDS)).isTrue();
            Future<?> ack = workers.submit(() -> firstCallback.get().onSuccess(null));
            ack.get(2, TimeUnit.SECONDS);
            assertThat(rejected.get())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("client completion callback");
            release.countDown();
            CompletableFuture<Void> secondSend = second.get(2, TimeUnit.SECONDS);
            CompletableFuture<Void> third = producer.sendAsync(Optional.empty(), new byte[] {3});
            assertThat(third.isDone()).isFalse();
            producer.close();
            assertThatThrownBy(secondSend::join).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(third::join).hasCauseInstanceOf(IllegalStateException.class);
            verify(client, never()).shutdown();
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void closeInducedSendFailuresRejectReentrantCloseAndSettleAllSends() throws Exception {
        MQProducer client = mock(MQProducer.class);
        RocketMQBenchmarkProducer producer = new RocketMQBenchmarkProducer(client, "topic");
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        AtomicReference<Throwable> rejected = new AtomicReference<>();
        first.whenComplete(
                (unused, error) -> {
                    try {
                        producer.close();
                    } catch (RuntimeException failure) {
                        rejected.set(failure);
                    }
                });

        producer.close();

        assertThat(rejected.get())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("client completion callback");
        assertThatThrownBy(first::join).hasCauseInstanceOf(IllegalStateException.class);
        assertThatThrownBy(second::join).hasCauseInstanceOf(IllegalStateException.class);
        verify(client, never()).shutdown();
    }

    @Test
    void failedAcknowledgementAlsoGuardsReentrantClose() throws Exception {
        MQProducer client = mock(MQProducer.class);
        RocketMQBenchmarkProducer producer = new RocketMQBenchmarkProducer(client, "topic");
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Throwable> rejected =
                send.handle(
                        (unused, error) -> {
                            try {
                                producer.close();
                                return null;
                            } catch (RuntimeException failure) {
                                return failure;
                            }
                        });
        ArgumentCaptor<SendCallback> callback = ArgumentCaptor.forClass(SendCallback.class);
        verify(client).send(any(Message.class), callback.capture());
        RuntimeException failure = new RuntimeException("acknowledgement failed");
        callback.getValue().onException(failure);
        assertThat(rejected.join()).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(send::join).hasCause(failure);
        CompletableFuture<Void> next = producer.sendAsync(Optional.empty(), new byte[] {2});
        assertThat(next.isDone()).isFalse();
        producer.close();
        assertThatThrownBy(next::join).hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void callbackWithoutAFailureDoesNotReportSuccess() throws Exception {
        MQProducer client = mock(MQProducer.class);
        RocketMQBenchmarkProducer producer = new RocketMQBenchmarkProducer(client, "topic");
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        ArgumentCaptor<SendCallback> callback = ArgumentCaptor.forClass(SendCallback.class);
        verify(client).send(any(Message.class), callback.capture());
        callback.getValue().onException(null);
        assertThatThrownBy(send::join).hasCauseInstanceOf(IllegalStateException.class);
        producer.close();
    }
}
