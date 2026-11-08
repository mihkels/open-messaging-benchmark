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
                    workers.submit(() -> {
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
    void closeInducedSendFailuresRejectReentrantCloseAndSettleAllSends() {
        MQProducer client = mock(MQProducer.class);
        RocketMQBenchmarkProducer producer = new RocketMQBenchmarkProducer(client, "topic");
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        AtomicReference<Throwable> rejected = new AtomicReference<>();
        first.whenComplete((_, error) -> {
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
