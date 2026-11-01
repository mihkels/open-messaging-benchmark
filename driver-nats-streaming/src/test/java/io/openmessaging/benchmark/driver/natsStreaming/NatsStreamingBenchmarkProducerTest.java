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
package io.openmessaging.benchmark.driver.natsStreaming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.streaming.AckHandler;
import io.nats.streaming.StreamingConnection;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

@Timeout(10)
class NatsStreamingBenchmarkProducerTest {
    @Test
    void publishFailureAndEmptyGuidCompleteExceptionally() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        IOException failure = new IOException("publish failed");
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenThrow(failure)
                .thenReturn("");
        NatsStreamingBenchmarkProducer producer =
                new NatsStreamingBenchmarkProducer(connection, "topic");
        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(failure);
        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCauseInstanceOf(IllegalStateException.class);
        producer.close();
    }

    @Test
    void synchronousAcknowledgementIsMatchedAfterPublishReturns() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenAnswer(
                        invocation -> {
                            AckHandler handler = invocation.getArgument(2);
                            handler.onAck("message-id", null);
                            return "message-id";
                        });
        NatsStreamingBenchmarkProducer producer =
                new NatsStreamingBenchmarkProducer(connection, "topic");
        producer.sendAsync(Optional.empty(), new byte[] {1}).get(2, TimeUnit.SECONDS);
        producer.close();
    }

    @Test
    void acknowledgementErrorsAndMismatchedIdentifiersAreExceptional() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenReturn("message-id");
        NatsStreamingBenchmarkProducer producer =
                new NatsStreamingBenchmarkProducer(connection, "topic");
        CompletableFuture<Void> rejected = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> mismatched = producer.sendAsync(Optional.empty(), new byte[] {2});
        ArgumentCaptor<AckHandler> acknowledgements = ArgumentCaptor.forClass(AckHandler.class);
        verify(connection, times(2))
                .publish(eq("topic"), any(byte[].class), acknowledgements.capture());
        IOException failure = new IOException("broker rejected");
        acknowledgements.getAllValues().get(0).onAck("message-id", failure);
        acknowledgements.getAllValues().get(1).onAck("another-id", null);
        assertThatThrownBy(rejected::join).hasCause(failure);
        assertThatThrownBy(mismatched::join).hasCauseInstanceOf(IllegalStateException.class);
        producer.close();
    }

    @Test
    void cancellationOwnsPayloadAndCloseSettlesPendingWithoutClosingConnection() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenReturn("message-id");
        NatsStreamingBenchmarkProducer producer =
                new NatsStreamingBenchmarkProducer(connection, "topic");
        byte[] payload = {1, 2, 3};
        CompletableFuture<Void> canceled = producer.sendAsync(Optional.empty(), payload);
        ArgumentCaptor<byte[]> wirePayload = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<AckHandler> acknowledgements = ArgumentCaptor.forClass(AckHandler.class);
        verify(connection).publish(eq("topic"), wirePayload.capture(), acknowledgements.capture());
        assertThat(canceled.cancel(false)).isTrue();
        Arrays.fill(payload, (byte) 9);
        assertThat(wirePayload.getValue()).containsExactly(1, 2, 3);

        CompletableFuture<Void> pending = producer.sendAsync(Optional.empty(), new byte[] {4});
        producer.close();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IllegalStateException.class);
        acknowledgements.getValue().onAck("message-id", null);
        assertThat(canceled).isCancelled();
        verify(connection, never()).close();
        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {5}).join())
                .hasCauseInstanceOf(IllegalStateException.class);
        verify(connection, times(2)).publish(eq("topic"), any(byte[].class), any(AckHandler.class));
    }

    @Test
    void closingOneHandleDoesNotSettleAnotherHandlesSend() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenReturn("first-id")
                .thenReturn("second-id");
        NatsStreamingBenchmarkProducer first = new NatsStreamingBenchmarkProducer(connection, "topic");
        NatsStreamingBenchmarkProducer second = new NatsStreamingBenchmarkProducer(connection, "topic");
        CompletableFuture<Void> firstSend = first.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> secondSend = second.sendAsync(Optional.empty(), new byte[] {2});
        ArgumentCaptor<AckHandler> acknowledgements = ArgumentCaptor.forClass(AckHandler.class);
        verify(connection, times(2))
                .publish(eq("topic"), any(byte[].class), acknowledgements.capture());
        first.close();
        assertThatThrownBy(firstSend::join).hasCauseInstanceOf(IllegalStateException.class);
        assertThat(secondSend.isDone()).isFalse();
        acknowledgements.getAllValues().get(1).onAck("second-id", null);
        secondSend.get(2, TimeUnit.SECONDS);
        second.close();
        verify(connection, never()).close();
    }

    @Test
    void closeWaitsForPublicationBoundaryAndPreservesInterruption() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenAnswer(
                        invocation -> {
                            publishing.countDown();
                            release.await();
                            return "message-id";
                        });
        NatsStreamingBenchmarkProducer producer =
                new NatsStreamingBenchmarkProducer(connection, "topic");
        ExecutorService sender = Executors.newSingleThreadExecutor();
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch closeFinished = new CountDownLatch(1);
        AtomicBoolean interruptionVisible = new AtomicBoolean();
        Thread closer =
                new Thread(
                        () -> {
                            closeStarted.countDown();
                            producer.close();
                            interruptionVisible.set(Thread.currentThread().isInterrupted());
                            closeFinished.countDown();
                        });
        try {
            Future<CompletableFuture<Void>> send =
                    sender.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {1}));
            assertThat(publishing.await(2, TimeUnit.SECONDS)).isTrue();
            closer.start();
            assertThat(closeStarted.await(2, TimeUnit.SECONDS)).isTrue();
            closer.interrupt();
            assertThat(closeFinished.await(150, TimeUnit.MILLISECONDS)).isFalse();
            release.countDown();
            CompletableFuture<Void> pending = send.get(2, TimeUnit.SECONDS);
            assertThat(closeFinished.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(interruptionVisible).isTrue();
            assertThatThrownBy(pending::join).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {2}).join())
                    .hasCauseInstanceOf(IllegalStateException.class);
            verify(connection, times(1)).publish(eq("topic"), any(byte[].class), any(AckHandler.class));
            verify(connection, never()).close();
        } finally {
            release.countDown();
            closer.join(2000);
            sender.shutdownNow();
        }
    }

    @Test
    void interruptedPublishPreservesItsCauseAndInterruptFlag() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        InterruptedException failure = new InterruptedException("interrupted");
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenThrow(failure);
        NatsStreamingBenchmarkProducer producer =
                new NatsStreamingBenchmarkProducer(connection, "topic");
        try {
            assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                    .hasCause(failure);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
            producer.close();
        }
    }

    @Test
    void reentrantCloseIsRejectedUntilNestedPublicationsReturn() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<AckHandler> firstAcknowledgement = new AtomicReference<>();
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenAnswer(
                        invocation -> {
                            int call = calls.incrementAndGet();
                            if (call == 1) {
                                firstAcknowledgement.set(invocation.getArgument(2));
                                return "first";
                            }
                            if (call == 2) {
                                firstAcknowledgement.get().onAck("first", null);
                                return "second";
                            }
                            return "nested";
                        });
        NatsStreamingBenchmarkProducer producer =
                new NatsStreamingBenchmarkProducer(connection, "topic");
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
                            Thread thread = new Thread(task, "nats-streaming-reentrant-test");
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
            producer.close();
            assertThatThrownBy(secondSend::join).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(nested.get()::join).hasCauseInstanceOf(IllegalStateException.class);
            verify(connection, never()).close();
        } finally {
            sender.shutdownNow();
        }
    }

    @Test
    void asynchronousAcknowledgementCannotCloseWhileAnotherPublicationIsActive() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<AckHandler> firstAcknowledgement = new AtomicReference<>();
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenAnswer(
                        invocation -> {
                            int call = calls.incrementAndGet();
                            if (call == 1) {
                                firstAcknowledgement.set(invocation.getArgument(2));
                                return "first";
                            }
                            if (call == 2) {
                                publishing.countDown();
                                if (!release.await(5, TimeUnit.SECONDS)) {
                                    throw new AssertionError("publication was not released");
                                }
                            }
                            return "other";
                        });
        NatsStreamingBenchmarkProducer producer =
                new NatsStreamingBenchmarkProducer(connection, "topic");
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
            Future<?> ack = workers.submit(() -> firstAcknowledgement.get().onAck("first", null));
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
            verify(connection, never()).close();
        } finally {
            release.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void closeInducedSendFailuresRejectReentrantCloseAndSettleAllSends() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenReturn("message-id");
        NatsStreamingBenchmarkProducer producer =
                new NatsStreamingBenchmarkProducer(connection, "topic");
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
        verify(connection, never()).close();
    }

    @Test
    void failedAcknowledgementAlsoGuardsReentrantClose() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        when(connection.publish(eq("topic"), any(byte[].class), any(AckHandler.class)))
                .thenReturn("message-id");
        NatsStreamingBenchmarkProducer producer =
                new NatsStreamingBenchmarkProducer(connection, "topic");
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
        ArgumentCaptor<AckHandler> acknowledgement = ArgumentCaptor.forClass(AckHandler.class);
        verify(connection).publish(eq("topic"), any(byte[].class), acknowledgement.capture());
        IOException failure = new IOException("acknowledgement failed");
        acknowledgement.getValue().onAck("message-id", failure);
        assertThat(rejected.join()).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(send::join).hasCause(failure);
        CompletableFuture<Void> next = producer.sendAsync(Optional.empty(), new byte[] {2});
        assertThat(next.isDone()).isFalse();
        producer.close();
        assertThatThrownBy(next::join).hasCauseInstanceOf(IllegalStateException.class);
    }
}
