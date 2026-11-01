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
package io.openmessaging.benchmark.driver.pravega;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class PravegaBenchmarkProducerTest {

    @Test
    void overlappingTimestampedSendsKeepDistinctBuffers() {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        EventStreamWriter<ByteBuffer> writer = mock();
        when(factory.<ByteBuffer>createEventWriter(eq("stream"), any(), any())).thenReturn(writer);
        List<ByteBuffer> writes = new ArrayList<>();
        List<CompletableFuture<Void>> acknowledgements = new ArrayList<>();
        when(writer.writeEvent(any(ByteBuffer.class)))
                .thenAnswer(
                        invocation -> {
                            writes.add(invocation.getArgument(0));
                            CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
                            acknowledgements.add(acknowledgement);
                            return acknowledgement;
                        });
        PravegaBenchmarkProducer producer = new PravegaBenchmarkProducer("stream", factory, true, true);

        long before = System.currentTimeMillis();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1, 2, 3});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {4, 5, 6});
        long after = System.currentTimeMillis();

        assertThat(writes).hasSize(2);
        assertThat(writes.get(0)).isNotSameAs(writes.get(1));
        assertTimestamped(writes.get(0), new byte[] {1, 2, 3}, before, after);
        assertTimestamped(writes.get(1), new byte[] {4, 5, 6}, before, after);
        assertThat(first).isNotDone();
        assertThat(second).isNotDone();
        acknowledgements.forEach(future -> future.complete(null));
        assertThat(first).isCompleted();
        assertThat(second).isCompleted();
    }

    @Test
    void cancelledSendDoesNotRetainCallerPayload() {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        EventStreamWriter<ByteBuffer> writer = mock();
        when(factory.<ByteBuffer>createEventWriter(eq("stream"), any(), any())).thenReturn(writer);
        List<ByteBuffer> writes = new ArrayList<>();
        CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
        when(writer.writeEvent(any(ByteBuffer.class)))
                .thenAnswer(
                        invocation -> {
                            writes.add(invocation.getArgument(0));
                            return acknowledgement;
                        });
        PravegaBenchmarkProducer producer =
                new PravegaBenchmarkProducer("stream", factory, false, true);
        byte[] payload = {1, 2, 3};

        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), payload);
        assertThat(send.cancel(false)).isTrue();
        Arrays.fill(payload, (byte) 0);

        assertThat(bytes(writes.getFirst())).containsExactly(1, 2, 3);
        acknowledgement.complete(null);
        assertThat(send).isCancelled();
    }

    @Test
    void closeKeepsAcknowledgementsAndSettlesRemainingSends() throws Exception {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        EventStreamWriter<ByteBuffer> writer = mock();
        when(factory.<ByteBuffer>createEventWriter(eq("stream"), any(), any())).thenReturn(writer);
        CompletableFuture<Void> acknowledged = new CompletableFuture<>();
        CompletableFuture<Void> pending = new CompletableFuture<>();
        when(writer.writeEvent(any(ByteBuffer.class))).thenReturn(acknowledged, pending);
        doAnswer(
                        invocation -> {
                            acknowledged.complete(null);
                            return null;
                        })
                .when(writer)
                .close();
        PravegaBenchmarkProducer producer =
                new PravegaBenchmarkProducer("stream", factory, false, true);
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});

        producer.close();
        producer.close();

        assertThat(first).isCompleted();
        assertThat(second).isCompletedExceptionally();
        pending.complete(null);
        assertThat(second).isCompletedExceptionally();
        assertThat(producer.sendAsync(Optional.empty(), new byte[] {3})).isCompletedExceptionally();
        verify(writer, times(1)).close();
    }

    @Test
    void concurrentCloseSettlesAnInProgressSend() throws Exception {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        EventStreamWriter<ByteBuffer> writer = mock();
        when(factory.<ByteBuffer>createEventWriter(eq("stream"), any(), any())).thenReturn(writer);
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(writer.writeEvent(any(ByteBuffer.class)))
                .thenAnswer(
                        invocation -> {
                            publishing.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("publish was not released");
                            }
                            return new CompletableFuture<Void>();
                        });
        PravegaBenchmarkProducer producer =
                new PravegaBenchmarkProducer("stream", factory, false, true);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var sending = executor.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {1}));
            assertThat(publishing.await(5, TimeUnit.SECONDS)).isTrue();
            var closing =
                    executor.submit(
                            () -> {
                                producer.close();
                                return null;
                            });
            release.countDown();
            CompletableFuture<Void> result = sending.get(5, TimeUnit.SECONDS);
            closing.get(5, TimeUnit.SECONDS);
            assertThat(result).isCompletedExceptionally();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void concurrentCloseSettlesSendsWhileTheWriterIsStillClosing() throws Exception {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        EventStreamWriter<ByteBuffer> writer = mock();
        when(factory.<ByteBuffer>createEventWriter(eq("stream"), any(), any())).thenReturn(writer);
        when(writer.writeEvent(any(ByteBuffer.class))).thenReturn(new CompletableFuture<>());
        CountDownLatch closingWriter = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            closingWriter.countDown();
                            if (!releaseWriter.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("writer close was not released");
                            }
                            return null;
                        })
                .when(writer)
                .close();
        PravegaBenchmarkProducer producer =
                new PravegaBenchmarkProducer("stream", factory, false, true);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            var firstClose =
                    executor.submit(
                            () -> {
                                producer.close();
                                return null;
                            });
            assertThat(closingWriter.await(5, TimeUnit.SECONDS)).isTrue();

            producer.close();

            assertThat(send).isCompletedExceptionally();
            releaseWriter.countDown();
            firstClose.get(5, TimeUnit.SECONDS);
            verify(writer, times(1)).close();
        } finally {
            releaseWriter.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void synchronousWriterFailureCompletesExceptionally() {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        EventStreamWriter<ByteBuffer> writer = mock();
        when(factory.<ByteBuffer>createEventWriter(eq("stream"), any(), any())).thenReturn(writer);
        IllegalStateException cause = new IllegalStateException("writer closed");
        when(writer.writeEvent(any(ByteBuffer.class))).thenThrow(cause);
        PravegaBenchmarkProducer producer =
                new PravegaBenchmarkProducer("stream", factory, false, true);

        CompletableFuture<Void> result = producer.sendAsync(Optional.empty(), new byte[] {1});
        assertThatThrownBy(result::join).hasCause(cause);
    }

    @Test
    void closeFromClientCallbackCanBeRetriedLater() throws Exception {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        EventStreamWriter<ByteBuffer> writer = mock();
        CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
        when(factory.<ByteBuffer>createEventWriter(eq("stream"), any(), any())).thenReturn(writer);
        when(writer.writeEvent(any(ByteBuffer.class))).thenReturn(acknowledgement);
        PravegaBenchmarkProducer producer =
                new PravegaBenchmarkProducer("stream", factory, false, true);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> callback =
                send.thenRun(
                        () -> assertThatThrownBy(producer::close).isInstanceOf(IllegalStateException.class));

        acknowledgement.complete(null);
        callback.join();
        verify(writer, never()).close();
        producer.close();
        verify(writer).close();
    }

    @Test
    void undeclaredInterruptedWriterCloseStillSettlesSends() throws Exception {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        EventStreamWriter<ByteBuffer> writer = mock();
        when(factory.<ByteBuffer>createEventWriter(eq("stream"), any(), any())).thenReturn(writer);
        when(writer.writeEvent(any(ByteBuffer.class))).thenReturn(new CompletableFuture<>());
        InterruptedException interrupted = new InterruptedException("writer interrupted");
        doAnswer(
                        invocation -> {
                            throw interrupted;
                        })
                .when(writer)
                .close();
        PravegaBenchmarkProducer producer =
                new PravegaBenchmarkProducer("stream", factory, false, true);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        try {
            assertThatThrownBy(producer::close).isSameAs(interrupted);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThatThrownBy(send::join).hasRootCause(interrupted);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void transactionalProbeUsesOwnedBuffersForEveryEvent() throws Exception {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        TransactionalEventStreamWriter<ByteBuffer> writer = mock();
        Transaction<ByteBuffer> transaction = mock();
        when(factory.<ByteBuffer>createTransactionalEventWriter(
                        anyString(), eq("stream"), any(), any()))
                .thenReturn(writer);
        when(writer.beginTxn()).thenReturn(transaction);
        List<ByteBuffer> writes = new ArrayList<>();
        doAnswer(
                        invocation -> {
                            writes.add(invocation.getArgument(1));
                            return null;
                        })
                .when(transaction)
                .writeEvent(eq("key"), any(ByteBuffer.class));
        PravegaBenchmarkTransactionProducer producer =
                new PravegaBenchmarkTransactionProducer("stream", factory, true, true, 3);
        byte[] payload = {1, 2, 3};

        long before = System.currentTimeMillis();
        CompletableFuture<Void> result = producer.sendAsync(Optional.of("key"), payload);
        long after = System.currentTimeMillis();
        Arrays.fill(payload, (byte) 0);

        assertThat(result).isCompleted();
        assertThat(writes).hasSize(3);
        assertThat(writes.get(0)).isNotSameAs(writes.get(1));
        assertThat(writes.get(1)).isNotSameAs(writes.get(2));
        writes.forEach(buffer -> assertTimestamped(buffer, new byte[] {1, 2, 3}, before, after));
        verify(transaction).commit();
    }

    @Test
    void failedTransactionCompletesExceptionallyAndAllowsNextTransaction() throws Exception {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        TransactionalEventStreamWriter<ByteBuffer> writer = mock();
        Transaction<ByteBuffer> failed = mock();
        Transaction<ByteBuffer> next = mock();
        when(factory.<ByteBuffer>createTransactionalEventWriter(
                        anyString(), eq("stream"), any(), any()))
                .thenReturn(writer);
        when(writer.beginTxn()).thenReturn(failed, next);
        TxnFailedException cause = new TxnFailedException("injected failure");
        doThrow(cause).when(failed).writeEvent(any(ByteBuffer.class));
        PravegaBenchmarkTransactionProducer producer =
                new PravegaBenchmarkTransactionProducer("stream", factory, false, true, 2);

        CompletableFuture<Void> result = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertThatThrownBy(result::join).hasCause(cause);
        verify(failed).abort();
        assertThat(producer.sendAsync(Optional.empty(), new byte[] {2})).isCompleted();
        verify(next).writeEvent(any(ByteBuffer.class));
    }

    @Test
    void transactionCreationFailureCompletesExceptionallyAndAllowsRetry() {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        TransactionalEventStreamWriter<ByteBuffer> writer = mock();
        Transaction<ByteBuffer> next = mock();
        when(factory.<ByteBuffer>createTransactionalEventWriter(
                        anyString(), eq("stream"), any(), any()))
                .thenReturn(writer);
        IllegalStateException cause = new IllegalStateException("controller unavailable");
        when(writer.beginTxn()).thenThrow(cause).thenReturn(next);
        PravegaBenchmarkTransactionProducer producer =
                new PravegaBenchmarkTransactionProducer("stream", factory, false, true, 2);

        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(cause);
        assertThat(producer.sendAsync(Optional.empty(), new byte[] {2})).isCompleted();
    }

    @Test
    void checkedControllerFailureCompletesExceptionally() {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        TransactionalEventStreamWriter<ByteBuffer> writer = mock();
        when(factory.<ByteBuffer>createTransactionalEventWriter(
                        anyString(), eq("stream"), any(), any()))
                .thenReturn(writer);
        IOException cause = new IOException("controller unavailable");
        when(writer.beginTxn())
                .thenAnswer(
                        invocation -> {
                            throw cause;
                        });
        PravegaBenchmarkTransactionProducer producer =
                new PravegaBenchmarkTransactionProducer("stream", factory, false, true, 2);

        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(cause);
    }

    @Test
    void checkedAbortFailureIsSuppressedOnTransactionFailure() throws Exception {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        TransactionalEventStreamWriter<ByteBuffer> writer = mock();
        Transaction<ByteBuffer> transaction = mock();
        when(factory.<ByteBuffer>createTransactionalEventWriter(
                        anyString(), eq("stream"), any(), any()))
                .thenReturn(writer);
        when(writer.beginTxn()).thenReturn(transaction);
        TxnFailedException cause = new TxnFailedException("write failed");
        IOException abortFailure = new IOException("controller unavailable");
        doThrow(cause).when(transaction).writeEvent(any(ByteBuffer.class));
        doAnswer(
                        invocation -> {
                            throw abortFailure;
                        })
                .when(transaction)
                .abort();
        PravegaBenchmarkTransactionProducer producer =
                new PravegaBenchmarkTransactionProducer("stream", factory, false, true, 2);

        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(cause);
        assertThat(cause.getSuppressed()).containsExactly(abortFailure);
    }

    @Test
    void transactionalCloseClosesWriterWhenAbortFails() throws Exception {
        EventStreamClientFactory factory = mock(EventStreamClientFactory.class);
        TransactionalEventStreamWriter<ByteBuffer> writer = mock();
        Transaction<ByteBuffer> transaction = mock();
        when(factory.<ByteBuffer>createTransactionalEventWriter(
                        anyString(), eq("stream"), any(), any()))
                .thenReturn(writer);
        when(writer.beginTxn()).thenReturn(transaction);
        IllegalStateException cause = new IllegalStateException("abort failed");
        IllegalStateException closeFailure = new IllegalStateException("writer close failed");
        doThrow(cause).when(transaction).abort();
        doThrow(closeFailure).when(writer).close();
        PravegaBenchmarkTransactionProducer producer =
                new PravegaBenchmarkTransactionProducer("stream", factory, false, true, 2);
        producer.sendAsync(Optional.empty(), new byte[] {1});

        assertThatThrownBy(producer::close)
                .isSameAs(cause)
                .satisfies(error -> assertThat(error.getSuppressed()).containsExactly(closeFailure));
        verify(writer).close();
        assertThat(producer.sendAsync(Optional.empty(), new byte[] {2})).isCompletedExceptionally();
    }

    private static void assertTimestamped(
            ByteBuffer buffer, byte[] payload, long before, long after) {
        ByteBuffer copy = buffer.duplicate();
        assertThat(copy.getLong()).isBetween(before, after);
        byte[] actual = new byte[copy.remaining()];
        copy.get(actual);
        assertThat(actual).isEqualTo(payload);
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }
}
