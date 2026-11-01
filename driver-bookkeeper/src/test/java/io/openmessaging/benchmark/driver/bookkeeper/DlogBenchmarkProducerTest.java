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
package io.openmessaging.benchmark.driver.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dlshade.org.apache.distributedlog.DLSN;
import dlshade.org.apache.distributedlog.LogRecord;
import dlshade.org.apache.distributedlog.api.AsyncLogWriter;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DlogBenchmarkProducerTest {

    @Test
    void canceledSendDoesNotRetainCallerPayload() {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        AtomicReference<LogRecord> written = new AtomicReference<>();
        CompletableFuture<DLSN> acknowledgement = new CompletableFuture<>();
        when(writer.write(any(LogRecord.class)))
                .thenAnswer(
                        invocation -> {
                            written.set(invocation.getArgument(0));
                            return acknowledgement;
                        });
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(writer);
        byte[] payload = {1, 2, 3};

        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), payload);
        assertThat(send.cancel(false)).isTrue();
        Arrays.fill(payload, (byte) 0);

        assertThat(written.get().getPayload()).containsExactly(1, 2, 3);
        acknowledgement.complete(null);
        assertThat(send).isCancelled();
    }

    @Test
    void synchronousWriterFailureCompletesExceptionally() {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        IllegalStateException cause = new IllegalStateException("writer closed");
        when(writer.write(any(LogRecord.class))).thenThrow(cause);
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(writer);

        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(cause);
    }

    @Test
    void synchronousInterruptedWriteCompletesExceptionallyAndRestoresInterrupt() {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        InterruptedException cause = new InterruptedException("write interrupted");
        when(writer.write(any(LogRecord.class)))
                .thenAnswer(
                        invocation -> {
                            throw cause;
                        });
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(writer);

        try {
            CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
            assertThatThrownBy(send::join).hasCause(cause);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void closeFailsPendingSendsAndIgnoresLateAcknowledgements() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        CompletableFuture<DLSN> acknowledgement = new CompletableFuture<>();
        when(writer.write(any(LogRecord.class))).thenReturn(acknowledgement);
        when(writer.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(writer);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        producer.close();
        acknowledgement.complete(null);
        producer.close();

        assertThat(send).isCompletedExceptionally();
        assertThat(producer.sendAsync(Optional.empty(), new byte[] {2})).isCompletedExceptionally();
        verify(writer, times(1)).write(any(LogRecord.class));
        verify(writer, times(1)).asyncClose();
    }

    @Test
    void preservesAcknowledgementDeliveredDuringClose() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        CompletableFuture<DLSN> acknowledgement = new CompletableFuture<>();
        when(writer.write(any(LogRecord.class))).thenReturn(acknowledgement);
        when(writer.asyncClose())
                .thenAnswer(
                        invocation -> {
                            acknowledgement.complete(null);
                            return CompletableFuture.completedFuture(null);
                        });
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(writer);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        producer.close();

        assertThat(send).isCompletedWithValue(null);
    }

    @Test
    void closeFailureStillSettlesPendingSends() {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        IllegalStateException failure = new IllegalStateException("close failed");
        when(writer.write(any(LogRecord.class))).thenReturn(new CompletableFuture<>());
        when(writer.asyncClose()).thenReturn(CompletableFuture.failedFuture(failure));
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(writer);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertThatThrownBy(producer::close).hasCause(failure);
        assertThatThrownBy(send::join).hasCauseInstanceOf(IllegalStateException.class);
        assertThatThrownBy(send::join).hasRootCause(failure);
    }

    @Test
    void closeFromClientCallbackCanBeRetriedLater() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        CompletableFuture<DLSN> acknowledgement = new CompletableFuture<>();
        when(writer.write(any(LogRecord.class))).thenReturn(acknowledgement);
        when(writer.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(manager, writer);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> callback =
                send.thenRun(
                        () -> assertThatThrownBy(producer::close).isInstanceOf(IllegalStateException.class));

        acknowledgement.complete(null);
        callback.join();
        verify(writer, never()).asyncClose();
        verify(manager, never()).close();
        producer.close();
        verify(writer).asyncClose();
        verify(manager).close();
    }

    @Test
    void closeWaitsForAnInProgressWriteBeforeClosingWriter() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(writer.write(any(LogRecord.class)))
                .thenAnswer(
                        invocation -> {
                            writing.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("write was not released");
                            }
                            return new CompletableFuture<DLSN>();
                        });
        when(writer.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(writer);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var sending = executor.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {1}));
            assertThat(writing.await(5, TimeUnit.SECONDS)).isTrue();
            var closing =
                    executor.submit(
                            () -> {
                                producer.close();
                                return null;
                            });
            assertThatThrownBy(() -> closing.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            verify(writer, never()).asyncClose();
            release.countDown();
            CompletableFuture<Void> send = sending.get(5, TimeUnit.SECONDS);
            closing.get(5, TimeUnit.SECONDS);
            assertThat(send).isCompletedExceptionally();
            verify(writer).asyncClose();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void closeOwnsWriterAndManagerAndClosesEachOnlyOnce() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        when(writer.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(manager, writer);

        producer.close();
        producer.close();

        var order = inOrder(writer, manager);
        order.verify(writer).asyncClose();
        order.verify(manager).close();
        verify(writer, times(1)).asyncClose();
        verify(manager, times(1)).close();
    }

    @Test
    void writerFailureDoesNotPreventManagerCloseAndRetainsBothFailures() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        IOException writerFailure = new IOException("writer close failed");
        IOException managerFailure = new IOException("manager close failed");
        when(writer.write(any(LogRecord.class))).thenReturn(new CompletableFuture<>());
        when(writer.asyncClose()).thenReturn(CompletableFuture.failedFuture(writerFailure));
        doThrow(managerFailure).when(manager).close();
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(manager, writer);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        Throwable failure = catchThrowable(producer::close);

        assertThat(failure).isInstanceOf(ExecutionException.class).hasCause(writerFailure);
        assertThat(failure.getSuppressed()).containsExactly(managerFailure);
        assertThatThrownBy(send::join).hasRootCause(writerFailure);
        verify(writer).asyncClose();
        verify(manager).close();
    }

    @Test
    void managerCloseFailureAlsoSettlesOutstandingSends() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        IOException managerFailure = new IOException("manager close failed");
        when(writer.write(any(LogRecord.class))).thenReturn(new CompletableFuture<>());
        when(writer.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        doThrow(managerFailure).when(manager).close();
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(manager, writer);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertThat(catchThrowable(producer::close)).isSameAs(managerFailure);
        assertThatThrownBy(send::join).hasRootCause(managerFailure);
        verify(writer).asyncClose();
        verify(manager).close();
    }

    @Test
    void interruptedWriterCloseStillClosesManagerAndRestoresInterrupt() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        CompletableFuture<Void> writerClose = new CompletableFuture<>();
        when(writer.asyncClose()).thenReturn(writerClose);
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(manager, writer);

        Thread.currentThread().interrupt();
        try {
            assertThat(catchThrowable(producer::close)).isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(writerClose.isCancelled()).isFalse();
            verify(manager).close();
        } finally {
            Thread.interrupted();
            writerClose.complete(null);
        }
    }

    @Test
    void concurrentCloseDoesNotDiscardAcknowledgementsDuringWriterClose() throws Exception {
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        CompletableFuture<DLSN> acknowledgement = new CompletableFuture<>();
        CompletableFuture<Void> writerClose = new CompletableFuture<>();
        CountDownLatch closingWriter = new CountDownLatch(1);
        when(writer.write(any(LogRecord.class))).thenReturn(acknowledgement);
        when(writer.asyncClose())
                .thenAnswer(
                        invocation -> {
                            closingWriter.countDown();
                            return writerClose;
                        });
        DlogBenchmarkProducer producer = new DlogBenchmarkProducer(manager, writer);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        var executor = Executors.newSingleThreadExecutor();
        try {
            var firstClose =
                    executor.submit(
                            () -> {
                                producer.close();
                                return null;
                            });
            assertThat(closingWriter.await(5, TimeUnit.SECONDS)).isTrue();
            producer.close();
            assertThat(send).isNotDone();
            acknowledgement.complete(null);
            assertThat(send).isCompletedWithValue(null);
            writerClose.complete(null);
            firstClose.get(5, TimeUnit.SECONDS);
            verify(writer, times(1)).asyncClose();
            verify(manager, times(1)).close();
        } finally {
            acknowledgement.complete(null);
            writerClose.complete(null);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
