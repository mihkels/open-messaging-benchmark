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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import dlshade.org.apache.distributedlog.DLSN;
import dlshade.org.apache.distributedlog.LogRecordWithDLSN;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.LogReader;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DlogBenchmarkConsumerTest {
    @Test
    void reconnectsAtTheLastReadDlsnAndPreservesPayloadAndTimestamp() throws Exception {
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader initial = mock(LogReader.class);
        LogReader reopened = mock(LogReader.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        DLSN lastDlsn = new DLSN(7, 3, 1);
        byte[] payload = {1, 2, 3};
        LogRecordWithDLSN record = new LogRecordWithDLSN(lastDlsn, 12345, payload, 0);
        CompletableFuture<Received> received = new CompletableFuture<>();
        CountDownLatch reconnected = new CountDownLatch(1);
        when(initial.readNext(false)).thenReturn(record).thenThrow(new IOException("read failed"));
        when(manager.openLogReader(lastDlsn))
                .thenAnswer(
                        invocation -> {
                            reconnected.countDown();
                            return reopened;
                        });
        doAnswer(
                        invocation -> {
                            received.complete(new Received(invocation.getArgument(0), invocation.getArgument(1)));
                            return null;
                        })
                .when(callback)
                .messageReceived(any(byte[].class), anyLong());
        DlogBenchmarkConsumer consumer = new DlogBenchmarkConsumer(manager, initial, callback);
        try {
            Received message = received.get(5, TimeUnit.SECONDS);
            assertThat(message.payload()).containsExactly(payload);
            assertThat(message.timestamp()).isEqualTo(12345);
            assertThat(reconnected.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            consumer.close();
        }

        verify(manager).openLogReader(lastDlsn);
        verify(initial, times(1)).close();
        verify(reopened, times(1)).close();
        verify(manager, times(1)).close();
    }

    @Test
    void callbackReentrantCloseIsRejectedWithoutStoppingDelivery() throws Exception {
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Throwable> rejected = new CompletableFuture<>();
        CompletableFuture<Void> secondMessage = new CompletableFuture<>();
        AtomicInteger delivered = new AtomicInteger();
        AtomicReference<DlogBenchmarkConsumer> consumerRef = new AtomicReference<>();
        LogRecordWithDLSN first = record(1);
        LogRecordWithDLSN second = record(2);
        when(reader.readNext(false))
                .thenAnswer(
                        invocation -> {
                            awaitUninterruptibly(release);
                            return first;
                        })
                .thenReturn(second)
                .thenReturn(null);
        doAnswer(
                        invocation -> {
                            if (delivered.incrementAndGet() == 1) {
                                rejected.complete(catchThrowable(consumerRef.get()::close));
                            } else {
                                secondMessage.complete(null);
                            }
                            return null;
                        })
                .when(callback)
                .messageReceived(any(byte[].class), anyLong());
        DlogBenchmarkConsumer consumer = new DlogBenchmarkConsumer(manager, reader, callback);
        consumerRef.set(consumer);
        try {
            release.countDown();
            assertThat(rejected.get(5, TimeUnit.SECONDS)).isInstanceOf(IllegalStateException.class);
            secondMessage.get(5, TimeUnit.SECONDS);
            verify(reader, never()).close();
            verify(manager, never()).close();
        } finally {
            release.countDown();
            consumer.close();
        }

        consumer.close();
        verify(reader, times(1)).close();
        verify(manager, times(1)).close();
    }

    @Test
    void readerCloseFailureDoesNotPreventManagerClose() throws Exception {
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        IOException readerFailure = new IOException("reader cleanup failed");
        IOException managerFailure = new IOException("manager cleanup failed");
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(reader.readNext(false))
                .thenAnswer(
                        invocation -> {
                            reading.countDown();
                            awaitUninterruptibly(release);
                            return null;
                        });
        doAnswer(
                        invocation -> {
                            release.countDown();
                            throw readerFailure;
                        })
                .when(reader)
                .close();
        doThrow(managerFailure).when(manager).close();
        DlogBenchmarkConsumer consumer =
                new DlogBenchmarkConsumer(manager, reader, mock(ConsumerCallback.class));
        try {
            assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
            Throwable failure = catchThrowable(consumer::close);
            assertThat(failure).isSameAs(readerFailure);
            assertThat(failure.getSuppressed()).containsExactly(managerFailure);
            verify(reader, times(1)).close();
            verify(manager, times(1)).close();
        } finally {
            release.countDown();
        }
    }

    @Test
    void closePreservesInterruptAndStillClosesBothResources() throws Exception {
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        when(reader.readNext(false))
                .thenAnswer(
                        invocation -> {
                            worker.set(Thread.currentThread());
                            reading.countDown();
                            awaitUninterruptibly(release);
                            return null;
                        });
        DlogBenchmarkConsumer consumer =
                new DlogBenchmarkConsumer(manager, reader, mock(ConsumerCallback.class), 100);
        try {
            assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.currentThread().interrupt();
            Throwable failure = catchThrowable(consumer::close);
            assertThat(failure).isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            Thread.interrupted();
            verify(reader, timeout(5000)).close();
            verify(manager, timeout(5000)).close();
        } finally {
            Thread.interrupted();
            release.countDown();
            if (worker.get() != null) {
                worker.get().join(5000);
            }
        }
    }

    @Test
    void closeBoundsAReaderThatIgnoresInterruptsAndPreventsLateDelivery() throws Exception {
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        when(reader.readNext(false))
                .thenAnswer(
                        invocation -> {
                            worker.set(Thread.currentThread());
                            reading.countDown();
                            awaitUninterruptibly(release);
                            return record(1);
                        });
        DlogBenchmarkConsumer consumer = new DlogBenchmarkConsumer(manager, reader, callback, 50);
        try {
            assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
            Throwable failure = catchThrowable(consumer::close);
            assertThat(failure).isInstanceOf(TimeoutException.class);
            verify(reader).close();
            verify(manager).close();
            release.countDown();
            worker.get().join(5000);
            assertThat(worker.get().isAlive()).isFalse();
            verifyNoInteractions(callback);
        } finally {
            release.countDown();
        }
    }

    @Test
    void blockedReaderCloseIsBoundedAndManagerCloseStillRuns() throws Exception {
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        IOException lateFailure = new IOException("late reader close failed");
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CountDownLatch readerClosing = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        AtomicReference<Thread> cleanup = new AtomicReference<>();
        when(reader.readNext(false))
                .thenAnswer(
                        invocation -> {
                            worker.set(Thread.currentThread());
                            reading.countDown();
                            awaitUninterruptibly(releaseRead);
                            return null;
                        });
        doAnswer(
                        invocation -> {
                            cleanup.set(Thread.currentThread());
                            readerClosing.countDown();
                            awaitUninterruptibly(releaseClose);
                            throw lateFailure;
                        })
                .when(reader)
                .close();
        DlogBenchmarkConsumer consumer =
                new DlogBenchmarkConsumer(manager, reader, mock(ConsumerCallback.class), 50);
        try {
            assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
            Throwable failure = catchThrowable(consumer::close);
            assertThat(failure).isInstanceOf(TimeoutException.class);
            assertThat(readerClosing.await(5, TimeUnit.SECONDS)).isTrue();
            verify(manager, timeout(5000)).close();
            releaseClose.countDown();
            cleanup.get().join(5000);
            assertThat(cleanup.get().isAlive()).isFalse();
            assertThat(failure.getSuppressed()).contains(lateFailure);
            releaseRead.countDown();
            worker.get().join(5000);
            assertThat(worker.get().isAlive()).isFalse();
        } finally {
            releaseClose.countDown();
            releaseRead.countDown();
        }
    }

    @Test
    void blockedManagerCloseIsBoundedAndItsLateFailureIsReported() throws Exception {
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        IOException lateFailure = new IOException("late manager close failed");
        CountDownLatch managerClosing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> cleanup = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            cleanup.set(Thread.currentThread());
                            managerClosing.countDown();
                            awaitUninterruptibly(release);
                            throw lateFailure;
                        })
                .when(manager)
                .close();
        DlogBenchmarkConsumer consumer =
                new DlogBenchmarkConsumer(manager, reader, mock(ConsumerCallback.class), 50);
        try {
            Throwable failure = catchThrowable(consumer::close);
            assertThat(failure).isInstanceOf(TimeoutException.class);
            assertThat(managerClosing.await(5, TimeUnit.SECONDS)).isTrue();
            verify(reader).close();
            release.countDown();
            cleanup.get().join(5000);
            assertThat(cleanup.get().isAlive()).isFalse();
            assertThat(failure.getSuppressed()).contains(lateFailure);
            verify(manager, times(1)).close();
        } finally {
            release.countDown();
        }
    }

    @Test
    void reportsCallbackFailureWithReaderAndManagerCleanupFailures() throws Exception {
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        IllegalStateException callbackFailure = new IllegalStateException("callback failed");
        IOException readerFailure = new IOException("reader cleanup failed");
        IOException managerFailure = new IOException("manager cleanup failed");
        CountDownLatch readerClosing = new CountDownLatch(1);
        when(reader.readNext(false)).thenReturn(record(1));
        doThrow(callbackFailure).when(callback).messageReceived(any(byte[].class), anyLong());
        doAnswer(
                        invocation -> {
                            readerClosing.countDown();
                            throw readerFailure;
                        })
                .when(reader)
                .close();
        doThrow(managerFailure).when(manager).close();
        DlogBenchmarkConsumer consumer = new DlogBenchmarkConsumer(manager, reader, callback);

        assertThat(readerClosing.await(5, TimeUnit.SECONDS)).isTrue();
        Throwable failure = catchThrowable(consumer::close);

        assertThat(failure).isSameAs(callbackFailure);
        assertThat(failure.getSuppressed()).contains(readerFailure, managerFailure);
        verify(reader, times(1)).close();
        verify(manager, times(1)).close();
    }

    @Test
    void interruptedCallbackRemainsThePrimaryFailureAndRestoresInterrupt() throws Exception {
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        InterruptedException callbackFailure = new InterruptedException("callback interrupted");
        IOException readerFailure = new IOException("reader cleanup failed");
        CountDownLatch readerClosing = new CountDownLatch(1);
        when(reader.readNext(false)).thenReturn(record(1));
        doAnswer(
                        invocation -> {
                            throw callbackFailure;
                        })
                .when(callback)
                .messageReceived(any(byte[].class), anyLong());
        doAnswer(
                        invocation -> {
                            readerClosing.countDown();
                            throw readerFailure;
                        })
                .when(reader)
                .close();
        DlogBenchmarkConsumer consumer = new DlogBenchmarkConsumer(manager, reader, callback);
        try {
            assertThat(readerClosing.await(5, TimeUnit.SECONDS)).isTrue();
            Throwable failure = catchThrowable(consumer::close);
            assertThat(failure).isSameAs(callbackFailure);
            assertThat(failure.getSuppressed()).contains(readerFailure);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verify(manager).close();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void lateReconnectAfterTimeoutIsClosedAndItsFailureIsReported() throws Exception {
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader initial = mock(LogReader.class);
        LogReader late = mock(LogReader.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        DLSN lastDlsn = new DLSN(2, 0, 0);
        LogRecordWithDLSN record = new LogRecordWithDLSN(lastDlsn, 12, new byte[] {1}, 0);
        IOException lateFailure = new IOException("late reader cleanup failed");
        CountDownLatch reopening = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        when(initial.readNext(false)).thenReturn(record).thenThrow(new IOException("read failed"));
        when(manager.openLogReader(lastDlsn))
                .thenAnswer(
                        invocation -> {
                            worker.set(Thread.currentThread());
                            reopening.countDown();
                            awaitUninterruptibly(release);
                            return late;
                        });
        doThrow(lateFailure).when(late).close();
        DlogBenchmarkConsumer consumer = new DlogBenchmarkConsumer(manager, initial, callback, 50);
        try {
            assertThat(reopening.await(5, TimeUnit.SECONDS)).isTrue();
            Throwable failure = catchThrowable(consumer::close);
            assertThat(failure).isInstanceOf(TimeoutException.class);
            verify(initial, times(1)).close();
            verify(manager, times(1)).close();
            release.countDown();
            worker.get().join(5000);
            assertThat(worker.get().isAlive()).isFalse();
            verify(late, times(1)).close();
            assertThat(failure.getSuppressed()).contains(lateFailure);
            verify(callback, times(1)).messageReceived(any(byte[].class), anyLong());
        } finally {
            release.countDown();
        }
    }

    private static LogRecordWithDLSN record(long timestamp) {
        return new LogRecordWithDLSN(new DLSN(1, timestamp, 0), timestamp, new byte[] {1}, 0);
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (!latch.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting for test coordination");
                    }
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private record Received(byte[] payload, long timestamp) {}
}
