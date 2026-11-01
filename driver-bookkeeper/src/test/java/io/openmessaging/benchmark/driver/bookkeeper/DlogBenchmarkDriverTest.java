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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dlshade.org.apache.distributedlog.DLSN;
import dlshade.org.apache.distributedlog.api.AsyncLogWriter;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.LogReader;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreationCompletion;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DlogBenchmarkDriverTest {
    private static final String TOPIC = "stream";

    @Test
    void producerCreationWaitsForWriterAndTransfersBothOwnedResources() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        CompletableFuture<AsyncLogWriter> opening = new CompletableFuture<>();
        CountDownLatch openingStarted = new CountDownLatch(1);
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openAsyncLogWriter())
                .thenAnswer(
                        invocation -> {
                            openingStarted.countDown();
                            return opening;
                        });
        when(writer.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        DlogBenchmarkDriver driver = new DlogBenchmarkDriver(namespace);

        CompletableFuture<BenchmarkProducer> creation = driver.createProducer(TOPIC);
        assertThat(openingStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(creation).isNotDone();
        opening.complete(writer);
        BenchmarkProducer producer = creation.get(5, TimeUnit.SECONDS);
        verify(manager, never()).close();
        verify(writer, never()).asyncClose();

        producer.close();
        producer.close();
        verify(writer, times(1)).asyncClose();
        verify(manager, times(1)).close();
        verify(namespace, never()).close();
        driver.close();
        verify(namespace).close();
    }

    @Test
    void consumerCreationWaitsForItsInitialReaderAtInitialDlsn() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        CountDownLatch openingStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openLogReader(DLSN.InitialDLSN))
                .thenAnswer(
                        invocation -> {
                            openingStarted.countDown();
                            await(release);
                            return reader;
                        });
        DlogBenchmarkDriver driver = new DlogBenchmarkDriver(namespace);
        CompletableFuture<BenchmarkConsumer> creation =
                driver.createConsumer(TOPIC, "ignored", mock(ConsumerCallback.class));
        try {
            assertThat(openingStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation).isNotDone();
            verify(manager).openLogReader(DLSN.InitialDLSN);
            release.countDown();
            BenchmarkConsumer consumer = creation.get(5, TimeUnit.SECONDS);
            verify(reader, never()).close();
            verify(manager, never()).close();

            consumer.close();
            verify(reader).close();
            verify(manager).close();
            verify(namespace, never()).close();
            driver.close();
            verify(namespace).close();
        } finally {
            release.countDown();
        }
    }

    @Test
    void readerOpenFailureIsExceptionalAndRetainsManagerCleanupFailure() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        IOException openFailure = new IOException("cannot open initial reader");
        IOException cleanupFailure = new IOException("cannot close manager");
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openLogReader(DLSN.InitialDLSN)).thenThrow(openFailure);
        doThrow(cleanupFailure).when(manager).close();

        CompletableFuture<BenchmarkConsumer> creation =
                new DlogBenchmarkDriver(namespace)
                        .createConsumer(TOPIC, "ignored", mock(ConsumerCallback.class));

        assertThatThrownBy(() -> quiescence(creation).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(cleanupFailure);
        assertThatThrownBy(creation::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(openFailure);
        assertThat(openFailure.getSuppressed()).contains(cleanupFailure);
        verify(manager).close();
    }

    @Test
    void writerOpenFailureIsExceptionalAndClosesManager() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        IOException openFailure = new IOException("cannot open writer");
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openAsyncLogWriter()).thenReturn(CompletableFuture.failedFuture(openFailure));

        CompletableFuture<BenchmarkProducer> creation =
                new DlogBenchmarkDriver(namespace).createProducer(TOPIC);

        quiescence(creation).get(5, TimeUnit.SECONDS);
        assertThatThrownBy(creation::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(openFailure);
        verify(manager).close();
    }

    @Test
    void managerOpenFailureIsReportedByBothCreationMethods() throws Exception {
        Namespace namespace = mock(Namespace.class);
        IOException openFailure = new IOException("cannot open stream");
        when(namespace.openLog(TOPIC)).thenThrow(openFailure);
        DlogBenchmarkDriver driver = new DlogBenchmarkDriver(namespace);

        CompletableFuture<BenchmarkProducer> producer = driver.createProducer(TOPIC);
        CompletableFuture<BenchmarkConsumer> consumer =
                driver.createConsumer(TOPIC, "ignored", mock(ConsumerCallback.class));

        quiescence(producer).get(5, TimeUnit.SECONDS);
        quiescence(consumer).get(5, TimeUnit.SECONDS);
        assertThatThrownBy(producer::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(openFailure);
        assertThatThrownBy(consumer::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(openFailure);
    }

    @Test
    void nullInitialReaderFailsCreationAndClosesManager() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openLogReader(DLSN.InitialDLSN)).thenReturn(null);

        CompletableFuture<BenchmarkConsumer> creation =
                new DlogBenchmarkDriver(namespace)
                        .createConsumer(TOPIC, "ignored", mock(ConsumerCallback.class));

        quiescence(creation).get(5, TimeUnit.SECONDS);
        assertThatThrownBy(creation::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(NullPointerException.class);
        verify(manager).close();
    }

    @Test
    void synchronousWriterOpenFailureClosesManager() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        IllegalStateException openFailure = new IllegalStateException("writer setup failed");
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openAsyncLogWriter()).thenThrow(openFailure);

        CompletableFuture<BenchmarkProducer> creation =
                new DlogBenchmarkDriver(namespace).createProducer(TOPIC);

        quiescence(creation).get(5, TimeUnit.SECONDS);
        assertThatThrownBy(creation::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(openFailure);
        verify(manager).close();
    }

    @Test
    void nullWriterFutureFailsCreationAndClosesManager() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openAsyncLogWriter()).thenReturn(null);

        CompletableFuture<BenchmarkProducer> creation =
                new DlogBenchmarkDriver(namespace).createProducer(TOPIC);

        quiescence(creation).get(5, TimeUnit.SECONDS);
        assertThatThrownBy(creation::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(NullPointerException.class);
        verify(manager).close();
    }

    @Test
    void producerConstructionFailureClosesBothResourcesAndSuppressesCleanupFailures()
            throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        IllegalStateException constructionFailure = new IllegalStateException("no last transaction");
        IOException managerFailure = new IOException("manager cleanup failed");
        IOException writerFailure = new IOException("writer cleanup failed");
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openAsyncLogWriter()).thenReturn(CompletableFuture.completedFuture(writer));
        when(writer.getLastTxId()).thenThrow(constructionFailure);
        doThrow(managerFailure).when(manager).close();
        when(writer.asyncClose()).thenReturn(CompletableFuture.failedFuture(writerFailure));

        CompletableFuture<BenchmarkProducer> creation =
                new DlogBenchmarkDriver(namespace).createProducer(TOPIC);

        assertThatThrownBy(() -> quiescence(creation).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);
        assertThatThrownBy(creation::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(constructionFailure);
        assertThat(constructionFailure.getSuppressed()).hasSize(2).contains(managerFailure);
        assertThat(constructionFailure.getSuppressed())
                .anySatisfy(
                        failure -> {
                            assertThat(failure).isInstanceOf(ExecutionException.class).hasCause(writerFailure);
                        });
        verify(manager).close();
        verify(writer).asyncClose();
    }

    @Test
    void consumerConstructionFailureClosesManagerAndInitialReader() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openLogReader(DLSN.InitialDLSN)).thenReturn(reader);

        CompletableFuture<BenchmarkConsumer> creation =
                new DlogBenchmarkDriver(namespace).createConsumer(TOPIC, "ignored", null);

        quiescence(creation).get(5, TimeUnit.SECONDS);
        assertThatThrownBy(creation::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(NullPointerException.class);
        verify(reader).close();
        verify(manager).close();
    }

    @Test
    void consumerConstructionFailureRetainsBothCleanupFailures() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        IOException managerFailure = new IOException("manager cleanup failed");
        IOException readerFailure = new IOException("reader cleanup failed");
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openLogReader(DLSN.InitialDLSN)).thenReturn(reader);
        doThrow(managerFailure).when(manager).close();
        doThrow(readerFailure).when(reader).close();

        CompletableFuture<BenchmarkConsumer> creation =
                new DlogBenchmarkDriver(namespace).createConsumer(TOPIC, "ignored", null);

        assertThatThrownBy(() -> quiescence(creation).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);
        Throwable failure = catchThrowable(creation::join).getCause();
        assertThat(failure).isInstanceOf(NullPointerException.class);
        assertThat(failure.getSuppressed()).containsExactly(managerFailure, readerFailure);
        verify(manager).close();
        verify(reader).close();
    }

    @Test
    void cancellationBeforeManagerOpensClosesTheLateManagerAndWriter() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        CountDownLatch managerOpening = new CountDownLatch(1);
        CountDownLatch releaseManager = new CountDownLatch(1);
        CountDownLatch writerOpening = new CountDownLatch(1);
        CompletableFuture<AsyncLogWriter> writerResult = new CompletableFuture<>();
        when(namespace.openLog(TOPIC))
                .thenAnswer(
                        invocation -> {
                            managerOpening.countDown();
                            await(releaseManager);
                            return manager;
                        });
        when(manager.openAsyncLogWriter())
                .thenAnswer(
                        invocation -> {
                            writerOpening.countDown();
                            return writerResult;
                        });
        when(writer.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<BenchmarkProducer> creation =
                new DlogBenchmarkDriver(namespace).createProducer(TOPIC);
        try {
            assertThat(managerOpening.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(true)).isTrue();
            assertThat(quiescence(creation)).isNotDone();
            releaseManager.countDown();
            assertThat(writerOpening.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(writerResult.isCancelled()).isFalse();
            writerResult.complete(writer);
            quiescence(creation).get(5, TimeUnit.SECONDS);

            assertThat(creation).isCancelled();
            verify(manager, times(1)).close();
            verify(writer, times(1)).asyncClose();
        } finally {
            releaseManager.countDown();
            writerResult.complete(writer);
        }
    }

    @Test
    void cancellationDuringWriterOpenClosesManagerAndLateWriter() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        CountDownLatch openingStarted = new CountDownLatch(1);
        AtomicReference<Thread> closingThread = new AtomicReference<>();
        CompletableFuture<AsyncLogWriter> opening = new CompletableFuture<>();
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openAsyncLogWriter())
                .thenAnswer(
                        invocation -> {
                            openingStarted.countDown();
                            return opening;
                        });
        when(writer.asyncClose())
                .thenAnswer(
                        invocation -> {
                            closingThread.set(Thread.currentThread());
                            return CompletableFuture.completedFuture(null);
                        });
        CompletableFuture<BenchmarkProducer> creation =
                new DlogBenchmarkDriver(namespace).createProducer(TOPIC);
        try {
            assertThat(openingStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(false)).isTrue();
            assertThat(opening.isCancelled()).isFalse();
            assertThat(quiescence(creation)).isNotDone();
            opening.complete(writer);
            quiescence(creation).get(5, TimeUnit.SECONDS);

            verify(manager, times(1)).close();
            verify(writer, times(1)).asyncClose();
            assertThat(closingThread.get()).isNotSameAs(Thread.currentThread());
        } finally {
            opening.complete(writer);
        }
    }

    @Test
    void cancellationDuringProducerConstructionClosesTheCompletedHandle() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        CountDownLatch constructing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<AsyncLogWriter> writerOpen = new CompletableFuture<>();
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openAsyncLogWriter()).thenReturn(writerOpen);
        when(writer.getLastTxId())
                .thenAnswer(
                        invocation -> {
                            constructing.countDown();
                            await(release);
                            return 0L;
                        });
        when(writer.asyncClose()).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<BenchmarkProducer> creation =
                new DlogBenchmarkDriver(namespace).createProducer(TOPIC);
        CompletableFuture<Void> completing =
                CompletableFuture.runAsync(() -> writerOpen.complete(writer));
        try {
            assertThat(constructing.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(false)).isTrue();
            assertThat(quiescence(creation)).isNotDone();
            release.countDown();
            completing.get(5, TimeUnit.SECONDS);
            quiescence(creation).get(5, TimeUnit.SECONDS);

            verify(writer, times(1)).asyncClose();
            verify(manager, times(1)).close();
        } finally {
            release.countDown();
        }
    }

    @Test
    void lateWriterCleanupFailureIsReportedByCancellationQuiescence() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        AsyncLogWriter writer = mock(AsyncLogWriter.class);
        IOException cleanupFailure = new IOException("late writer cleanup failed");
        CountDownLatch openingStarted = new CountDownLatch(1);
        CompletableFuture<AsyncLogWriter> opening = new CompletableFuture<>();
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openAsyncLogWriter())
                .thenAnswer(
                        invocation -> {
                            openingStarted.countDown();
                            return opening;
                        });
        when(writer.asyncClose()).thenReturn(CompletableFuture.failedFuture(cleanupFailure));
        CompletableFuture<BenchmarkProducer> creation =
                new DlogBenchmarkDriver(namespace).createProducer(TOPIC);
        try {
            assertThat(openingStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(false)).isTrue();
            opening.complete(writer);
            assertThatThrownBy(() -> quiescence(creation).get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCause(cleanupFailure);

            verify(manager, times(1)).close();
            verify(writer, times(1)).asyncClose();
        } finally {
            opening.complete(writer);
        }
    }

    @Test
    void cancellationDuringReaderOpenClosesManagerAndLateReader() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        CountDownLatch openingStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openLogReader(DLSN.InitialDLSN))
                .thenAnswer(
                        invocation -> {
                            openingStarted.countDown();
                            await(release);
                            return reader;
                        });
        CompletableFuture<BenchmarkConsumer> creation =
                new DlogBenchmarkDriver(namespace).createConsumer(TOPIC, "ignored", callback);
        try {
            assertThat(openingStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(true)).isTrue();
            assertThat(quiescence(creation)).isNotDone();
            release.countDown();
            quiescence(creation).get(5, TimeUnit.SECONDS);

            verify(manager, times(1)).close();
            verify(reader, times(1)).close();
            verify(callback, never()).messageReceived(any(byte[].class), anyLong());
        } finally {
            release.countDown();
        }
    }

    @Test
    void lateReaderCleanupFailureIsReportedByCancellationQuiescence() throws Exception {
        Namespace namespace = mock(Namespace.class);
        DistributedLogManager manager = mock(DistributedLogManager.class);
        LogReader reader = mock(LogReader.class);
        IOException cleanupFailure = new IOException("late reader cleanup failed");
        CountDownLatch openingStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(namespace.openLog(TOPIC)).thenReturn(manager);
        when(manager.openLogReader(DLSN.InitialDLSN))
                .thenAnswer(
                        invocation -> {
                            openingStarted.countDown();
                            await(release);
                            return reader;
                        });
        doThrow(cleanupFailure).when(reader).close();
        CompletableFuture<BenchmarkConsumer> creation =
                new DlogBenchmarkDriver(namespace)
                        .createConsumer(TOPIC, "ignored", mock(ConsumerCallback.class));
        try {
            assertThat(openingStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(false)).isTrue();
            release.countDown();
            assertThatThrownBy(() -> quiescence(creation).get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCause(cleanupFailure);

            verify(manager, times(1)).close();
            verify(reader, times(1)).close();
        } finally {
            release.countDown();
        }
    }

    private static CompletableFuture<Void> quiescence(CompletableFuture<?> creation) {
        return ((ResourceCreationCompletion) creation).quiescence();
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("timed out waiting for test coordination");
        }
    }
}
