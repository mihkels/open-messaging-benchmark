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

import dlshade.com.google.common.util.concurrent.ThreadFactoryBuilder;
import dlshade.org.apache.distributedlog.DLSN;
import dlshade.org.apache.distributedlog.LogRecordWithDLSN;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.LogReader;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DlogBenchmarkConsumer implements BenchmarkConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlogBenchmarkConsumer.class);
    private static final long CLOSE_WAIT_MILLIS = TimeUnit.SECONDS.toMillis(5);

    private final DistributedLogManager dlm;
    private final ConsumerCallback callback;
    private final ExecutorService executor;
    private final Future<?> readerTask;
    private final long closeWaitMillis;
    private final Object readerLock = new Object();
    private final Object closeLock = new Object();
    private final DlogFailures failures = new DlogFailures();
    private LogReader reader;
    // Only touched by the reader thread.
    private DLSN lastDLSN = DLSN.InitialDLSN;
    private volatile boolean closing;
    private volatile Thread readerThread;
    private boolean closeStarted;

    public DlogBenchmarkConsumer(
            DistributedLogManager dlm, LogReader initialReader, ConsumerCallback callback) {
        this(dlm, initialReader, callback, CLOSE_WAIT_MILLIS);
    }

    DlogBenchmarkConsumer(
            DistributedLogManager dlm,
            LogReader initialReader,
            ConsumerCallback callback,
            long closeWaitMillis) {
        this.dlm = Objects.requireNonNull(dlm, "log manager");
        this.reader = Objects.requireNonNull(initialReader, "initial log reader");
        this.callback = Objects.requireNonNull(callback, "consumer callback");
        if (closeWaitMillis <= 0) {
            throw new IllegalArgumentException("close wait must be positive");
        }
        this.closeWaitMillis = closeWaitMillis;
        this.executor =
                Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("dlog-benchmark-reader-thread-%d")
                                .setDaemon(true)
                                .build());
        try {
            this.readerTask =
                    executor.submit(() -> {
                        read();
                        return null;
                    });
        } catch (RuntimeException | Error failure) {
            try {
                executor.shutdownNow();
            } catch (RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private void read() {
        readerThread = Thread.currentThread();
        lastDLSN = DLSN.InitialDLSN;
        try {
            boolean keepReading = true;
            while (keepReading && !closing) {
                keepReading = readStep();
            }
        } catch (Exception | Error failure) {
            failures.add(failure);
            if (DlogFailures.isInterrupted(failure)) {
                Thread.currentThread().interrupt();
            }
            throw failure;
        } finally {
            closeReader(takeReader(null));
            readerThread = null;
        }
    }

    // Runs one iteration of the reader loop and returns whether the loop should continue.
    private boolean readStep() {
        LogReader current = currentReader();
        if (current == null) {
            if (closing) {
                return false;
            }
            OpenedReader opened = openReader();
            if (opened.reader() == null) {
                return opened.keepReading();
            }
            current = opened.reader();
        }
        return readRecord(current);
    }

    private OpenedReader openReader() {
        try {
            LogReader opened = Objects.requireNonNull(dlm.openLogReader(lastDLSN), "log reader");
            if (!installReader(opened)) {
                closeReader(opened);
                return OpenedReader.STOP;
            }
            log.info("Successfully open log reader for stream {} at {}", dlm.getStreamName(), lastDLSN);
            return new OpenedReader(opened, true);
        } catch (IOException e) {
            if (!closing) {
                log.error("Failed to open reader of stream {} at {}", dlm.getStreamName(), lastDLSN, e);
                return new OpenedReader(null, backoff(10, TimeUnit.SECONDS));
            }
            return OpenedReader.STOP;
        }
    }

    private boolean readRecord(LogReader current) {
        try {
            LogRecordWithDLSN record = current.readNext(false);
            if (record == null) {
                return backoff(1, TimeUnit.MILLISECONDS);
            }
            if (closing) {
                return false;
            }
            callback.messageReceived(record.getPayload(), record.getTransactionId());
            lastDLSN = record.getDlsn();
        } catch (IOException e) {
            if (!closing) {
                log.info(
                        "Encountered error on reading records from reading stream {}, last record = {}",
                        dlm.getStreamName(),
                        lastDLSN,
                        e);
            }
            closeReader(takeReader(current));
        }
        return true;
    }

    // Outcome of opening a reader: the installed reader, or whether to retry without one.
    private record OpenedReader(LogReader reader, boolean keepReading) {
        private static final OpenedReader STOP = new OpenedReader(null, false);
    }

    private static boolean backoff(long delay, TimeUnit unit) {
        try {
            unit.sleep(delay);
            return true;
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private LogReader currentReader() {
        synchronized (readerLock) {
            return reader;
        }
    }

    private boolean installReader(LogReader opened) {
        synchronized (readerLock) {
            if (closing) {
                return false;
            }
            reader = opened;
            return true;
        }
    }

    private LogReader takeReader(LogReader expected) {
        synchronized (readerLock) {
            if (expected != null && reader != expected) {
                return null;
            }
            LogReader owned = reader;
            reader = null;
            return owned;
        }
    }

    private void closeReader(LogReader owned) {
        closeResource(owned, "reader");
    }

    private void closeResource(AutoCloseable owned, String name) {
        if (owned == null) {
            return;
        }
        try {
            owned.close();
        } catch (Throwable failure) {
            failures.add(failure);
            if (DlogFailures.isInterrupted(failure)) {
                Thread.currentThread().interrupt();
            }
            log.warn("Failed to close a DistributedLog {}", name, failure);
        }
    }

    private CompletableFuture<Void> closeAsync(AutoCloseable owned, String name) {
        if (owned == null) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> completed = new CompletableFuture<>();
        try {
            // DistributedLog close can wait on a monitor held by the reader thread.
            Thread.ofVirtual()
                    .name("dlog-benchmark-" + name + "-close")
                    .start(() -> {
                        try {
                            closeResource(owned, name);
                        } finally {
                            completed.complete(null);
                        }
                    });
        } catch (RuntimeException | Error failure) {
            failures.add(failure);
            completed.complete(null);
        }
        return completed;
    }

    private void await(Future<?> task, long deadline) throws InterruptedException, TimeoutException {
        try {
            task.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (ExecutionException failure) {
            failures.add(failure.getCause() == null ? failure : failure.getCause());
        } catch (CancellationException failure) {
            failures.add(failure);
        }
    }

    @Override
    public void close() throws Exception {
        if (Thread.currentThread() == readerThread) {
            throw new IllegalStateException(
                    "Cannot close a DistributedLog consumer from its own reader callback");
        }
        synchronized (closeLock) {
            if (closeStarted) {
                failures.throwIfAny();
                return;
            }
            closeStarted = true;
            synchronized (readerLock) {
                closing = true;
            }
            try {
                executor.shutdown();
                Thread thread = readerThread;
                if (thread != null) {
                    thread.interrupt();
                }
            } catch (RuntimeException | Error failure) {
                failures.add(failure);
            }
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(closeWaitMillis);
            boolean stopWaiting = false;
            CompletableFuture<Void> readerClose = closeAsync(takeReader(null), "reader");
            try {
                await(readerClose, deadline);
            } catch (InterruptedException | TimeoutException failure) {
                failures.add(failure);
                stopWaiting = true;
            }
            CompletableFuture<Void> managerClose = closeAsync(dlm, "manager");
            if (!stopWaiting) {
                try {
                    await(managerClose, deadline);
                    await(readerTask, deadline);
                } catch (InterruptedException | TimeoutException failure) {
                    failures.add(failure);
                    stopWaiting = true;
                }
            }
            if (stopWaiting) {
                try {
                    executor.shutdownNow();
                } catch (RuntimeException | Error cleanupFailure) {
                    failures.add(cleanupFailure);
                }
            }
            failures.throwIfAny();
        }
    }
}
