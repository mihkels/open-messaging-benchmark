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
package io.openmessaging.benchmark.driver.nsq;

import com.github.brainlag.nsq.NSQProducer;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NsqBenchmarkProducer implements BenchmarkProducer {
    private final NSQProducer nsqProducer;
    private final String topic;
    private final ExecutorService executor;
    private final Semaphore semaphore;
    private final Object lifecycleLock = new Object();
    private final Set<SendTask> pending = new HashSet<>();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private volatile boolean closed;
    private Thread closingThread;

    public NsqBenchmarkProducer(final NSQProducer nsqProducer, final String topic) {
        this(nsqProducer, topic, Executors.newCachedThreadPool(), 1000);
    }

    NsqBenchmarkProducer(
            NSQProducer nsqProducer, String topic, ExecutorService executor, int maxInFlight) {
        this.nsqProducer = nsqProducer;
        this.topic = topic;
        this.executor = executor;
        this.semaphore = new Semaphore(maxInFlight);
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        if (payload == null) {
            return CompletableFuture.failedFuture(new NullPointerException("payload"));
        }

        boolean acquired = false;
        try {
            while (!closed) {
                acquired = semaphore.tryAcquire(100, TimeUnit.MILLISECONDS);
                if (acquired) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }

        if (closed) {
            if (acquired) {
                semaphore.release();
            }
            return CompletableFuture.failedFuture(new IllegalStateException("NSQ producer is closed"));
        }

        SendTask task;
        try {
            // Cancellation can complete the future while the NSQ client is still using the bytes.
            task = new SendTask(payload.clone());
        } catch (Throwable failure) {
            semaphore.release();
            return CompletableFuture.failedFuture(failure);
        }

        synchronized (lifecycleLock) {
            if (closed) {
                task.fail(new IllegalStateException("NSQ producer is closed"));
            } else {
                pending.add(task);
                try {
                    executor.execute(task);
                } catch (RuntimeException failure) {
                    task.fail(failure);
                }
            }
        }
        return task.future;
    }

    @Override
    public void close() {
        List<SendTask> outstanding = null;
        synchronized (lifecycleLock) {
            if (closed) {
                if (closingThread == Thread.currentThread() && !termination.isDone()) {
                    return;
                }
            } else {
                closed = true;
                closingThread = Thread.currentThread();
                outstanding = List.copyOf(pending);
            }
        }

        if (outstanding == null) {
            awaitTermination();
            return;
        }

        try {
            RuntimeException failure = null;
            try {
                executor.shutdownNow();
            } catch (RuntimeException e) {
                failure = e;
            }
            for (SendTask task : outstanding) {
                task.fail(new IllegalStateException("NSQ producer is closed"));
            }
            try {
                nsqProducer.shutdown();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        } catch (RuntimeException | Error failure) {
            termination.completeExceptionally(failure);
            throw failure;
        } finally {
            termination.complete(null);
        }
    }

    private void awaitTermination() {
        try {
            termination.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure.getCause() instanceof Error error) {
                throw error;
            }
            throw failure;
        }
    }

    private final class SendTask implements Runnable {
        private final byte[] payload;
        private final CompletableFuture<Void> future = new CompletableFuture<>();
        private final AtomicBoolean released = new AtomicBoolean();

        private SendTask(byte[] payload) {
            this.payload = payload;
        }

        @Override
        public void run() {
            Throwable failure = null;
            try {
                if (!future.isDone() && !closed) {
                    nsqProducer.produce(topic, payload);
                    if (closed) {
                        failure = new IllegalStateException("NSQ producer is closed");
                    }
                } else if (!future.isDone()) {
                    failure = new IllegalStateException("NSQ producer is closed");
                }
            } catch (Throwable cause) {
                if (cause instanceof InterruptedException
                        || cause.getCause() instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                failure = cause;
            }
            finish(failure);
            if (failure instanceof Error error) {
                throw error;
            }
        }

        private void fail(Throwable failure) {
            finish(failure);
        }

        private void finish(Throwable failure) {
            if (released.compareAndSet(false, true)) {
                semaphore.release();
            }
            try {
                if (failure == null) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(failure);
                }
            } finally {
                synchronized (lifecycleLock) {
                    pending.remove(this);
                }
            }
        }
    }
}
