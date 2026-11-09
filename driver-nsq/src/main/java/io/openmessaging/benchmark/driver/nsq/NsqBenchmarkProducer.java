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
import io.openmessaging.benchmark.driver.CloseOnce;
import io.openmessaging.benchmark.driver.Failures;
import io.openmessaging.benchmark.driver.PendingSendTracker;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
    private final PendingSendTracker pending = new PendingSendTracker("NSQ producer");
    // The producer's closed state; its close action closes the tracker, which fails the sends
    private final CloseOnce<RuntimeException> lifecycle = new CloseOnce<>();

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
            while (!lifecycle.isClosed()) {
                acquired = semaphore.tryAcquire(100, TimeUnit.MILLISECONDS);
                if (acquired) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }

        if (lifecycle.isClosed()) {
            if (acquired) {
                semaphore.release();
            }
            return CompletableFuture.failedFuture(pending.closedFailure());
        }

        byte[] message;
        try {
            // Cancellation can complete the future while the NSQ client is still using the bytes.
            message = payload.clone();
        } catch (Throwable failure) {
            semaphore.release();
            return CompletableFuture.failedFuture(failure);
        }

        CompletableFuture<Void> future = pending.register();
        if (future.isDone()) {
            semaphore.release();
            return future;
        }
        SendTask task = new SendTask(message, future);
        try {
            executor.execute(task);
        } catch (RuntimeException failure) {
            task.finish(failure);
        }
        return future;
    }

    @Override
    public void close() {
        lifecycle.close(this::shutdownResources);
    }

    private void shutdownResources() {
        Failures failures = new Failures();
        failures.close(executor::shutdownNow);
        // Fails the pending sends before the client shuts down, as the client owns no send
        failures.close(() -> pending.close(() -> {}));
        failures.close(nsqProducer::shutdown);
        failures.throwIfAny(RuntimeException.class);
    }

    private final class SendTask implements Runnable {
        private final byte[] payload;
        private final CompletableFuture<Void> future;
        private final AtomicBoolean released = new AtomicBoolean();

        private SendTask(byte[] payload, CompletableFuture<Void> future) {
            this.payload = payload;
            this.future = future;
        }

        @Override
        public void run() {
            Throwable failure = null;
            try {
                if (!future.isDone() && !lifecycle.isClosed()) {
                    nsqProducer.produce(topic, payload);
                    if (lifecycle.isClosed()) {
                        failure = pending.closedFailure();
                    }
                } else if (!future.isDone()) {
                    failure = pending.closedFailure();
                }
            } catch (Throwable cause) {
                if (Failures.isInterruption(cause)) {
                    Thread.currentThread().interrupt();
                }
                failure = cause;
            }
            finish(failure);
            if (failure instanceof Error error) {
                throw error;
            }
        }

        // Release the permit before completing, so a completion callback can send again
        private void finish(Throwable failure) {
            if (released.compareAndSet(false, true)) {
                semaphore.release();
            }
            if (failure == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(failure);
            }
        }
    }
}
