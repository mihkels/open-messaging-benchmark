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

import dlshade.org.apache.distributedlog.LogRecord;
import dlshade.org.apache.distributedlog.api.AsyncLogWriter;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.util.TimeSequencer;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PendingSendTracker;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public final class DlogBenchmarkProducer implements BenchmarkProducer {

    private final AsyncLogWriter writer;
    private final DistributedLogManager manager;
    private final TimeSequencer sequencer = new TimeSequencer();
    private final Object lifecycleLock = new Object();
    private final PendingSendTracker pending = new PendingSendTracker("DistributedLog producer");

    public DlogBenchmarkProducer(AsyncLogWriter writer) {
        this(null, writer);
    }

    public DlogBenchmarkProducer(DistributedLogManager manager, AsyncLogWriter writer) {
        this.manager = manager;
        this.writer = Objects.requireNonNull(writer, "writer");
        sequencer.setLastId(writer.getLastTxId());
    }

    @Override
    public void close() throws Exception {
        if (!pending.beginClose()) {
            return;
        }
        DlogFailures failures = new DlogFailures();
        try {
            AsyncLogWriter writerToClose;
            synchronized (lifecycleLock) {
                // Wait for a write that started before close to finish using the writer.
                writerToClose = writer;
            }
            writerToClose.asyncClose().get();
        } catch (Throwable failure) {
            failures.add(failure);
        }
        try {
            if (manager != null) {
                manager.close();
            }
        } catch (Throwable failure) {
            failures.add(failure);
        }
        pending.failRemaining(failures.first());
        failures.throwIfAny();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        CompletableFuture<Void> send;
        synchronized (lifecycleLock) {
            send = pending.register();
            if (send.isDone()) {
                return send;
            }
            try {
                LogRecord record = new LogRecord(sequencer.nextId(), payload.clone());
                writer
                        .write(record)
                        .thenApply(dlsn -> (Void) null)
                        .whenComplete((unused, failure) -> pending.completeFromClient(send, failure));
            } catch (Exception e) {
                send.completeExceptionally(e);
                if (DlogFailures.isInterrupted(e)) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return send;
    }
}
