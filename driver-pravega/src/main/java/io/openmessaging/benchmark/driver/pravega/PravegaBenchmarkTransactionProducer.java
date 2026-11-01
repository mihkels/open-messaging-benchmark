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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaBenchmarkTransactionProducer implements BenchmarkProducer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkProducer.class);

    private final TransactionalEventStreamWriter<ByteBuffer> transactionWriter;

    private final boolean includeTimestampInEvent;

    // If null, a transaction has not been started.
    @GuardedBy("this")
    private Transaction<ByteBuffer> transaction;

    private final int eventsPerTransaction;
    private int eventCount = 0;
    private boolean closed;

    public PravegaBenchmarkTransactionProducer(
            String streamName,
            EventStreamClientFactory clientFactory,
            boolean includeTimestampInEvent,
            boolean enableConnectionPooling,
            int eventsPerTransaction) {
        log.info("PravegaBenchmarkProducer: BEGIN: streamName={}", streamName);

        final String writerId = UUID.randomUUID().toString();
        transactionWriter =
                clientFactory.createTransactionalEventWriter(
                        writerId,
                        streamName,
                        new ByteBufferSerializer(),
                        EventWriterConfig.builder().enableConnectionPooling(enableConnectionPooling).build());
        this.eventsPerTransaction = eventsPerTransaction;
        this.includeTimestampInEvent = includeTimestampInEvent;
    }

    @Override
    public synchronized CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("Producer is closed"));
        }
        try {
            if (transaction == null) {
                transaction = transactionWriter.beginTxn();
            }
            if (this.probeRequested(
                    key)) { // Populate probe transaction with the sufficient amount of events.
                // Probe events must use the same encoding as regular events: readers expect the timestamp
                while (eventCount < this.eventsPerTransaction - 1) {
                    writeEvent(key, PravegaMessageEncoder.encode(payload, includeTimestampInEvent));
                    eventCount++;
                }
            }
            writeEvent(key, PravegaMessageEncoder.encode(payload, includeTimestampInEvent));
            if (++eventCount >= eventsPerTransaction) {
                eventCount = 0;
                transaction.commit();
                transaction = null;
            }
        } catch (Exception e) {
            // Pravega can propagate checked controller failures from beginTxn without declaring them.
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            Transaction<ByteBuffer> failed = transaction;
            transaction = null;
            eventCount = 0;
            if (failed != null) {
                try {
                    failed.abort();
                } catch (Exception abortFailure) {
                    if (abortFailure instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    if (abortFailure != e) {
                        e.addSuppressed(abortFailure);
                    }
                }
            }
            return CompletableFuture.failedFuture(e);
        }
        return CompletableFuture.completedFuture(null);
    }

    private void writeEvent(Optional<String> key, ByteBuffer payload) throws TxnFailedException {
        if (key.isPresent()) {
            transaction.writeEvent(key.get(), payload);
        } else {
            transaction.writeEvent(payload);
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        try (transactionWriter) {
            if (transaction != null) {
                Transaction<ByteBuffer> open = transaction;
                transaction = null;
                open.abort();
            }
        }
    }

    /**
     * Indicates if producer probe had been requested by OpenMessaging benchmark.
     *
     * @param key - key provided to the probe.
     * @return true in case requested event had been created in context of producer probe.
     */
    private boolean probeRequested(Optional<String> key) {
        // For the expected key, see: LocalWorker.probeProducers()
        final String expectedKey = "key";
        return key.isPresent() && key.get().equals(expectedKey);
    }
}
