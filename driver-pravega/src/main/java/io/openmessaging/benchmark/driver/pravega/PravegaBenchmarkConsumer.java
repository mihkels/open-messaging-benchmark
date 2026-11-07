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

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PravegaBenchmarkConsumer implements BenchmarkConsumer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkConsumer.class);

    private final ExecutorService executor;
    private final EventStreamReader<ByteBuffer> reader;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object closeLock = new Object();
    private volatile Thread readerThread;
    private Exception closeFailure;

    /**
     * Constructs a consumer from owned resources.
     *
     * @param reader The Pravega event stream reader.
     * @param executor The executor service to run the reading task.
     */
    PravegaBenchmarkConsumer(EventStreamReader<ByteBuffer> reader, ExecutorService executor) {
        this.reader = reader;
        this.executor = executor;
    }

    /**
     * Factory method to safely create and initialize a PravegaBenchmarkConsumer.
     *
     * @param streamName The name of the stream to consume from.
     * @param scopeName The name of the scope.
     * @param subscriptionName The name of the subscription (reader group).
     * @param consumerCallback The callback to invoke for each message received.
     * @param clientFactory The Pravega client factory.
     * @param readerGroupManager The manager for reader groups.
     * @param includeTimestampInEvent Whether to expect a timestamp in the event payload.
     * @return A new, initialized {@link PravegaBenchmarkConsumer} instance.
     */
    public static PravegaBenchmarkConsumer create(
            String streamName,
            String scopeName,
            String subscriptionName,
            ConsumerCallback consumerCallback,
            EventStreamClientFactory clientFactory,
            ReaderGroupManager readerGroupManager,
            boolean includeTimestampInEvent) {
        log.info(
                "PravegaBenchmarkConsumer: BEGIN: subscriptionName={}, streamName={}",
                subscriptionName,
                streamName);

        // Create reader group if it doesn't already exist.
        final ReaderGroupConfig readerGroupConfig =
                ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        readerGroupManager.createReaderGroup(subscriptionName, readerGroupConfig);

        // Create reader.
        final EventStreamReader<ByteBuffer> reader =
                clientFactory.createReader(
                        UUID.randomUUID().toString(),
                        subscriptionName,
                        new ByteBufferSerializer(),
                        ReaderConfig.builder().disableTimeWindows(true).build());

        // Create the executor and consumer instance *after* all risky operations are done.
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        PravegaBenchmarkConsumer consumer = new PravegaBenchmarkConsumer(reader, executor);

        // Start a thread to read events.
        executor.submit(() -> consumer.readEvents(consumerCallback, includeTimestampInEvent));
        return consumer;
    }

    private void readEvents(ConsumerCallback consumerCallback, boolean includeTimestampInEvent) {
        readerThread = Thread.currentThread();
        try {
            boolean keepReading = true;
            while (keepReading && !closed.get()) {
                keepReading = readNextEvent(consumerCallback, includeTimestampInEvent);
            }
        } finally {
            readerThread = null;
        }
    }

    private boolean readNextEvent(
            ConsumerCallback consumerCallback, boolean includeTimestampInEvent) {
        try {
            final ByteBuffer event = reader.readNextEvent(1000).getEvent();
            if (event != null && !closed.get()) {
                consumerCallback.messageReceived(event, eventTimestamp(event, includeTimestampInEvent));
            }
            return true;
        } catch (ReinitializationRequiredException e) {
            // The reader group was reset, so this reader can never read again. Nothing
            // inspects the task's future, so stop here rather than rethrowing into it.
            log.error("Reader group requires reinitialization; stopping this reader", e);
            return false;
        } catch (RuntimeException e) {
            // The task's future is never inspected, so an escaping exception would stop
            // this reader silently and strand its segments for the rest of the run.
            log.error("Failed to process event; continuing to read", e);
            return true;
        }
    }

    private static long eventTimestamp(ByteBuffer event, boolean includeTimestampInEvent) {
        if (includeTimestampInEvent) {
            return event.getLong();
        }
        // This will result in an invalid end-to-end latency measurement of 0 seconds.
        return TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE);
    }

    @Override
    public void close() throws Exception {
        if (Thread.currentThread() == readerThread) {
            throw new IllegalStateException("Cannot close a Pravega consumer from its own callback");
        }
        synchronized (closeLock) {
            if (closed.getAndSet(true)) {
                rethrowPreviousCloseFailure();
                return;
            }
            CloseOutcome outcome = new CloseOutcome();
            shutdownExecutor(outcome);
            closeReader(outcome);
            closeFailure = outcome.failure;
            if (outcome.interrupted) {
                Thread.currentThread().interrupt();
            }
            if (outcome.failure != null) {
                throw outcome.failure;
            }
        }
    }

    private void rethrowPreviousCloseFailure() throws Exception {
        if (closeFailure != null) {
            if (closeFailure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw closeFailure;
        }
    }

    private void shutdownExecutor(CloseOutcome outcome) {
        try {
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                outcome.add(new TimeoutException("Timed out waiting for the Pravega reader to stop"));
                executor.shutdownNow();
            }
        } catch (Exception shutdownFailure) {
            outcome.add(shutdownFailure);
            if (shutdownFailure instanceof InterruptedException) {
                outcome.interrupted = true;
                forceShutdownExecutor(outcome);
            }
        }
    }

    private void forceShutdownExecutor(CloseOutcome outcome) {
        try {
            executor.shutdownNow();
        } catch (RuntimeException additional) {
            outcome.add(additional);
        }
    }

    private void closeReader(CloseOutcome outcome) {
        try {
            reader.close();
        } catch (Exception readerFailure) {
            outcome.interrupted |= readerFailure instanceof InterruptedException;
            outcome.add(readerFailure);
        }
    }

    /** Collects the first close failure and suppresses later distinct ones onto it. */
    private static final class CloseOutcome {
        private Exception failure;
        private boolean interrupted;

        private void add(Exception additional) {
            if (failure == null) {
                failure = additional;
            } else if (failure != additional) {
                failure.addSuppressed(additional);
            }
        }
    }
}
