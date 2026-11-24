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
package io.github.mihkels.omb.driver.kafka;

import io.github.mihkels.omb.driver.AbandonableCreation;
import io.github.mihkels.omb.driver.BenchmarkConsumer;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.RebalanceEvent;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBenchmarkConsumer implements BenchmarkConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaBenchmarkConsumer.class);
    private static final String CONSUME_FAILURE_MESSAGE = "exception occur while consuming message";

    private enum StartupState {
        WAITING,
        HANDED_OFF,
        FAILED,
        CANCELLED
    }

    private final KafkaConsumer<String, byte[]> consumer;
    private final Object lifecycleLock = new Object();
    private final ExecutorService executor;
    private final Future<?> consumerTask;
    private final @Nullable Readiness readiness;
    private final CreationFuture creationFuture = new CreationFuture(this);
    private StartupState startupState;
    private @Nullable Throwable startupFailure;
    private @Nullable Exception closeFailure;
    private volatile boolean closing = false;
    private volatile @Nullable Thread consumerThread;
    private final boolean autoCommit;

    /**
     * Subscribes a consumer and transfers it only once its group assignment and initial positions are
     * ready. Cancellation closes the untransferred consumer.
     *
     * @param consumer the client to subscribe
     * @param topic the topic to consume
     * @param consumerConfig consumer configuration
     * @param callback message callback
     * @param pollTimeoutMs poll timeout in milliseconds
     * @return the future for the ready consumer
     */
    public static CompletableFuture<BenchmarkConsumer> create(
            KafkaConsumer<String, byte[]> consumer,
            String topic,
            Properties consumerConfig,
            ConsumerCallback callback,
            long pollTimeoutMs) {
        Readiness readiness = new Readiness(topic, callback);
        consumer.subscribe(Set.of(topic), readiness);
        return new KafkaBenchmarkConsumer(consumer, consumerConfig, callback, pollTimeoutMs, readiness)
                .creation();
    }

    // Handing out the creation future is the point; a method keeps SpotBugs from reading it as a leak
    private CreationFuture creation() {
        return creationFuture;
    }

    // Tests only: skips the readiness handshake that create() performs
    KafkaBenchmarkConsumer(
            KafkaConsumer<String, byte[]> consumer,
            Properties consumerConfig,
            ConsumerCallback callback) {
        this(consumer, consumerConfig, callback, 100L);
    }

    KafkaBenchmarkConsumer(
            KafkaConsumer<String, byte[]> consumer,
            Properties consumerConfig,
            ConsumerCallback callback,
            long pollTimeoutMs) {
        this(consumer, consumerConfig, callback, pollTimeoutMs, null);
    }

    private KafkaBenchmarkConsumer(
            KafkaConsumer<String, byte[]> consumer,
            Properties consumerConfig,
            ConsumerCallback callback,
            long pollTimeoutMs,
            @Nullable Readiness readiness) {
        this.consumer = consumer;
        this.readiness = readiness;
        this.startupState = readiness == null ? StartupState.HANDED_OFF : StartupState.WAITING;
        this.autoCommit =
                Boolean.parseBoolean(
                        (String)
                                consumerConfig.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"));
        this.executor =
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("kafka-consumer").factory());
        try {
            this.consumerTask =
                    this.executor.submit(() -> {
                        consumerThread = Thread.currentThread();
                        try {
                            consume(callback, pollTimeoutMs);
                        } catch (RuntimeException | Error error) {
                            synchronized (lifecycleLock) {
                                if (startupState == StartupState.WAITING) {
                                    startupState = StartupState.FAILED;
                                    startupFailure = error;
                                    closing = true;
                                }
                            }
                            throw error;
                        } finally {
                            finishConsumer();
                        }
                    });
        } catch (Throwable error) {
            this.executor.shutdownNow();
            throw error;
        }
    }

    @Override
    public void close() throws Exception {
        boolean wakeup = false;
        synchronized (lifecycleLock) {
            if (!closing) {
                closing = true;
                wakeup = true;
                if (startupState == StartupState.WAITING) {
                    startupState = StartupState.FAILED;
                    startupFailure = new IllegalStateException("Kafka consumer closed before it was ready");
                }
            }
        }
        if (wakeup) {
            wakeup();
        }
        if (Thread.currentThread() == consumerThread) {
            return;
        }
        try {
            consumerTask.get();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw error;
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private void consume(ConsumerCallback callback, long pollTimeoutMs) {
        while (!closing) {
            ConsumerRecords<String, byte[]> records = poll(pollTimeoutMs);
            if (records != null) {
                deliver(records, callback);
                checkReadiness();
            }
        }
    }

    // Polls once; returns null when the poll failed and the iteration should be skipped.
    private @Nullable ConsumerRecords<String, byte[]> poll(long pollTimeoutMs) {
        try {
            return consumer.poll(Duration.ofMillis(pollTimeoutMs));
        } catch (Exception error) {
            if (!closing) {
                handlePollFailure(error);
            }
            return null;
        }
    }

    private void deliver(ConsumerRecords<String, byte[]> records, ConsumerCallback callback) {
        try {
            Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
            for (ConsumerRecord<String, byte[]> consumerRecord : records) {
                if (closing) {
                    break;
                }
                long timestamp = consumerRecord.timestamp();
                long publishTimestamp = timestamp < 0 ? ConsumerCallback.NO_PUBLISH_TIMESTAMP : timestamp;
                callback.messageReceived(
                        consumerRecord.value(), publishTimestamp, consumerRecord.partition());
                offsetMap.put(
                        new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                        new OffsetAndMetadata(consumerRecord.offset() + 1));
            }
            if (!closing && !autoCommit && !offsetMap.isEmpty()) {
                consumer.commitAsync(offsetMap, null);
            }
        } catch (Exception error) {
            log.error(CONSUME_FAILURE_MESSAGE, error);
        }
    }

    private void checkReadiness() {
        Readiness readiness = this.readiness;
        if (!closing && readiness != null) {
            try {
                completeWhenReady(readiness);
            } catch (Exception error) {
                handlePollFailure(error);
            }
        }
    }

    private void completeWhenReady(Readiness readiness) {
        if (!readiness.assigned) {
            return;
        }
        synchronized (lifecycleLock) {
            if (startupState != StartupState.WAITING || closing) {
                return;
            }
        }
        try {
            Set<TopicPartition> assignment = consumer.assignment();
            if (assignment.isEmpty()
                    && consumer.partitionsFor(readiness.topic, Duration.ZERO).isEmpty()) {
                return;
            }
            for (TopicPartition partition : assignment) {
                consumer.position(partition, Duration.ZERO);
            }
        } catch (TimeoutException _) {
            return;
        }

        synchronized (lifecycleLock) {
            if (startupState != StartupState.WAITING || closing) {
                return;
            }
            startupState = StartupState.HANDED_OFF;
        }
        creationFuture.succeed();
    }

    private void handlePollFailure(Exception error) {
        if (error instanceof InterruptException) {
            Thread.currentThread().interrupt();
        }
        synchronized (lifecycleLock) {
            if (startupState == StartupState.WAITING
                    && !(error instanceof RetriableException)
                    && !(error instanceof WakeupException)) {
                startupState = StartupState.FAILED;
                startupFailure = error;
                closing = true;
                return;
            }
        }
        log.error(CONSUME_FAILURE_MESSAGE, error);
    }

    private void finishConsumer() {
        Exception failure = null;
        try {
            consumer.close();
        } catch (Exception error) {
            failure = error;
        }

        Throwable initializationFailure = null;
        synchronized (lifecycleLock) {
            closeFailure = failure;
            if (failure != null && startupFailure != null && failure != startupFailure) {
                startupFailure.addSuppressed(failure);
            }
            if (startupState == StartupState.FAILED) {
                initializationFailure = startupFailure;
            }
        }
        executor.shutdown();
        if (initializationFailure != null) {
            creationFuture.fail(initializationFailure);
        }
    }

    private void wakeup() {
        try {
            consumer.wakeup();
        } catch (RuntimeException error) {
            log.warn("Failed to wake up Kafka consumer during close", error);
        }
    }

    private static final class CreationFuture extends CompletableFuture<BenchmarkConsumer>
            implements AbandonableCreation {
        private final KafkaBenchmarkConsumer owner;

        private CreationFuture(KafkaBenchmarkConsumer owner) {
            this.owner = owner;
        }

        @Override
        public void abandonCreation() {
            synchronized (owner.lifecycleLock) {
                if (owner.startupState != StartupState.WAITING) {
                    return;
                }
                owner.startupState = StartupState.FAILED;
                owner.startupFailure = new IllegalStateException("Kafka consumer creation was abandoned");
                owner.closing = true;
            }
            owner.wakeup();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            CancellationException cancellation =
                    new CancellationException("Kafka consumer creation cancelled");
            synchronized (owner.lifecycleLock) {
                if (owner.startupState != StartupState.WAITING) {
                    return owner.startupState == StartupState.CANCELLED;
                }
                owner.startupState = StartupState.CANCELLED;
                owner.startupFailure = cancellation;
                owner.closing = true;
            }
            owner.wakeup();
            super.completeExceptionally(cancellation);
            return true;
        }

        private void succeed() {
            super.complete(owner);
        }

        private void fail(Throwable error) {
            super.completeExceptionally(error);
        }
    }

    // Gates creation on the first assignment and reports every assignment change to the callback.
    // Kafka calls it on the poll thread.
    private static final class Readiness implements ConsumerRebalanceListener {
        private final String topic;
        private final ConsumerCallback callback;
        private volatile boolean assigned;

        private Readiness(String topic, ConsumerCallback callback) {
            this.topic = topic;
            this.callback = callback;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            assigned = false;
            report(RebalanceEvent.Kind.REVOKED, partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            assigned = true;
            report(RebalanceEvent.Kind.ASSIGNED, partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            assigned = false;
            report(RebalanceEvent.Kind.LOST, partitions);
        }

        // Cooperative rebalancing calls the listener with empty collections; those change nothing
        private void report(RebalanceEvent.Kind kind, Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) {
                return;
            }
            try {
                callback.partitionsChanged(
                        new RebalanceEvent(
                                kind,
                                System.currentTimeMillis(),
                                partitions.stream().map(TopicPartition::partition).sorted().toList()));
            } catch (RuntimeException error) {
                // A failing callback must not abort the rebalance
                log.warn("Rebalance callback failed for topic {}", topic, error);
            }
        }
    }
}
