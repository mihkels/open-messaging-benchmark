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
package io.openmessaging.benchmark.driver.redis;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntryBinary;

public final class RedisBenchmarkConsumer implements BenchmarkConsumer {
    private static final Logger log = LoggerFactory.getLogger(RedisBenchmarkConsumer.class);
    private static final byte[] PAYLOAD_FIELD = "payload".getBytes(UTF_8);

    private final String topic;
    private final String subscriptionName;
    private final ExecutorService executor;
    private final Future<?> consumerTask;
    private final long closeTimeoutNanos;
    private volatile boolean closing = false;
    private volatile Thread consumerThread;

    public RedisBenchmarkConsumer(
            final String consumerId,
            final String topic,
            final String subscriptionName,
            final JedisPool pool,
            ConsumerCallback consumerCallback) {
        this(consumerId, topic, subscriptionName, pool, consumerCallback, Duration.ofSeconds(5));
    }

    RedisBenchmarkConsumer(
            final String consumerId,
            final String topic,
            final String subscriptionName,
            final JedisPool pool,
            ConsumerCallback consumerCallback,
            Duration closeTimeout) {
        this.topic = topic;
        this.subscriptionName = subscriptionName;
        this.closeTimeoutNanos = closeTimeout.toNanos();
        if (closeTimeoutNanos <= 0) {
            throw new IllegalArgumentException("Consumer close timeout must be positive");
        }
        this.executor = Executors.newSingleThreadExecutor();
        byte[] groupName = subscriptionName.getBytes(UTF_8);
        byte[] consumerName = consumerId.getBytes(UTF_8);
        Map<byte[], StreamEntryID> streamQuery =
                Collections.singletonMap(topic.getBytes(UTF_8), StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY);

        this.consumerTask =
                this.executor.submit(
                        () -> consume(pool, groupName, consumerName, streamQuery, consumerCallback));
    }

    private void consume(
            JedisPool pool,
            byte[] groupName,
            byte[] consumerName,
            Map<byte[], StreamEntryID> streamQuery,
            ConsumerCallback consumerCallback) {
        consumerThread = Thread.currentThread();
        boolean keepReading = true;
        while (keepReading && !closing) {
            keepReading = pollOnce(pool, groupName, consumerName, streamQuery, consumerCallback);
        }
        log.info("Consumer loop exited for topic '{}' subscription '{}'", topic, subscriptionName);
    }

    // Reads and delivers one batch; returns false when the loop should stop.
    private boolean pollOnce(
            JedisPool pool,
            byte[] groupName,
            byte[] consumerName,
            Map<byte[], StreamEntryID> streamQuery,
            ConsumerCallback consumerCallback) {
        // Get a fresh Jedis connection from the pool for each iteration
        try (Jedis jedis = pool.getResource()) {
            // Use block(1000) for 1 second timeout instead of block(0) which blocks
            // indefinitely
            // This allows the loop to check the closing flag periodically
            List<Map.Entry<byte[], List<StreamEntryBinary>>> range =
                    jedis.xreadGroupBinary(
                            groupName,
                            consumerName,
                            XReadGroupParams.xReadGroupParams()
                                    .count(100) // Read up to 100 messages at a time
                                    .block(1000), // 1 second timeout
                            streamQuery);
            deliver(jedis, range, consumerCallback);
        } catch (redis.clients.jedis.exceptions.JedisDataException e) {
            // Ignore NOGROUP errors during shutdown - the stream/group may have been deleted
            if (closing && e.getMessage() != null && e.getMessage().contains("NOGROUP")) {
                log.debug("Stream or consumer group no longer exists (expected during shutdown)");
                return false;
            } else if (!closing) {
                log.error("Failed to read from consumer instance.", e);
            }
        } catch (Exception e) {
            if (!closing) {
                log.error("Failed to read from consumer instance.", e);
            }
        }
        return true;
    }

    private void deliver(
            Jedis jedis,
            List<Map.Entry<byte[], List<StreamEntryBinary>>> range,
            ConsumerCallback consumerCallback) {
        if (range == null) {
            return;
        }
        for (var streamEntries : range) {
            for (StreamEntryBinary entry : streamEntries.getValue()) {
                if (closing) {
                    return;
                }
                long timestamp = entry.getID().getTime();
                byte[] payload = readPayload(entry);
                consumerCallback.messageReceived(payload, timestamp);

                // Acknowledge the message
                jedis.xack(topic, subscriptionName, entry.getID());
            }
        }
    }

    private static byte[] readPayload(StreamEntryBinary entry) {
        for (var field : entry.getFields().entrySet()) {
            if (Arrays.equals(field.getKey(), PAYLOAD_FIELD)) {
                return field.getValue();
            }
        }
        throw new IllegalStateException("Missing payload field in Redis stream entry " + entry.getID());
    }

    @Override
    public void close() throws Exception {
        if (Thread.currentThread() == consumerThread) {
            throw new IllegalStateException("Cannot close Redis consumer from its own callback");
        }
        log.info("Closing Redis consumer for topic '{}' subscription '{}'", topic, subscriptionName);
        closing = true;
        consumerTask.cancel(true);
        executor.shutdownNow();

        CloseFailures failures = new CloseFailures(Thread.interrupted());
        boolean terminated = awaitTermination(failures);
        if (terminated && !consumerTask.isCancelled()) {
            collectTaskFailure(failures);
        }
        if (failures.interrupted) {
            Thread.currentThread().interrupt();
        }
        if (failures.failure != null) {
            throw failures.failure;
        }
        log.info("Redis consumer closed for topic '{}' subscription '{}'", topic, subscriptionName);
    }

    private boolean awaitTermination(CloseFailures failures) {
        long deadline = System.nanoTime() + closeTimeoutNanos;
        while (true) {
            try {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0 || !executor.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                    failures.add(new TimeoutException("Redis consumer thread did not terminate"));
                    return false;
                }
                return true;
            } catch (InterruptedException e) {
                failures.interrupted = true;
                failures.add(e);
            }
        }
    }

    private void collectTaskFailure(CloseFailures failures) {
        try {
            consumerTask.get();
        } catch (ExecutionException e) {
            failures.add(e);
        } catch (InterruptedException e) {
            failures.interrupted = true;
            failures.add(e);
        }
    }

    // Collects close failures: the first is primary and later ones are suppressed into it.
    private static final class CloseFailures {
        private Exception failure;
        private boolean interrupted;

        private CloseFailures(boolean interrupted) {
            this.interrupted = interrupted;
            this.failure =
                    interrupted ? new InterruptedException("Interrupted before consumer close") : null;
        }

        private void add(Exception next) {
            if (failure == null) {
                failure = next;
            } else {
                failure.addSuppressed(next);
            }
        }
    }
}
