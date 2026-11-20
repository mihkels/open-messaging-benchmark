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
package io.github.mihkels.omb.driver.redis;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.mihkels.omb.driver.BenchmarkConsumer;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.Failures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;
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
    private static final byte[] TIMESTAMP_FIELD = "ts".getBytes(UTF_8);

    private final String topic;
    private final String subscriptionName;
    private final JedisPool pool;
    private final byte[] groupName;
    private final byte[] consumerName;
    private final Map<byte[], StreamEntryID> streamQuery;
    // Read up to 100 messages at a time; block(1000) rather than block(0) wakes the loop every
    // second so it can check the closing flag
    private final XReadGroupParams readParams =
            XReadGroupParams.xReadGroupParams().count(100).block(1000);
    private final ConsumerCallback consumerCallback;
    private final ExecutorService executor;
    private final Future<?> consumerTask;
    private final long closeTimeoutNanos;
    private volatile boolean closing = false;
    private volatile @Nullable Thread consumerThread;

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
        this.pool = pool;
        this.consumerCallback = consumerCallback;
        this.closeTimeoutNanos = closeTimeout.toNanos();
        if (closeTimeoutNanos <= 0) {
            throw new IllegalArgumentException("Consumer close timeout must be positive");
        }
        this.executor =
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("redis-consumer").factory());
        this.groupName = subscriptionName.getBytes(UTF_8);
        this.consumerName = consumerId.getBytes(UTF_8);
        this.streamQuery = Map.of(topic.getBytes(UTF_8), StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY);

        this.consumerTask = this.executor.submit(this::consume);
    }

    private void consume() {
        consumerThread = Thread.currentThread();
        boolean keepReading = true;
        while (keepReading && !closing) {
            keepReading = pollOnce();
        }
        log.info("Consumer loop exited for topic '{}' subscription '{}'", topic, subscriptionName);
    }

    // Reads and delivers one batch; returns false when the loop should stop.
    private boolean pollOnce() {
        // Get a fresh Jedis connection from the pool for each iteration
        try (Jedis jedis = pool.getResource()) {
            List<Map.Entry<byte[], List<StreamEntryBinary>>> range =
                    jedis.xreadGroupBinary(groupName, consumerName, readParams, streamQuery);
            deliver(jedis, range);
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

    private void deliver(Jedis jedis, List<Map.Entry<byte[], List<StreamEntryBinary>>> range) {
        if (range == null) {
            return;
        }
        // The only stream read is topic; its delivered entries are acknowledged in one call
        List<StreamEntryID> delivered = new ArrayList<>();
        try {
            for (var streamEntries : range) {
                for (StreamEntryBinary entry : streamEntries.getValue()) {
                    if (closing) {
                        return;
                    }
                    byte[] payload = field(entry, PAYLOAD_FIELD);
                    if (payload == null) {
                        throw new IllegalStateException(
                                "Missing payload field in Redis stream entry " + entry.getID());
                    }
                    consumerCallback.messageReceived(payload, publishTimestamp(entry));
                    delivered.add(entry.getID());
                }
            }
        } finally {
            if (!delivered.isEmpty()) {
                jedis.xack(topic, subscriptionName, delivered.toArray(StreamEntryID[]::new));
            }
        }
    }

    private static byte @Nullable [] field(StreamEntryBinary entry, byte[] name) {
        for (var field : entry.getFields().entrySet()) {
            if (Arrays.equals(field.getKey(), name)) {
                return field.getValue();
            }
        }
        return null;
    }

    // The entry ID's time is the broker's receive time, so only the producer's field is used;
    // an entry without a readable one is counted but gives no latency sample.
    private static long publishTimestamp(StreamEntryBinary entry) {
        byte[] value = field(entry, TIMESTAMP_FIELD);
        if (value != null) {
            try {
                return Long.parseLong(new String(value, UTF_8));
            } catch (NumberFormatException _) {
                // fall through to the placeholder
            }
        }
        return ConsumerCallback.NO_PUBLISH_TIMESTAMP;
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

        Failures failures = new Failures();
        failures.deferInterrupt("Interrupted before consumer close");
        boolean terminated = awaitTermination(failures);
        if (terminated && !consumerTask.isCancelled()) {
            collectTaskFailure(failures);
        }
        failures.throwIfAny();
        log.info("Redis consumer closed for topic '{}' subscription '{}'", topic, subscriptionName);
    }

    private boolean awaitTermination(Failures failures) {
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
                failures.addInterruption(e);
            }
        }
    }

    private void collectTaskFailure(Failures failures) {
        try {
            consumerTask.get();
        } catch (ExecutionException e) {
            failures.add(e);
        } catch (InterruptedException e) {
            failures.addInterruption(e);
        }
    }
}
