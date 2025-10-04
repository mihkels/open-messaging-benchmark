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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

public class RedisBenchmarkConsumer implements BenchmarkConsumer {
    private static final Logger log = LoggerFactory.getLogger(RedisBenchmarkConsumer.class);

    private final String topic;
    private final String subscriptionName;
    private final ExecutorService executor;
    private final Future<?> consumerTask;
    private volatile boolean closing = false;

    public RedisBenchmarkConsumer(
            final String consumerId,
            final String topic,
            final String subscriptionName,
            final JedisPool pool,
            ConsumerCallback consumerCallback) {
        this.topic = topic;
        this.subscriptionName = subscriptionName;
        this.executor = Executors.newSingleThreadExecutor();

        this.consumerTask =
                this.executor.submit(
                        () -> {
                            while (!closing) {
                                // Get a fresh Jedis connection from the pool for each iteration
                                try (Jedis jedis = pool.getResource()) {
                                    Map<String, StreamEntryID> streamQuery =
                                            Collections.singletonMap(topic, StreamEntryID.UNRECEIVED_ENTRY);

                                    // Use block(1000) for 1 second timeout instead of block(0) which blocks
                                    // indefinitely
                                    // This allows the loop to check the closing flag periodically
                                    List<Map.Entry<String, List<StreamEntry>>> range =
                                            jedis.xreadGroup(
                                                    subscriptionName,
                                                    consumerId,
                                                    XReadGroupParams.xReadGroupParams()
                                                            .count(100) // Read up to 100 messages at a time
                                                            .block(1000), // 1 second timeout
                                                    streamQuery);

                                    if (range != null && !range.isEmpty()) {
                                        for (Map.Entry<String, List<StreamEntry>> streamEntries : range) {
                                            for (StreamEntry entry : streamEntries.getValue()) {
                                                if (closing) {
                                                    break;
                                                }
                                                long timestamp = entry.getID().getTime();
                                                byte[] payload = entry.getFields().get("payload").getBytes(UTF_8);
                                                consumerCallback.messageReceived(payload, timestamp);

                                                // Acknowledge the message
                                                jedis.xack(topic, subscriptionName, entry.getID());
                                            }
                                        }
                                    }
                                } catch (redis.clients.jedis.exceptions.JedisDataException e) {
                                    // Ignore NOGROUP errors during shutdown - the stream/group may have been deleted
                                    if (closing && e.getMessage() != null && e.getMessage().contains("NOGROUP")) {
                                        log.debug(
                                                "Stream or consumer group no longer exists (expected during shutdown)");
                                        break;
                                    } else if (!closing) {
                                        log.error("Failed to read from consumer instance.", e);
                                    }
                                } catch (Exception e) {
                                    if (!closing) {
                                        log.error("Failed to read from consumer instance.", e);
                                    }
                                }
                            }
                            log.info(
                                    "Consumer loop exited for topic '{}' subscription '{}'", topic, subscriptionName);
                        });
    }

    @Override
    public void close() throws Exception {
        log.info("Closing Redis consumer for topic '{}' subscription '{}'", topic, subscriptionName);
        closing = true;

        // Cancel the consumer task immediately
        if (consumerTask != null) {
            consumerTask.cancel(true);
        }

        // Shutdown the executor with a timeout
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                    log.warn("Executor did not terminate in time, forcing shutdown");
                    executor.shutdownNow();
                    // Wait a bit more for tasks to respond to being cancelled
                    if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                        log.warn("Executor did not terminate after forced shutdown");
                    }
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for executor shutdown");
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        log.info("Redis consumer closed for topic '{}' subscription '{}'", topic, subscriptionName);
    }
}
