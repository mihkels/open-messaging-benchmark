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
package io.openmessaging.benchmark.driver.kafka;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import io.openmessaging.benchmark.driver.BenchmarkDriver.TopicInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaTopicCreator {
    private static final Logger log = LoggerFactory.getLogger(KafkaTopicCreator.class);

    private static final int MAX_BATCH_SIZE = 500;
    private static final int MAX_CREATION_ATTEMPTS =
            6; // Number of times to retry entire creation process
    private static final long ATTEMPT_TIMEOUT_MS = 60_000; // 1 minute per attempt

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final AdminClient admin;
    private final Map<String, String> topicConfigs;
    private final short replicationFactor;
    private final int maxBatchSize;

    KafkaTopicCreator(
            AdminClient admin,
            Map<String, String> topicConfigs,
            short replicationFactor,
            int maxBatchSize) {
        this.admin = admin;
        this.topicConfigs = topicConfigs;
        this.replicationFactor = replicationFactor;
        this.maxBatchSize = maxBatchSize;
    }

    KafkaTopicCreator(AdminClient admin, Map<String, String> topicConfigs, short replicationFactor) {
        this(admin, topicConfigs, replicationFactor, MAX_BATCH_SIZE);
    }

    CompletableFuture<Void> create(List<TopicInfo> topicInfos) {
        return CompletableFuture.runAsync(() -> createWithRetry(topicInfos));
    }

    private void createWithRetry(List<TopicInfo> topicInfos) {
        RuntimeException lastException = null;

        for (int attempt = 1; attempt <= MAX_CREATION_ATTEMPTS; attempt++) {
            try {
                log.info("Topic creation attempt {}/{}", attempt, MAX_CREATION_ATTEMPTS);
                createBlocking(topicInfos);
                log.info("Successfully created all {} topics", topicInfos.size());
                return; // Success!
            } catch (RuntimeException e) {
                lastException = e;
                log.warn(
                        "Topic creation attempt {}/{} failed: {}",
                        attempt,
                        MAX_CREATION_ATTEMPTS,
                        e.getMessage());

                if (attempt < MAX_CREATION_ATTEMPTS) {
                    // Before retrying, check if topics were actually created
                    List<String> topicNames = topicInfos.stream().map(TopicInfo::topic).toList();
                    try {
                        var existingTopics =
                                admin.listTopics().names().get(5, java.util.concurrent.TimeUnit.SECONDS);
                        long alreadyCreated = topicNames.stream().filter(existingTopics::contains).count();
                        if (alreadyCreated == topicNames.size()) {
                            log.info("All topics already exist, considering creation successful");
                            return;
                        }
                        log.info("{}/{} topics already exist", alreadyCreated, topicNames.size());
                    } catch (Exception checkException) {
                        log.warn("Failed to check existing topics: {}", checkException.getMessage());
                    }

                    long backoffMs = 2000 * attempt;
                    log.info("Waiting {}ms before retrying topic creation", backoffMs);
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Topic creation interrupted", ie);
                    }
                }
            }
        }

        throw new RuntimeException(
                String.format(
                        "Failed to create topics after %d attempts. Last error: %s",
                        MAX_CREATION_ATTEMPTS, lastException != null ? lastException.getMessage() : "unknown"),
                lastException);
    }

    private void createBlocking(List<TopicInfo> topicInfos) {
        BlockingQueue<TopicInfo> queue = new ArrayBlockingQueue<>(topicInfos.size(), true, topicInfos);
        List<TopicInfo> batch = new ArrayList<>();
        AtomicInteger succeeded = new AtomicInteger();

        long startTime = System.currentTimeMillis();

        ScheduledFuture<?> loggingFuture =
                executor.scheduleAtFixedRate(
                        () -> log.info("Created topics {}/{}", succeeded.get(), topicInfos.size()),
                        10,
                        10,
                        SECONDS);

        try {
            while (succeeded.get() < topicInfos.size()) {
                // Check timeout for this attempt
                if (System.currentTimeMillis() - startTime > ATTEMPT_TIMEOUT_MS) {
                    throw new RuntimeException(
                            String.format(
                                    "Topic creation attempt timed out after %d ms. Created %d/%d topics",
                                    ATTEMPT_TIMEOUT_MS, succeeded.get(), topicInfos.size()));
                }

                int batchSize = queue.drainTo(batch, maxBatchSize);
                if (batchSize > 0) {
                    executeBatch(batch)
                            .forEach(
                                    (topicInfo, success) -> {
                                        if (success) {
                                            succeeded.incrementAndGet();
                                        } else {
                                            log.warn("Topic creation failed for: {}, will retry", topicInfo.topic());
                                            if (!queue.offer(topicInfo)) {
                                                log.error("Failed to re-queue topic for retry: {}", topicInfo.topic());
                                            }
                                        }
                                    });
                    batch.clear();
                } else {
                    // Avoid busy-waiting
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Topic creation interrupted", e);
                    }
                }
            }
        } finally {
            loggingFuture.cancel(true);
        }
    }

    private Map<TopicInfo, Boolean> executeBatch(List<TopicInfo> batch) {
        log.debug("Executing batch, size: {}", batch.size());
        var lookup = batch.stream().collect(toMap(TopicInfo::topic, identity()));
        var newTopics = batch.stream().map(this::newTopic).toList();

        try {
            return admin.createTopics(newTopics).values().entrySet().stream()
                    .collect(toMap(e -> lookup.get(e.getKey()), e -> isSuccess(e.getValue())));
        } catch (Exception e) {
            log.error(
                    "Failed to execute batch creation for topics: {}",
                    batch.stream().map(TopicInfo::topic).toList(),
                    e);
            // Return all as failures
            return batch.stream().collect(toMap(t -> t, t -> false));
        }
    }

    private NewTopic newTopic(TopicInfo topicInfo) {
        var newTopic = new NewTopic(topicInfo.topic(), topicInfo.partitions(), replicationFactor);
        newTopic.configs(topicConfigs);
        return newTopic;
    }

    private boolean isSuccess(KafkaFuture<Void> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            log.warn(
                    "Topic creation failed: {}",
                    e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            return e.getCause() instanceof TopicExistsException;
        }
        return true;
    }
}
