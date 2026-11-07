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
import java.util.concurrent.ExecutorService;
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
    private static final long RETRY_BACKOFF_MS = 2000L;
    private static final String INTERRUPTED_MESSAGE = "Topic creation interrupted";

    private final AdminClient admin;
    private final Map<String, String> topicConfigs;
    private final short replicationFactor;
    private final int maxBatchSize;
    private final long retryBackoffMs;

    KafkaTopicCreator(
            AdminClient admin,
            Map<String, String> topicConfigs,
            short replicationFactor,
            int maxBatchSize) {
        this(admin, topicConfigs, replicationFactor, maxBatchSize, RETRY_BACKOFF_MS);
    }

    // The backoff is a parameter so tests can exercise the retry path quickly.
    KafkaTopicCreator(
            AdminClient admin,
            Map<String, String> topicConfigs,
            short replicationFactor,
            int maxBatchSize,
            long retryBackoffMs) {
        this.retryBackoffMs = retryBackoffMs;
        this.admin = admin;
        this.topicConfigs = topicConfigs;
        this.replicationFactor = replicationFactor;
        this.maxBatchSize = maxBatchSize;
    }

    KafkaTopicCreator(AdminClient admin, Map<String, String> topicConfigs, short replicationFactor) {
        this(admin, topicConfigs, replicationFactor, MAX_BATCH_SIZE);
    }

    CompletableFuture<Void> create(List<TopicInfo> topicInfos) {
        if (topicInfos.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        ExecutorService worker =
                Executors.newSingleThreadExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "kafka-topic-creation");
                    thread.setDaemon(true);
                    return thread;
                });
        ScheduledExecutorService progress =
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "kafka-topic-progress");
                    thread.setDaemon(true);
                    return thread;
                });
        class CreationFuture extends CompletableFuture<Void> {
            private boolean terminal;
            private boolean cancelled;

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                synchronized (this) {
                    if (terminal) {
                        return cancelled;
                    }
                    terminal = true;
                    cancelled = true;
                }
                try {
                    worker.shutdownNow();
                    progress.shutdownNow();
                } finally {
                    super.cancel(mayInterruptIfRunning);
                }
                return true;
            }

            void finish(Throwable error) {
                synchronized (this) {
                    if (terminal) {
                        return;
                    }
                    terminal = true;
                }
                if (error == null) {
                    super.complete(null);
                } else {
                    super.completeExceptionally(error);
                }
            }
        }
        CreationFuture result = new CreationFuture();
        try {
            worker.execute(() -> {
                Throwable failure = null;
                try {
                    createWithRetry(topicInfos, progress);
                } catch (Throwable error) {
                    failure = error;
                } finally {
                    progress.shutdownNow();
                    worker.shutdown();
                }
                result.finish(failure);
            });
        } catch (RuntimeException error) {
            worker.shutdownNow();
            progress.shutdownNow();
            result.finish(error);
        }
        return result;
    }

    // Package-private so tests can drive the retry path with a controlled progress executor.
    void createWithRetry(List<TopicInfo> topicInfos, ScheduledExecutorService progress) {
        RuntimeException lastException = null;

        for (int attempt = 1; attempt <= MAX_CREATION_ATTEMPTS; attempt++) {
            try {
                log.info("Topic creation attempt {}/{}", attempt, MAX_CREATION_ATTEMPTS);
                createBlocking(topicInfos, progress);
                log.info("Successfully created all {} topics", topicInfos.size());
                return; // Success!
            } catch (RuntimeException e) {
                if (Thread.currentThread().isInterrupted() || isInterruption(e)) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                lastException = e;
                log.warn(
                        "Topic creation attempt {}/{} failed: {}",
                        attempt,
                        MAX_CREATION_ATTEMPTS,
                        e.getMessage());

                // Before retrying, check if topics were actually created
                if (attempt < MAX_CREATION_ATTEMPTS && allTopicsExist(topicInfos)) {
                    return;
                }
                if (attempt < MAX_CREATION_ATTEMPTS) {
                    sleepBeforeRetry(attempt);
                }
            }
        }

        throw new RuntimeException(
                String.format(
                        "Failed to create topics after %d attempts. Last error: %s",
                        MAX_CREATION_ATTEMPTS, lastException.getMessage()),
                lastException);
    }

    private boolean allTopicsExist(List<TopicInfo> topicInfos) {
        List<String> topicNames = topicInfos.stream().map(TopicInfo::topic).toList();
        try {
            var existingTopics = admin.listTopics().names().get(5, SECONDS);
            long alreadyCreated = topicNames.stream().filter(existingTopics::contains).count();
            if (alreadyCreated == topicNames.size()) {
                log.info("All topics already exist, considering creation successful");
                return true;
            }
            log.info("{}/{} topics already exist", alreadyCreated, topicNames.size());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(INTERRUPTED_MESSAGE, interrupted);
        } catch (Exception checkException) {
            if (isInterruption(checkException)) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(INTERRUPTED_MESSAGE, checkException);
            }
            log.warn("Failed to check existing topics: {}", checkException.getMessage());
        }
        return false;
    }

    private void sleepBeforeRetry(int attempt) {
        long backoffMs = retryBackoffMs * attempt;
        log.info("Waiting {}ms before retrying topic creation", backoffMs);
        sleepInterruptibly(backoffMs);
    }

    private static void sleepInterruptibly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(INTERRUPTED_MESSAGE, e);
        }
    }

    private void createBlocking(List<TopicInfo> topicInfos, ScheduledExecutorService progress) {
        BlockingQueue<TopicInfo> queue = new ArrayBlockingQueue<>(topicInfos.size(), true, topicInfos);
        List<TopicInfo> batch = new ArrayList<>();
        AtomicInteger succeeded = new AtomicInteger();

        long startTime = System.currentTimeMillis();

        ScheduledFuture<?> loggingFuture =
                progress.scheduleAtFixedRate(
                        () -> log.info("Created topics {}/{}", succeeded.get(), topicInfos.size()),
                        10,
                        10,
                        SECONDS);

        try {
            while (succeeded.get() < topicInfos.size()) {
                checkAttemptTimeout(startTime, succeeded.get(), topicInfos.size());
                int batchSize = queue.drainTo(batch, maxBatchSize);
                if (batchSize > 0) {
                    createBatch(batch, queue, succeeded);
                    batch.clear();
                } else {
                    // Avoid busy-waiting
                    sleepInterruptibly(100);
                }
            }
        } finally {
            loggingFuture.cancel(true);
        }
    }

    private static void checkAttemptTimeout(long startTime, int succeeded, int total) {
        if (System.currentTimeMillis() - startTime > ATTEMPT_TIMEOUT_MS) {
            throw new IllegalStateException(
                    String.format(
                            "Topic creation attempt timed out after %d ms. Created %d/%d topics",
                            ATTEMPT_TIMEOUT_MS, succeeded, total));
        }
    }

    private void createBatch(
            List<TopicInfo> batch, BlockingQueue<TopicInfo> queue, AtomicInteger succeeded) {
        for (Map.Entry<TopicInfo, Boolean> result : executeBatch(batch).entrySet()) {
            TopicInfo topicInfo = result.getKey();
            if (Boolean.TRUE.equals(result.getValue())) {
                succeeded.incrementAndGet();
            } else {
                log.warn("Topic creation failed for: {}, will retry", topicInfo.topic());
                if (!queue.offer(topicInfo)) {
                    log.error("Failed to re-queue topic for retry: {}", topicInfo.topic());
                }
            }
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
            if (Thread.currentThread().isInterrupted() || isInterruption(e)) {
                Thread.currentThread().interrupt();
                throw (RuntimeException) e;
            }
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

    private static boolean isInterruption(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }
}
