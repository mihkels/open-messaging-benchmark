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
import io.openmessaging.benchmark.driver.Failures;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.RetriableException;
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
    private static final long PROGRESS_LOG_INTERVAL_MS = 10_000;
    private static final String INTERRUPTED_MESSAGE = "Topic creation interrupted";

    private final AdminClient admin;
    private final Map<String, String> topicConfigs;
    private final short replicationFactor;
    private final int maxBatchSize;
    private final long retryBackoffMs;
    private final LongSupplier clockMillis;

    KafkaTopicCreator(
            AdminClient admin,
            Map<String, String> topicConfigs,
            short replicationFactor,
            int maxBatchSize) {
        this(
                admin,
                topicConfigs,
                replicationFactor,
                maxBatchSize,
                RETRY_BACKOFF_MS,
                System::currentTimeMillis);
    }

    // The backoff and clock are parameters so tests can exercise the retry path quickly.
    KafkaTopicCreator(
            AdminClient admin,
            Map<String, String> topicConfigs,
            short replicationFactor,
            int maxBatchSize,
            long retryBackoffMs,
            LongSupplier clockMillis) {
        this.retryBackoffMs = retryBackoffMs;
        this.clockMillis = clockMillis;
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
                    createWithRetry(topicInfos);
                } catch (Throwable error) {
                    failure = error;
                } finally {
                    worker.shutdown();
                }
                result.finish(failure);
            });
        } catch (RuntimeException error) {
            worker.shutdownNow();
            result.finish(error);
        }
        return result;
    }

    // Package-private so tests can drive the retry path synchronously.
    void createWithRetry(List<TopicInfo> topicInfos) {
        RuntimeException lastException = null;

        for (int attempt = 1; attempt <= MAX_CREATION_ATTEMPTS; attempt++) {
            try {
                log.info("Topic creation attempt {}/{}", attempt, MAX_CREATION_ATTEMPTS);
                createBlocking(topicInfos);
                log.info("Successfully created all {} topics", topicInfos.size());
                return; // Success!
            } catch (FatalTopicCreationException e) {
                throw e;
            } catch (RuntimeException e) {
                if (Thread.currentThread().isInterrupted() || Failures.isInterruption(e)) {
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
                if (attempt < MAX_CREATION_ATTEMPTS) {
                    if (allTopicsExist(topicInfos)) {
                        return;
                    }
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
            if (Failures.isInterruption(checkException)) {
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

    // Failed topics are re-queued, so the queue is empty only once every topic has succeeded.
    private void createBlocking(List<TopicInfo> topicInfos) {
        Queue<TopicInfo> queue = new ArrayDeque<>(topicInfos);
        List<TopicInfo> batch = new ArrayList<>();
        int succeeded = 0;

        long startTime = clockMillis.getAsLong();
        long nextProgressLog = startTime + PROGRESS_LOG_INTERVAL_MS;

        while (succeeded < topicInfos.size()) {
            long now = clockMillis.getAsLong();
            checkAttemptTimeout(now - startTime, succeeded, topicInfos.size());
            if (now >= nextProgressLog) {
                log.info("Created topics {}/{}", succeeded, topicInfos.size());
                nextProgressLog = now + PROGRESS_LOG_INTERVAL_MS;
            }
            while (batch.size() < maxBatchSize && !queue.isEmpty()) {
                batch.add(queue.remove());
            }
            int created = createBatch(batch, queue);
            batch.clear();
            succeeded += created;
            if (created == 0) {
                // The whole batch failed and was re-queued; back off rather than spin on the controller
                sleepInterruptibly(retryBackoffMs);
            }
        }
    }

    private static void checkAttemptTimeout(long elapsedMs, int succeeded, int total) {
        if (elapsedMs > ATTEMPT_TIMEOUT_MS) {
            throw new IllegalStateException(
                    String.format(
                            "Topic creation attempt timed out after %d ms. Created %d/%d topics",
                            ATTEMPT_TIMEOUT_MS, succeeded, total));
        }
    }

    // Returns the number of topics created; failed topics are re-queued for the next batch.
    private int createBatch(List<TopicInfo> batch, Queue<TopicInfo> queue) {
        int succeeded = 0;
        for (Map.Entry<TopicInfo, Boolean> result : executeBatch(batch).entrySet()) {
            TopicInfo topicInfo = result.getKey();
            if (Boolean.TRUE.equals(result.getValue())) {
                succeeded++;
            } else {
                log.warn("Topic creation failed for: {}, will retry", topicInfo.topic());
                queue.add(topicInfo);
            }
        }
        return succeeded;
    }

    private Map<TopicInfo, Boolean> executeBatch(List<TopicInfo> batch) {
        log.debug("Executing batch, size: {}", batch.size());
        var lookup = batch.stream().collect(toMap(TopicInfo::topic, identity()));
        var newTopics = batch.stream().map(this::newTopic).toList();

        try {
            return admin.createTopics(newTopics).values().entrySet().stream()
                    .collect(toMap(e -> lookup.get(e.getKey()), e -> isSuccess(e.getValue())));
        } catch (FatalTopicCreationException e) {
            throw e;
        } catch (Exception e) {
            if (Thread.currentThread().isInterrupted() || Failures.isInterruption(e)) {
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
            if (e.getCause() instanceof TopicExistsException) {
                return true;
            }
            if (!(e.getCause() instanceof RetriableException)) {
                // Errors such as an invalid replication factor or an ACL denial fail every retry
                throw new FatalTopicCreationException(e.getCause());
            }
            return false;
        }
        return true;
    }

    private static final class FatalTopicCreationException extends RuntimeException {
        FatalTopicCreationException(Throwable cause) {
            super("Topic creation failed with a non-retriable error: " + cause, cause);
        }
    }
}
