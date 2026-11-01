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
package io.openmessaging.benchmark.driver;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates closeable resources in batches with a bounded number of retries per resource.
 *
 * <p>Each operation owns its scheduler and its individual results until the returned future
 * succeeds. Cancellation and failure close untransferred results, including late completions;
 * individual creation futures are not cancelled because doing so could discard their eventual
 * results.
 *
 * @param <R> the type of resource request
 * @param <C> the type of created resource
 */
public final class ResourceCreator<R, C extends AutoCloseable> {
    private static final Logger log = LoggerFactory.getLogger(ResourceCreator.class);

    private final String name;
    private final int maxBatchSize;
    private final long interBatchDelayMs;
    private final int maxRetries;
    private final Function<R, CompletableFuture<C>> createOne;

    /**
     * Configures a batched resource creator.
     *
     * @param name resource type used in progress logs
     * @param maxBatchSize maximum number of concurrent requests in a batch
     * @param interBatchDelayMs delay before each batch, including the first
     * @param maxRetries retries per failed resource after the first attempt; zero disables retries
     * @param createOne creates one resource and returns its completion future
     */
    public ResourceCreator(
            String name,
            int maxBatchSize,
            long interBatchDelayMs,
            int maxRetries,
            Function<R, CompletableFuture<C>> createOne) {
        this.name = Objects.requireNonNull(name, "name");
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        if (interBatchDelayMs < 0) {
            throw new IllegalArgumentException("interBatchDelayMs must not be negative");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
        this.maxBatchSize = maxBatchSize;
        this.interBatchDelayMs = interBatchDelayMs;
        this.maxRetries = maxRetries;
        this.createOne = Objects.requireNonNull(createOne, "createOne");
    }

    /**
     * Creates all requested resources, preserving input order in the successful result.
     *
     * @param resources resource requests
     * @return the batch future, which transfers ownership only on success
     */
    public CompletableFuture<List<C>> create(List<R> resources) {
        List<R> requests = List.copyOf(resources);
        if (requests.isEmpty()) {
            return ResourceBatch.completed(List.of());
        }
        return new Operation(requests).start();
    }

    /**
     * Creates an aggregate handle from asynchronously discovered resource requests, without
     * scheduling or retries. The aggregate takes ownership of all individual resources on success. On
     * failure or cancellation, every untransferred resource, including late completions, is closed.
     * Individual creation futures are not cancelled.
     *
     * @param requests asynchronous ordered list of resource requests
     * @param createOne creates one resource for each request
     * @param aggregate constructs a handle owning the complete ordered list of resources
     * @return the aggregate handle's creation future
     * @param <R> request type
     * @param <C> individually created resource type
     * @param <T> aggregate handle type
     */
    public static <R, C extends AutoCloseable, T extends AutoCloseable> CompletableFuture<T> compose(
            CompletableFuture<List<R>> requests,
            Function<R, CompletableFuture<C>> createOne,
            Function<List<C>, T> aggregate) {
        return ResourceBatch.compose(requests, createOne, aggregate);
    }

    private final class Operation {
        private final Queue<Attempt> pending = new ArrayDeque<>();
        private final ScheduledThreadPoolExecutor executor;
        private final ResourceBatch<C> batch;
        private final AtomicInteger succeeded = new AtomicInteger();
        private final int total;
        private volatile Thread workerThread;
        private int inFlight;
        private boolean stopping;

        private Operation(List<R> requests) {
            total = requests.size();
            for (int i = 0; i < requests.size(); i++) {
                pending.add(new Attempt(i, requests.get(i)));
            }
            executor =
                    new ScheduledThreadPoolExecutor(
                            1,
                            runnable -> {
                                Thread thread = new Thread(runnable, "resource-creation-" + name);
                                thread.setDaemon(true);
                                workerThread = thread;
                                return thread;
                            }) {
                        @Override
                        protected void terminated() {
                            try {
                                batch.finishedStarting();
                            } finally {
                                super.terminated();
                            }
                        }
                    };
            executor.setRemoveOnCancelPolicy(true);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            batch = new ResourceBatch<>(requests.size(), this::shutdown);
        }

        private CompletableFuture<List<C>> start() {
            try {
                executor.scheduleAtFixedRate(
                        () -> log.info("Created {}s {}/{}", name, succeeded.get(), total),
                        10,
                        10,
                        TimeUnit.SECONDS);
                scheduleNextBatch();
            } catch (Throwable error) {
                batch.fail(error);
            }
            return batch.future();
        }

        private void scheduleNextBatch() {
            synchronized (this) {
                if (!stopping && batch.isActive()) {
                    executor.schedule(this::startBatch, interBatchDelayMs, TimeUnit.MILLISECONDS);
                }
            }
        }

        private void startBatch() {
            List<Attempt> current = new ArrayList<>();
            synchronized (this) {
                if (stopping || !batch.isActive()) {
                    return;
                }
                while (current.size() < maxBatchSize && !pending.isEmpty()) {
                    current.add(pending.remove());
                }
                inFlight = current.size();
            }

            log.debug("Executing {} creation batch, size: {}", name, current.size());
            for (Attempt attempt : current) {
                if (!batch.isActive()) {
                    break;
                }
                try {
                    CompletableFuture<C> result =
                            Objects.requireNonNull(createOne.apply(attempt.request), "creation future");
                    batch.watch(result, (resource, error) -> completed(attempt, resource, error));
                } catch (Throwable error) {
                    completed(attempt, null, error);
                }
            }
        }

        private void completed(Attempt attempt, C resource, Throwable error) {
            Throwable cause =
                    error == null && resource == null
                            ? new NullPointerException("creation completed with a null resource")
                            : error == null ? null : ResourceBatch.unwrap(error);
            if (cause == null) {
                succeeded.incrementAndGet();
                batch.succeed(attempt.index, resource);
            }

            Throwable terminalFailure = null;
            boolean schedule = false;
            synchronized (this) {
                inFlight--;
                if (stopping || !batch.isActive()) {
                    return;
                }
                if (cause != null) {
                    if (attempt.firstFailure == null) {
                        attempt.firstFailure = cause;
                    }
                    boolean interrupted = isInterruption(cause);
                    if (interrupted || cause instanceof CancellationException) {
                        if (interrupted && Thread.currentThread() == workerThread) {
                            Thread.currentThread().interrupt();
                        }
                        stopping = true;
                        terminalFailure = cause;
                    } else if (attempt.retries < maxRetries) {
                        attempt.retries++;
                        pending.add(attempt);
                        log.debug("Retrying {} creation after failure", name, cause);
                    } else {
                        stopping = true;
                        terminalFailure = attempt.firstFailure;
                    }
                }
                schedule = terminalFailure == null && inFlight == 0;
            }

            if (terminalFailure != null) {
                batch.fail(terminalFailure);
            } else if (schedule) {
                try {
                    scheduleNextBatch();
                } catch (Throwable scheduleFailure) {
                    batch.fail(scheduleFailure);
                }
            }
        }

        private void shutdown() {
            if (Thread.currentThread() == workerThread) {
                executor.shutdown();
            } else {
                executor.shutdownNow();
            }
        }

        private final class Attempt {
            private final int index;
            private final R request;
            private int retries;
            private Throwable firstFailure;

            private Attempt(int index, R request) {
                this.index = index;
                this.request = request;
            }
        }
    }

    private static boolean isInterruption(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }
}
