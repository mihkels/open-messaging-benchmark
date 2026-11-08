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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds one aggregate handle from asynchronously discovered resource requests, owning every
 * individually created resource until the aggregate is handed off. See {@link
 * ResourceCreator#compose}.
 */
final class ResourceComposition {
    private static final Logger log = LoggerFactory.getLogger(ResourceComposition.class);

    private ResourceComposition() {}

    /**
     * An already handed-off creation result that still reports quiescence and can be abandoned.
     *
     * @param value the result
     * @return the completed future
     * @param <T> the result type
     */
    static <T> CompletableFuture<T> completed(T value) {
        CreationTracker tracker = new CreationTracker();
        ComposedFuture<T> result = new ComposedFuture<>(tracker);
        result.complete(value);
        tracker.release();
        return result;
    }

    static <R, C extends AutoCloseable, T extends AutoCloseable> CompletableFuture<T> compose(
            CompletableFuture<List<R>> requests,
            Function<R, CompletableFuture<C>> create,
            Function<List<C>, T> aggregate) {
        Objects.requireNonNull(requests, "requests");
        Objects.requireNonNull(create, "create");
        Objects.requireNonNull(aggregate, "aggregate");

        CreationTracker tracker = new CreationTracker();
        ComposedFuture<T> result = new ComposedFuture<>(tracker);
        tracker.follow(requests);
        AtomicReference<ResourceBatch<C>> active = new AtomicReference<>();
        tracker.retain();
        result.whenComplete((value, error) -> {
            try {
                ResourceBatch<C> batch = active.get();
                if (batch != null) {
                    batch.future().cancel(false);
                }
            } finally {
                tracker.release();
            }
        });
        requests.whenComplete((resources, error) -> {
            try {
                if (error != null) {
                    result.completeExceptionally(Failures.unwrap(error));
                } else if (!result.isDone()) {
                    createAll(resources, create, aggregate, result, tracker, active);
                }
            } finally {
                tracker.release();
            }
        });
        return result;
    }

    // Creates one resource per request and aggregates them into the result.
    private static <R, C extends AutoCloseable, T extends AutoCloseable> void createAll(
            List<R> resources,
            Function<R, CompletableFuture<C>> create,
            Function<List<C>, T> aggregate,
            ComposedFuture<T> result,
            CreationTracker tracker,
            AtomicReference<ResourceBatch<C>> active) {
        final List<R> inputs;
        try {
            inputs = List.copyOf(resources);
        } catch (RuntimeException failure) {
            result.completeExceptionally(failure);
            return;
        }

        if (inputs.isEmpty()) {
            completeAggregate(List.of(), aggregate, result, tracker);
            return;
        }
        ResourceBatch<C> batch = new ResourceBatch<>(inputs.size(), () -> {});
        tracker.retain();
        batch
                .quiescence()
                .whenComplete((ignored, failure) -> {
                    try {
                        if (failure != null) {
                            tracker.record(Failures.unwrap(failure));
                        }
                    } finally {
                        tracker.release();
                    }
                });
        tracker.retain();
        batch
                .future()
                .whenComplete((created, failure) -> {
                    try {
                        if (failure != null) {
                            result.completeExceptionally(Failures.unwrap(failure));
                        } else {
                            completeAggregate(created, aggregate, result, tracker);
                        }
                    } finally {
                        tracker.release();
                    }
                });
        active.set(batch);
        if (result.isDone()) {
            batch.future().cancel(false);
        }
        batch.start(inputs, create);
    }

    private static <C extends AutoCloseable, T extends AutoCloseable> void completeAggregate(
            List<C> resources,
            Function<List<C>, T> aggregate,
            CompletableFuture<T> result,
            CreationTracker tracker) {
        T handle;
        try {
            handle = Objects.requireNonNull(aggregate.apply(resources), "aggregate resource");
        } catch (Throwable error) {
            Set<C> closed = Collections.newSetFromMap(new IdentityHashMap<>());
            for (C resource : resources) {
                if (closed.add(resource)) {
                    closeAfterFailure(resource, error, tracker);
                }
            }
            if (!result.completeExceptionally(error)) {
                recordLateFailure(result, error);
            }
            return;
        }
        if (!result.complete(handle)) {
            Throwable failure = completionFailure(result);
            closeAfterFailure(handle, failure, tracker);
        }
    }

    private static void closeAfterFailure(
            AutoCloseable resource, @Nullable Throwable failure, CreationTracker tracker) {
        try {
            resource.close();
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (failure != null && failure != error) {
                failure.addSuppressed(error);
            }
            tracker.record(error);
            log.warn("Failed to close an untransferred aggregate resource", error);
        }
    }

    private static void recordLateFailure(CompletableFuture<?> result, Throwable error) {
        Throwable failure = completionFailure(result);
        if (failure != null && failure != error) {
            failure.addSuppressed(error);
        }
        log.warn("Failed to construct an untransferred aggregate resource", error);
    }

    private static @Nullable Throwable completionFailure(CompletableFuture<?> result) {
        try {
            result.join();
            return null;
        } catch (CompletionException | CancellationException error) {
            return Failures.unwrap(error);
        }
    }

    private static final class ComposedFuture<T> extends CompletableFuture<T>
            implements ResourceCreationCompletion, AbandonableCreation {
        private final CreationTracker tracker;

        private ComposedFuture(CreationTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        public CompletableFuture<Void> quiescence() {
            return tracker.quiescence();
        }

        @Override
        public void abandonCreation() {
            cancel(false);
        }
    }
}
