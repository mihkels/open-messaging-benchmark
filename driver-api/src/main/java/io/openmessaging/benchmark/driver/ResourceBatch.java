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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns individual creation results until the whole batch is handed off.
 *
 * @param <C> the type of resource owned by the batch
 */
final class ResourceBatch<C extends AutoCloseable> {
    private static final Logger log = LoggerFactory.getLogger(ResourceBatch.class);

    private enum State {
        ACTIVE,
        HANDED_OFF,
        FAILED,
        CANCELLED
    }

    private final Object lock = new Object();
    private final List<C> results;
    private final Set<C> closed = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<AbandonableCreation> pending =
            Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<AbandonableCreation> abandoned =
            Collections.newSetFromMap(new IdentityHashMap<>());
    private final Tracker tracker = new Tracker();
    private final Runnable onFinish;
    private final ResultFuture future = new ResultFuture();
    private State state = State.ACTIVE;
    private Throwable failure;
    private int completed;

    ResourceBatch(int size, Runnable onFinish) {
        this.results = new ArrayList<>(Collections.nCopies(size, null));
        this.onFinish = onFinish;
    }

    static <R, C extends AutoCloseable> CompletableFuture<List<C>> create(
            List<R> requests, Function<R, CompletableFuture<C>> create) {
        if (requests.isEmpty()) {
            return completed(List.of());
        }

        ResourceBatch<C> batch = new ResourceBatch<>(requests.size(), () -> {});
        batch.start(requests, create);
        return batch.future();
    }

    static <T> CompletableFuture<T> completed(T value) {
        Tracker tracker = new Tracker();
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

        Tracker tracker = new Tracker();
        ComposedFuture<T> result = new ComposedFuture<>(tracker);
        tracker.follow(requests);
        AtomicReference<ResourceBatch<C>> active = new AtomicReference<>();
        tracker.retain();
        result.whenComplete(
                (value, error) -> {
                    try {
                        ResourceBatch<C> batch = active.get();
                        if (batch != null) {
                            batch.future().cancel(false);
                        }
                    } finally {
                        tracker.release();
                    }
                });
        requests.whenComplete(
                (resources, error) -> {
                    try {
                        if (error != null) {
                            result.completeExceptionally(unwrap(error));
                            return;
                        }
                        if (result.isDone()) {
                            return;
                        }
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
                                .whenComplete(
                                        (ignored, failure) -> {
                                            try {
                                                if (failure != null) {
                                                    tracker.record(unwrap(failure));
                                                }
                                            } finally {
                                                tracker.release();
                                            }
                                        });
                        tracker.retain();
                        batch
                                .future()
                                .whenComplete(
                                        (created, failure) -> {
                                            try {
                                                if (failure != null) {
                                                    result.completeExceptionally(unwrap(failure));
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
                    } finally {
                        tracker.release();
                    }
                });
        return result;
    }

    private <R> void start(List<R> requests, Function<R, CompletableFuture<C>> create) {
        try {
            for (int index = 0; index < requests.size() && isActive(); index++) {
                final int resultIndex = index;
                try {
                    CompletableFuture<C> attempt =
                            Objects.requireNonNull(create.apply(requests.get(index)), "creation future");
                    watch(
                            attempt,
                            (value, error) -> {
                                if (error == null) {
                                    succeed(resultIndex, value);
                                } else {
                                    fail(error);
                                }
                            });
                } catch (Throwable error) {
                    fail(error);
                }
            }
        } finally {
            finishedStarting();
        }
    }

    private static <C extends AutoCloseable, T extends AutoCloseable> void completeAggregate(
            List<C> resources,
            Function<List<C>, T> aggregate,
            CompletableFuture<T> result,
            Tracker tracker) {
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
            AutoCloseable resource, Throwable failure, Tracker tracker) {
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

    private static Throwable completionFailure(CompletableFuture<?> result) {
        try {
            result.join();
            return null;
        } catch (CompletionException | CancellationException error) {
            return unwrap(error);
        }
    }

    CompletableFuture<List<C>> future() {
        return future;
    }

    CompletableFuture<Void> quiescence() {
        return tracker.quiescence();
    }

    void finishedStarting() {
        tracker.release();
    }

    void watch(CompletableFuture<C> attempt, BiConsumer<C, Throwable> callback) {
        tracker.retain();
        try {
            tracker.follow(attempt);
            observe(attempt);
            attempt.whenComplete(
                    (value, error) -> {
                        try {
                            callback.accept(value, error);
                        } finally {
                            tracker.release();
                        }
                    });
        } catch (Throwable error) {
            tracker.release();
            throw error;
        }
    }

    boolean isActive() {
        synchronized (lock) {
            return state == State.ACTIVE;
        }
    }

    void observe(CompletableFuture<?> attempt) {
        if (!(attempt instanceof AbandonableCreation creation)) {
            return;
        }
        boolean stop = false;
        synchronized (lock) {
            if (state == State.ACTIVE) {
                pending.add(creation);
            } else if (state != State.HANDED_OFF) {
                stop = abandoned.add(creation);
            }
        }
        if (stop) {
            abandon(creation);
        }
    }

    void succeed(int index, C resource) {
        if (resource == null) {
            fail(new NullPointerException("creation completed with a null resource"));
            return;
        }

        List<C> handoff = null;
        boolean close = false;
        synchronized (lock) {
            if (state == State.ACTIVE) {
                results.set(index, resource);
                if (++completed == results.size()) {
                    state = State.HANDED_OFF;
                    pending.clear();
                    handoff = List.copyOf(results);
                }
            } else if (state != State.HANDED_OFF) {
                close = closed.add(resource);
            }
        }

        if (close) {
            close(resource);
        }
        if (handoff != null) {
            finish();
            future.succeed(handoff);
        }
    }

    void fail(Throwable error) {
        failOrCancel(unwrap(error), false);
    }

    private boolean cancel() {
        return failOrCancel(new CancellationException("Resource creation was cancelled"), true);
    }

    private boolean failOrCancel(Throwable cause, boolean cancellation) {
        List<C> toClose = new ArrayList<>();
        List<AbandonableCreation> toAbandon = new ArrayList<>();
        synchronized (lock) {
            if (state != State.ACTIVE) {
                return cancellation
                        && (state == State.CANCELLED
                                || (state == State.FAILED && failure instanceof CancellationException));
            }
            state = cancellation ? State.CANCELLED : State.FAILED;
            tracker.retain();
            failure = cause;
            for (AbandonableCreation creation : pending) {
                if (abandoned.add(creation)) {
                    toAbandon.add(creation);
                }
            }
            pending.clear();
            for (C resource : results) {
                if (resource != null && closed.add(resource)) {
                    toClose.add(resource);
                }
            }
            Collections.fill(results, null);
        }

        try {
            finish();
            toAbandon.forEach(this::abandon);
            toClose.forEach(this::close);
            future.fail(cause);
        } finally {
            tracker.release();
        }
        return true;
    }

    private void finish() {
        try {
            onFinish.run();
        } catch (Throwable error) {
            recordCleanupFailure(error);
        }
    }

    private void close(C resource) {
        try {
            resource.close();
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            recordCleanupFailure(error);
        }
    }

    private void abandon(AbandonableCreation creation) {
        try {
            creation.abandonCreation();
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            recordCleanupFailure(error);
        }
    }

    private void recordCleanupFailure(Throwable error) {
        Throwable original;
        synchronized (lock) {
            original = failure;
        }
        if (original != null && original != error) {
            original.addSuppressed(error);
        }
        tracker.record(error);
        log.warn("Failed to clean up a resource creation batch", error);
    }

    static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException)
                && error.getCause() != null) {
            error = error.getCause();
        }
        return error;
    }

    private final class ResultFuture extends CompletableFuture<List<C>>
            implements ResourceCreationCompletion {
        @Override
        public CompletableFuture<Void> quiescence() {
            return ResourceBatch.this.quiescence();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return ResourceBatch.this.cancel();
        }

        private void succeed(List<C> resources) {
            super.complete(resources);
        }

        private void fail(Throwable error) {
            super.completeExceptionally(error);
        }
    }

    private static final class ComposedFuture<T> extends CompletableFuture<T>
            implements ResourceCreationCompletion, AbandonableCreation {
        private final Tracker tracker;

        private ComposedFuture(Tracker tracker) {
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

    private static final class Tracker {
        private final CompletableFuture<Void> quiescence = new CompletableFuture<>();
        private final Set<Throwable> errors = Collections.newSetFromMap(new IdentityHashMap<>());
        private int active = 1;
        private Throwable failure;

        private synchronized void retain() {
            if (active == 0) {
                throw new IllegalStateException("Resource creation has already quiesced");
            }
            active++;
        }

        private void release() {
            Throwable outcome;
            synchronized (this) {
                if (--active != 0) {
                    return;
                }
                outcome = failure;
            }
            if (outcome == null) {
                quiescence.complete(null);
            } else {
                quiescence.completeExceptionally(outcome);
            }
        }

        private synchronized void record(Throwable error) {
            if (!errors.add(error)) {
                return;
            }
            if (failure == null) {
                failure = error;
            } else if (failure != error) {
                failure.addSuppressed(error);
            }
        }

        private void follow(CompletableFuture<?> future) {
            if (!(future instanceof ResourceCreationCompletion completion)) {
                return;
            }
            retain();
            try {
                completion
                        .quiescence()
                        .whenComplete(
                                (ignored, error) -> {
                                    try {
                                        if (error != null) {
                                            record(unwrap(error));
                                        }
                                    } finally {
                                        release();
                                    }
                                });
            } catch (Throwable error) {
                record(error);
                release();
            }
        }

        private CompletableFuture<Void> quiescence() {
            return quiescence.copy();
        }
    }
}
