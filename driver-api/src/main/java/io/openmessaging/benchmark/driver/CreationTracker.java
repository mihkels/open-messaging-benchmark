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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.jspecify.annotations.Nullable;

/**
 * Counts the outstanding work of a resource creation and completes its quiescence barrier once all
 * of it has finished, failing it with the first recorded cleanup failure.
 */
final class CreationTracker {
    private final CompletableFuture<Void> quiescence = new CompletableFuture<>();
    private final Set<Throwable> errors = Collections.newSetFromMap(new IdentityHashMap<>());
    private int active = 1;
    private @Nullable Throwable failure;

    synchronized void retain() {
        if (active == 0) {
            throw new IllegalStateException("Resource creation has already quiesced");
        }
        active++;
    }

    void release() {
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

    synchronized void record(Throwable error) {
        if (!errors.add(error)) {
            return;
        }
        if (failure == null) {
            failure = error;
        } else if (failure != error) {
            failure.addSuppressed(error);
        }
    }

    void follow(CompletableFuture<?> future) {
        if (!(future instanceof ResourceCreationCompletion completion)) {
            return;
        }
        retain();
        try {
            completion
                    .quiescence()
                    .whenComplete((ignored, error) -> {
                        try {
                            if (error != null) {
                                record(Failures.unwrap(error));
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

    CompletableFuture<Void> quiescence() {
        return quiescence.copy();
    }
}
