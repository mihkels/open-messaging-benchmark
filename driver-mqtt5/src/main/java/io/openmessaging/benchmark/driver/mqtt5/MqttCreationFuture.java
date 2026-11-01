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
package io.openmessaging.benchmark.driver.mqtt5;

import io.openmessaging.benchmark.driver.AbandonableCreation;
import io.openmessaging.benchmark.driver.ResourceCreationCompletion;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

final class MqttCreationFuture<T extends AutoCloseable> extends CompletableFuture<T>
        implements AbandonableCreation, ResourceCreationCompletion {
    private final T resource;
    private final Object lock = new Object();
    private final CompletableFuture<Void> cleanup = new CompletableFuture<>();
    private final CompletableFuture<Void> quiescence = new CompletableFuture<>();
    private int active = 1;
    private boolean connected;
    private boolean cleanupStarted;
    private boolean handoffStarted;
    private boolean disconnectedBeforeHandoff;
    private Throwable cleanupFailure;

    MqttCreationFuture(T resource) {
        this.resource = resource;
        whenComplete(
                (value, failure) -> {
                    if (isCancelled()) {
                        boolean shouldClose;
                        synchronized (lock) {
                            shouldClose = connected;
                        }
                        if (shouldClose) {
                            closeUntransferred();
                        }
                    }
                });
    }

    void connected() {
        synchronized (lock) {
            connected = true;
        }
        if (isCancelled()) {
            closeUntransferred();
        }
    }

    void succeed() {
        retain();
        startRetained(
                () -> {
                    boolean disconnected;
                    synchronized (lock) {
                        disconnected = disconnectedBeforeHandoff;
                        if (!disconnected) {
                            handoffStarted = true;
                        }
                    }
                    if (disconnected) {
                        fail(new IllegalStateException("MQTT client disconnected before creation completed"));
                        return;
                    }
                    if (!complete(resource) && isCancelled()) {
                        closeUntransferred();
                    }
                },
                this::fail);
    }

    boolean stopReconnectOnDisconnect() {
        synchronized (lock) {
            if (!handoffStarted) {
                disconnectedBeforeHandoff = true;
                return true;
            }
            return isCancelled() || cleanupStarted;
        }
    }

    void fail(Throwable failure) {
        if (isCancelled()) {
            closeUntransferred();
            return;
        }
        closeUntransferred()
                .whenComplete(
                        (unused, closeError) -> {
                            Throwable cause = unwrap(closeError);
                            if (cause != null && cause != failure) {
                                failure.addSuppressed(cause);
                            }
                            completeExceptionally(failure);
                        });
    }

    void retain() {
        synchronized (lock) {
            if (active == 0) {
                throw new IllegalStateException("MQTT creation already quiesced");
            }
            active++;
        }
    }

    void finished() {
        Throwable failure;
        synchronized (lock) {
            if (--active != 0) {
                return;
            }
            failure = cleanupFailure;
        }
        if (failure == null) {
            quiescence.complete(null);
        } else {
            quiescence.completeExceptionally(failure);
        }
    }

    private CompletableFuture<Void> closeUntransferred() {
        synchronized (lock) {
            if (cleanupStarted) {
                return cleanup;
            }
            cleanupStarted = true;
            active++;
        }
        startRetained(
                () -> {
                    try {
                        resource.close();
                        cleanup.complete(null);
                    } catch (Throwable failure) {
                        if (failure instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        recordCleanupFailure(failure);
                    }
                },
                this::recordCleanupFailure);
        return cleanup;
    }

    private void recordCleanupFailure(Throwable failure) {
        synchronized (lock) {
            cleanupFailure = failure;
        }
        cleanup.completeExceptionally(failure);
    }

    private void startRetained(Runnable action, Consumer<Throwable> launchFailure) {
        try {
            Thread.startVirtualThread(
                    () -> {
                        try {
                            action.run();
                        } finally {
                            finished();
                        }
                    });
        } catch (RuntimeException | Error failure) {
            try {
                launchFailure.accept(failure);
            } finally {
                finished();
            }
        }
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    @Override
    public void abandonCreation() {
        cancel(false);
    }

    @Override
    public CompletableFuture<Void> quiescence() {
        return quiescence.copy();
    }
}
