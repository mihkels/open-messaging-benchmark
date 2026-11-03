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

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Collects the failures of a multi-step cleanup so that every step runs and none is lost.
 *
 * <p>The first failure is the primary one and later distinct failures are suppressed into it.
 * Interruptions of the current thread are recorded with {@link #addInterruption} or {@link
 * #deferInterrupt}, which also remember to restore the thread's interrupt status in {@link
 * #restoreInterrupt()} and {@code throwIfAny}, once cleanup has finished blocking. Failures that
 * happened on other threads, including their interruptions, are recorded with {@link #add} and
 * never interrupt the current thread.
 *
 * <p>Instances are not thread-safe.
 */
public final class Failures {
    private Throwable failure;
    private boolean interrupted;

    /**
     * Records a failure. A {@code null} cause is ignored.
     *
     * @param cause the failure
     */
    public void add(Throwable cause) {
        failure = append(failure, cause);
    }

    /**
     * Records an interruption of the current thread and remembers to restore its interrupt status.
     *
     * @param interruption the interruption caught on the current thread
     */
    public void addInterruption(InterruptedException interruption) {
        interrupted = true;
        add(interruption);
    }

    /**
     * Clears the current thread's interrupt status so that cleanup can block, recording it as a
     * failure to be restored later.
     *
     * @param message describes the step the interruption arrived before
     */
    public void deferInterrupt(String message) {
        if (Thread.interrupted()) {
            addInterruption(new InterruptedException(message));
        }
    }

    /**
     * Closes a resource on the current thread, recording any failure instead of throwing it. A {@code
     * null} resource is ignored.
     *
     * @param resource the resource to close
     */
    public void close(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (InterruptedException e) {
            addInterruption(e);
        } catch (Throwable t) {
            add(t);
        }
    }

    public boolean hasFailure() {
        return failure != null;
    }

    /**
     * The primary failure, with later failures suppressed into it.
     *
     * @return the failure, or {@code null} if none was recorded
     */
    public Throwable failure() {
        return failure;
    }

    /** Restores the current thread's interrupt status if an interruption was recorded. */
    public void restoreInterrupt() {
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Restores the interrupt status, then throws if any failure was recorded.
     *
     * @param message the message of the thrown exception
     * @throws IllegalStateException wrapping the primary failure
     */
    public void throwIfAny(String message) {
        restoreInterrupt();
        if (failure != null) {
            throw new IllegalStateException(message, failure);
        }
    }

    /**
     * Restores the interrupt status, then rethrows the primary failure if any was recorded.
     *
     * @throws Exception the primary failure
     */
    public void throwIfAny() throws Exception {
        restoreInterrupt();
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * Combines two failures, suppressing {@code next} into {@code primary}.
     *
     * @param primary the primary failure, or {@code null}
     * @param next the additional failure, or {@code null}
     * @return the primary failure, or {@code next} if there is none
     */
    public static Throwable append(Throwable primary, Throwable next) {
        if (primary == null) {
            return next;
        }
        if (next != null && primary != next) {
            primary.addSuppressed(next);
        }
        return primary;
    }

    /**
     * Strips {@link CompletionException} and {@link ExecutionException} wrappers.
     *
     * @param failure the possibly wrapped failure
     * @return the underlying cause
     */
    public static Throwable unwrap(Throwable failure) {
        Throwable result = failure;
        while ((result instanceof CompletionException || result instanceof ExecutionException)
                && result.getCause() != null) {
            result = result.getCause();
        }
        return result;
    }
}
