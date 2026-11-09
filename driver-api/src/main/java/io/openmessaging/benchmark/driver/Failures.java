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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.jspecify.annotations.Nullable;

/**
 * Collects the failures of a multi-step cleanup so that every step runs and none is lost.
 *
 * <p>The first failure is the primary one and later distinct failures are suppressed into it.
 * Interruptions of the current thread are recorded with {@link #addInterruption} or {@link
 * #deferInterrupt}, which also remember to restore the thread's interrupt status in {@link
 * #restoreInterrupt()} and {@code throwIfAny}, once cleanup has finished blocking. Failures that
 * happened on other threads, including their interruptions, are recorded with {@link #add} and
 * never interrupt the current thread; {@link #addRestoringInterrupt} records them and restores the
 * interrupt as well. Cleanup whose next step must already see the interrupt closes resources with
 * {@link #closeRestoringInterruptNow} or {@link #closeAfterFailure}.
 *
 * <p>Instances are not thread-safe.
 */
public final class Failures {
    private @Nullable Throwable failure;
    private boolean interrupted;

    /**
     * Records a failure unless it is already the primary failure or suppressed into it. A {@code
     * null} cause is ignored.
     *
     * @param cause the failure
     */
    public void add(@Nullable Throwable cause) {
        failure = append(failure, cause);
    }

    /**
     * Records a failure and, if an interruption caused it on any thread, remembers to restore the
     * current thread's interrupt status in {@link #restoreInterrupt()} and {@code throwIfAny}. A
     * {@code null} cause is ignored.
     *
     * @param cause the failure
     */
    public void addRestoringInterrupt(@Nullable Throwable cause) {
        interrupted |= isInterruption(cause);
        add(cause);
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
     * Closes a resource on the current thread, recording any failure instead of throwing it. An
     * interrupted close is restored once cleanup has finished. A {@code null} resource is ignored.
     *
     * @param resource the resource to close
     */
    public void close(@Nullable AutoCloseable resource) {
        close(resource, false);
    }

    /**
     * Closes a resource on the current thread like {@link #close}, but restores the thread's
     * interrupt status at once if the close was interrupted, so that the next cleanup step already
     * sees it. A {@code null} resource is ignored.
     *
     * @param resource the resource to close
     */
    public void closeRestoringInterruptNow(@Nullable AutoCloseable resource) {
        close(resource, true);
    }

    private void close(@Nullable AutoCloseable resource, boolean restoreInterruptNow) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (InterruptedException e) {
            if (restoreInterruptNow) {
                Thread.currentThread().interrupt();
            }
            addInterruption(e);
        } catch (Throwable t) {
            add(t);
        }
    }

    /**
     * Closes resources on the current thread after {@code failure}, suppressing their close failures
     * into it. An interrupted close restores the thread's interrupt status at once.
     *
     * @param failure the failure that made the cleanup necessary
     * @param resources the resources to close, in order; {@code null} entries are ignored
     */
    public static void closeAfterFailure(Throwable failure, @Nullable AutoCloseable... resources) {
        Failures failures = new Failures();
        failures.add(failure);
        for (AutoCloseable resource : resources) {
            failures.closeRestoringInterruptNow(resource);
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
    public @Nullable Throwable failure() {
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
        rethrow(failure);
    }

    /**
     * Restores the interrupt status, then rethrows the primary failure if any was recorded. A failure
     * of type {@code type} and an unchecked failure are rethrown unchanged; any other checked failure
     * is wrapped in an {@link IllegalStateException}.
     *
     * @param type the exception type the caller declares
     * @param <X> the declared exception type
     * @throws X the primary failure
     */
    public <X extends Exception> void throwIfAny(Class<X> type) throws X {
        restoreInterrupt();
        if (failure == null) {
            return;
        }
        if (type.isInstance(failure)) {
            throw type.cast(failure);
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException(failure);
    }

    /**
     * Rethrows a failure unchanged, wrapping only a throwable that is neither an exception nor an
     * error. A {@code null} failure is ignored.
     *
     * @param failure the failure, or {@code null}
     * @throws Exception the failure
     */
    @SuppressWarnings("java:S112") // Failure aggregator: rethrows the recorded failure unchanged
    public static void rethrow(@Nullable Throwable failure) throws Exception {
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
     * Combines two failures, suppressing {@code next} into {@code primary} as {@link #suppress} does.
     *
     * @param primary the primary failure, or {@code null}
     * @param next the additional failure, or {@code null}
     * @return the primary failure, or {@code next} if there is none
     */
    public static @Nullable Throwable append(@Nullable Throwable primary, @Nullable Throwable next) {
        if (primary == null) {
            return next;
        }
        suppress(primary, next);
        return primary;
    }

    /**
     * Suppresses {@code next} into {@code primary}, unless either is {@code null}, both are the same
     * failure, or {@code next} is already suppressed into {@code primary}.
     *
     * @param primary the primary failure, or {@code null}
     * @param next the additional failure, or {@code null}
     */
    public static void suppress(@Nullable Throwable primary, @Nullable Throwable next) {
        if (primary == null || next == null || primary == next) {
            return;
        }
        for (Throwable suppressed : primary.getSuppressed()) {
            if (suppressed == next) {
                return;
            }
        }
        primary.addSuppressed(next);
    }

    /**
     * Tells whether an interruption caused a failure, anywhere in its cause chain.
     *
     * @param failure the failure, or {@code null}
     * @return whether the failure or one of its causes is an {@link InterruptedException}
     */
    public static boolean isInterruption(@Nullable Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
                return true;
            }
        }
        return false;
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
