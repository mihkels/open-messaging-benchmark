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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FailuresTest {

    @AfterEach
    void clearInterrupt() {
        Thread.interrupted();
    }

    @Test
    void keepsTheFirstFailureAndSuppressesTheRest() {
        Failures failures = new Failures();
        IOException first = new IOException("first");
        IllegalStateException second = new IllegalStateException("second");

        failures.add(null);
        failures.add(first);
        failures.add(second);
        failures.add(first);

        assertSame(first, failures.failure());
        assertArrayEquals(new Throwable[] {second}, first.getSuppressed());
    }

    @Test
    void addDistinctDoesNotSuppressTheSameFailureTwice() {
        Failures failures = new Failures();
        IOException first = new IOException("first");
        IllegalStateException second = new IllegalStateException("second");

        failures.addDistinct(null);
        failures.addDistinct(first);
        failures.addDistinct(second);
        failures.addDistinct(second);
        failures.addDistinct(first);

        assertSame(first, failures.failure());
        assertArrayEquals(new Throwable[] {second}, first.getSuppressed());
    }

    @Test
    void appendDistinctSkipsFailuresAlreadySuppressed() {
        IOException primary = new IOException("primary");
        IllegalStateException next = new IllegalStateException("next");

        assertSame(next, Failures.appendDistinct(null, next));
        assertSame(primary, Failures.appendDistinct(primary, next));
        assertSame(primary, Failures.appendDistinct(primary, next));
        assertSame(primary, Failures.appendDistinct(primary, null));

        assertArrayEquals(new Throwable[] {next}, primary.getSuppressed());
    }

    @Test
    void rethrowsFailuresUnchanged() {
        IOException exception = new IOException("exception");
        AssertionError error = new AssertionError("error");
        Throwable other = new Throwable("other");

        assertSame(exception, assertThrows(IOException.class, () -> Failures.rethrow(exception)));
        assertSame(error, assertThrows(AssertionError.class, () -> Failures.rethrow(error)));
        assertSame(
                other, assertThrows(IllegalStateException.class, () -> Failures.rethrow(other)).getCause());
        assertDoesNotThrow(() -> Failures.rethrow(null));
    }

    @Test
    void closesEveryResourceAndRecordsFailures() {
        Failures failures = new Failures();
        IOException failure = new IOException("close failed");
        boolean[] closed = new boolean[1];

        failures.close(() -> {
            throw failure;
        });
        failures.close(null);
        failures.close(() -> closed[0] = true);

        assertTrue(closed[0]);
        assertSame(failure, assertThrows(IOException.class, failures::throwIfAny));
    }

    @Test
    void defersTheInterruptUntilCleanupThrows() {
        Failures failures = new Failures();
        Thread.currentThread().interrupt();

        failures.deferInterrupt("before cleanup");

        assertFalse(Thread.currentThread().isInterrupted());
        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> failures.throwIfAny("cleanup failed"));
        assertEquals("cleanup failed", thrown.getMessage());
        assertEquals("before cleanup", thrown.getCause().getMessage());
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void restoresTheInterruptOfAnInterruptedClose() {
        Failures failures = new Failures();

        failures.close(() -> {
            throw new InterruptedException("close interrupted");
        });
        failures.restoreInterrupt();

        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void doesNotInterruptForAnotherThreadsInterruption() {
        Failures failures = new Failures();

        failures.add(new InterruptedException("producer task interrupted"));

        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> failures.throwIfAny("stop failed"));
        assertInstanceOf(InterruptedException.class, thrown.getCause());
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    void doesNothingWithoutFailures() throws Exception {
        Failures failures = new Failures();

        failures.throwIfAny();
        failures.throwIfAny("unused");

        assertFalse(failures.hasFailure());
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    void unwrapsCompletionAndExecutionWrappers() {
        IOException cause = new IOException("cause");

        Throwable wrapped = new CompletionException(new ExecutionException(cause));

        assertSame(cause, Failures.unwrap(wrapped));
        assertInstanceOf(CompletionException.class, Failures.unwrap(new CompletionException(null)));
    }
}
