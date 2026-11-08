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
package io.openmessaging.benchmark.driver.bookkeeper;

import io.openmessaging.benchmark.driver.Failures;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Thread-safe {@link Failures} that retains the first failure and subsequent cleanup failures,
 * including late reader cleanup. A failure caused by an interruption, on any thread, restores the
 * interrupt in {@link #throwIfAny()}.
 */
final class DlogFailures {
    private final Failures failures = new Failures();
    private boolean interrupted;

    synchronized void add(Throwable failure) {
        interrupted |= isInterrupted(failure);
        failures.addDistinct(failure);
    }

    synchronized Throwable first() {
        return failures.failure();
    }

    void throwIfAny() throws Exception {
        Throwable failure;
        boolean restoreInterrupt;
        synchronized (this) {
            failure = failures.failure();
            restoreInterrupt = interrupted;
        }
        if (restoreInterrupt) {
            Thread.currentThread().interrupt();
        }
        Failures.rethrow(failure);
    }

    static boolean isInterrupted(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }
}
