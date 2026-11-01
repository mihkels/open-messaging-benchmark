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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** Retains the first failure and subsequent cleanup failures, including late reader cleanup. */
final class DlogFailures {
    private final Set<Throwable> recorded = Collections.newSetFromMap(new IdentityHashMap<>());
    private Throwable first;
    private boolean interrupted;

    synchronized void add(Throwable failure) {
        if (!recorded.add(failure)) {
            return;
        }
        interrupted |= isInterrupted(failure);
        if (first == null) {
            first = failure;
        } else if (first != failure) {
            first.addSuppressed(failure);
        }
    }

    synchronized Throwable first() {
        return first;
    }

    void throwIfAny() throws Exception {
        Throwable failure;
        boolean restoreInterrupt;
        synchronized (this) {
            failure = first;
            restoreInterrupt = interrupted;
        }
        if (restoreInterrupt) {
            Thread.currentThread().interrupt();
        }
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
