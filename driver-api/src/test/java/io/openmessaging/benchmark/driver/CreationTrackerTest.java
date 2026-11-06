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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class CreationTrackerTest {
    @Test
    void waitsForFollowedCreationAndReportsItsFailure() {
        CreationTracker tracker = new CreationTracker();
        CompletableFuture<Void> nestedQuiescence = new CompletableFuture<>();
        CompletableFuture<Void> nested = new CreationFuture(nestedQuiescence);
        tracker.follow(nested);
        CompletableFuture<Void> quiescence = tracker.quiescence();
        tracker.release();

        assertFalse(quiescence.isDone());
        RuntimeException cleanupFailure = new RuntimeException("cleanup failed");
        nestedQuiescence.completeExceptionally(cleanupFailure);

        assertSame(
                cleanupFailure, assertThrows(CompletionException.class, quiescence::join).getCause());
    }

    @Test
    void quiescenceCanOnlyBeReachedOnce() {
        CreationTracker tracker = new CreationTracker();

        tracker.release();

        assertTrue(tracker.quiescence().isDone());
        assertThrows(IllegalStateException.class, tracker::retain);
    }

    private static final class CreationFuture extends CompletableFuture<Void>
            implements ResourceCreationCompletion {
        private final CompletableFuture<Void> quiescence;

        private CreationFuture(CompletableFuture<Void> quiescence) {
            this.quiescence = quiescence;
        }

        @Override
        public CompletableFuture<Void> quiescence() {
            return quiescence;
        }
    }
}
