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

import java.util.concurrent.CompletableFuture;

/**
 * Optional completion barrier for a resource-creation future.
 *
 * <p>The barrier completes after creation suppliers, registered completion callbacks, and cleanup
 * have finished, even when the result future has already failed or been cancelled. Cleanup failures
 * complete it exceptionally. A non-cooperative creation may never finish, so callers should use a
 * bounded wait and must not wait synchronously inside a result-future completion callback.
 */
public interface ResourceCreationCompletion {
    /**
     * Returns a future that completes when creation work and untransferred-resource cleanup quiesce.
     *
     * @return the quiescence barrier
     */
    CompletableFuture<Void> quiescence();
}
