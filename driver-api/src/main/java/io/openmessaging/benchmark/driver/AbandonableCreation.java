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

/**
 * Optional cooperative stop signal for a resource-creation future.
 *
 * <p>Implementations must not discard a successful resource by cancelling the future. They must
 * either complete normally with that resource or clean up any partially created resources and
 * complete exceptionally. This method must be idempotent and should not block.
 */
public interface AbandonableCreation {
    /** Requests cleanup when the enclosing batch no longer needs this creation. */
    void abandonCreation();
}
