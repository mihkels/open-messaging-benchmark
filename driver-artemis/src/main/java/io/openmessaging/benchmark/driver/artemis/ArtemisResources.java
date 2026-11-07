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
package io.openmessaging.benchmark.driver.artemis;

/** Shared cleanup for Artemis client handles that were opened before a creation failure. */
final class ArtemisResources {

    private ArtemisResources() {}

    /**
     * Closes a partially created resource, recording any close failure on the creation failure.
     *
     * @param resource the resource to close, or {@code null} when it was never created
     * @param failure the creation failure that triggered the cleanup
     */
    static void closeAfterFailure(AutoCloseable resource, Exception failure) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception closeException) {
            if (closeException != failure) {
                failure.addSuppressed(closeException);
            }
        }
    }
}
