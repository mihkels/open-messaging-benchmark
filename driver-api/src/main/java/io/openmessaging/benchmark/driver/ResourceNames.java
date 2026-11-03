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

import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;

/** Helpers for naming broker-side resources such as namespaces, groups and client ids. */
public final class ResourceNames {
    private static final int SUFFIX_BYTES = 5;

    private ResourceNames() {}

    /**
     * A short random suffix that keeps resource names from different runs apart.
     *
     * @return seven URL-safe base64 characters
     */
    public static String randomSuffix() {
        byte[] buffer = new byte[SUFFIX_BYTES];
        ThreadLocalRandom.current().nextBytes(buffer);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buffer);
    }
}
