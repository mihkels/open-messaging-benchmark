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
package io.openmessaging.benchmark;

import java.security.SecureRandom;
import java.util.UUID;

final class RunId {
    private static final SecureRandom RANDOM = new SecureRandom();

    private RunId() {}

    static String create() {
        long timestamp = System.currentTimeMillis();
        long mostSignificantBits =
                ((timestamp & 0xFFFFFFFFFFFFL) << 16) | 0x7000L | RANDOM.nextInt(0x1000);
        long leastSignificantBits = RANDOM.nextLong();
        leastSignificantBits = (leastSignificantBits & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(mostSignificantBits, leastSignificantBits).toString();
    }
}
