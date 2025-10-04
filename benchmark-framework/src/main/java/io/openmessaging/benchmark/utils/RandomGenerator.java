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
package io.openmessaging.benchmark.utils;

import java.util.Random;

public final class RandomGenerator {

    private static final Random random = new Random();
    private static final String KAFKA_SAFE_CHARS =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";

    private RandomGenerator() {}

    public static String getRandomString() {
        // Generate a Kafka-safe random string (only alphanumerics, underscore, and hyphen)
        StringBuilder sb = new StringBuilder(7);
        for (int i = 0; i < 7; i++) {
            sb.append(KAFKA_SAFE_CHARS.charAt(random.nextInt(KAFKA_SAFE_CHARS.length())));
        }
        return sb.toString();
    }
}
