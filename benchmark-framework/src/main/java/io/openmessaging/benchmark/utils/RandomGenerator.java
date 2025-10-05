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
    private static final char[] KAFKA_SAFE_CHARS = buildKafkaSafeChars();

    private RandomGenerator() {}

    public static String getRandomString() {
        // Generate a Kafka-safe random string (only alphanumerics, underscore, and hyphen)
        StringBuilder sb = new StringBuilder(7);
        for (int i = 0; i < 7; i++) {
            sb.append(KAFKA_SAFE_CHARS[random.nextInt(KAFKA_SAFE_CHARS.length)]);
        }
        return sb.toString();
    }

    private static char[] buildKafkaSafeChars() {
        char[] chars = new char[26 + 26 + 10 + 2]; // a-z + A-Z + 0-9 + _-
        int index = 0;

        for (char c = 'a'; c <= 'z'; c++) {
            chars[index++] = c;
        }
        for (char c = 'A'; c <= 'Z'; c++) {
            chars[index++] = c;
        }
        for (char c = '0'; c <= '9'; c++) {
            chars[index++] = c;
        }
        chars[index++] = '_';
        chars[index] = '-';

        return chars;
    }
}
