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
package io.openmessaging.benchmark.worker;

import java.util.ArrayList;
import java.util.List;

/** The JVM options of driver processes, and their encoding in a {@code java} argument file. */
final class JvmOptions {
    static final String PROPERTY = "omb.driver.jvmOpts";
    static final String ENV = "OMB_DRIVER_JVM_OPTS";

    static final List<String> DEFAULTS =
            List.of(
                    "-XX:+UseG1GC",
                    "--enable-native-access=ALL-UNNAMED",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                    "--add-opens=java.base/java.util.zip=ALL-UNNAMED",
                    "-Dio.netty.tryReflectionSetAccessible=true",
                    "-Dorg.apache.pulsar.shade.io.netty.tryReflectionSetAccessible=true",
                    "-Dio.netty.leakDetection.level=disabled");

    private JvmOptions() {}

    /**
     * The options configured with the {@value #PROPERTY} system property or the {@value #ENV}
     * environment variable.
     *
     * @return the configured options, or {@link #DEFAULTS} if none are configured
     * @throws IllegalArgumentException if the configured options contain an unterminated quote
     */
    static List<String> configured() {
        String configured = System.getProperty(PROPERTY, System.getenv(ENV));
        if (configured == null || configured.isBlank()) {
            return DEFAULTS;
        }
        return parse(configured);
    }

    /**
     * Splits options like a POSIX shell does for words: whitespace separates options, single quotes
     * keep text literally, double quotes keep whitespace and accept {@code \"} and {@code \\}, and
     * outside quotes a backslash escapes whitespace and quote characters.
     *
     * @param options the options as one string
     * @return the options
     * @throws IllegalArgumentException if a quote is not terminated
     */
    static List<String> parse(String options) {
        return new Tokenizer(options).tokens();
    }

    /** Splits the options string one character, or one escape sequence, at a time. */
    private static final class Tokenizer {
        private final String options;
        private final List<String> result = new ArrayList<>();
        private final StringBuilder current = new StringBuilder();
        private char quote = 0;
        private boolean started = false;

        Tokenizer(String options) {
            this.options = options;
        }

        List<String> tokens() {
            int i = 0;
            while (i < options.length()) {
                i = consume(i);
            }
            if (quote != 0) {
                throw new IllegalArgumentException("Unterminated quote in driver JVM options");
            }
            endToken();
            return List.copyOf(result);
        }

        // Consumes the character at i and returns the index of the next one.
        private int consume(int i) {
            char c = options.charAt(i);
            if (isEscape(c, i)) {
                current.append(options.charAt(i + 1));
                started = true;
                return i + 2;
            }
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                } else {
                    current.append(c);
                }
            } else if (c == '"' || c == '\'') {
                quote = c;
                started = true;
            } else if (Character.isWhitespace(c)) {
                endToken();
            } else {
                current.append(c);
                started = true;
            }
            return i + 1;
        }

        private boolean isEscape(char c, int i) {
            if (c != '\\' || quote == '\'' || i + 1 >= options.length()) {
                return false;
            }
            char next = options.charAt(i + 1);
            if (quote == '"') {
                return next == '"' || next == '\\';
            }
            return Character.isWhitespace(next) || next == '"' || next == '\'';
        }

        private void endToken() {
            if (started) {
                result.add(current.toString());
                current.setLength(0);
                started = false;
            }
        }
    }

    /**
     * Quotes one argument for a {@code java @argfile}.
     *
     * @param argument the argument
     * @return the argument in double quotes, with backslashes and double quotes escaped
     */
    static String quoteForArgumentFile(String argument) {
        return "\"" + argument.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
