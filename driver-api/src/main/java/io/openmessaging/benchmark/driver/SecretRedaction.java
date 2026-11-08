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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;

/**
 * Masks credentials in driver configuration before it is logged or written to a result file.
 *
 * <p>A value is masked when its key names a credential (see {@link #isSecretKey(String)}). String
 * values are also searched for embedded credentials:
 *
 * <ul>
 *   <li>a string that is a JSON object is parsed and masked recursively;
 *   <li>each {@code key=value} or {@code key: value} line whose key names a credential has its
 *       value masked, as in Kafka {@code producerConfig} properties strings;
 *   <li>the password in URI userinfo is masked, as in {@code amqp://user:pass@host}.
 * </ul>
 *
 * Everything else in the string is kept.
 */
public final class SecretRedaction {
    /** The text that replaces a masked value. */
    public static final String REDACTED = "[REDACTED]";

    // Separate patterns keep each regex under Sonar's complexity limit (java:S5843)
    private static final Pattern SECRET_WORD =
            Pattern.compile(
                    "password|passwd|passphrase|token|secret|credential|authorization|authentication",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern SECRET_COMPOUND =
            Pattern.compile(
                    "api.?key|access.?key|private.?key|sasl.?jaas|auth.?config", Pattern.CASE_INSENSITIVE);
    // Abbreviated password keys such as redisPass or adminPwd. Only as a suffix, so that names like
    // bypassCache or passive are not masked.
    private static final Pattern SECRET_SUFFIX =
            Pattern.compile("(?:pass|pwd)$", Pattern.CASE_INSENSITIVE);
    private static final String SCHEME_SEPARATOR = "://";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectWriter PRETTY = MAPPER.writerWithDefaultPrettyPrinter();

    private SecretRedaction() {}

    /**
     * Whether a configuration key names a credential, ignoring case: it contains a secret-like
     * fragment such as {@code password} or {@code apiKey}, or ends in an abbreviation such as {@code
     * pass} or {@code pwd}.
     *
     * @param key the configuration key
     * @return {@code true} if the value of this key must not be shown
     */
    public static boolean isSecretKey(String key) {
        return SECRET_WORD.matcher(key).find()
                || SECRET_COMPOUND.matcher(key).find()
                || SECRET_SUFFIX.matcher(key).find();
    }

    /**
     * Serializes a configuration object to pretty-printed JSON with its credentials masked, for
     * logging.
     *
     * @param config the configuration, a Jackson-serializable object
     * @return the masked JSON
     */
    public static String redactedJson(Object config) {
        Object tree = MAPPER.convertValue(config, Object.class);
        return PRETTY.writeValueAsString(redact(tree));
    }

    /**
     * Returns a copy of a map with credentials masked, recursing into nested maps, lists and strings.
     *
     * @param values the map
     * @return the masked copy, keyed by the string form of each key
     */
    public static Map<String, Object> redactMap(Map<?, ?> values) {
        Map<String, Object> redacted = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            String name = String.valueOf(key);
            redacted.put(name, isSecretKey(name) ? REDACTED : redact(value));
        });
        return redacted;
    }

    /**
     * Returns a value with credentials masked: maps and lists are copied recursively, strings are
     * searched for embedded credentials and other values are returned unchanged.
     *
     * @param value the value
     * @return the masked value
     */
    public static Object redact(Object value) {
        if (value instanceof Map<?, ?> map) {
            return redactMap(map);
        }
        if (value instanceof List<?> list) {
            List<Object> values = new ArrayList<>(list.size());
            list.forEach(item -> values.add(redact(item)));
            return values;
        }
        if (value instanceof String string) {
            return redactString(string);
        }
        return value;
    }

    /**
     * Masks credentials embedded in a string value.
     *
     * @param value the string
     * @return the string with embedded credentials masked
     */
    public static String redactString(String value) {
        if (value.isEmpty()) {
            return value;
        }
        String trimmed = value.strip();
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            Optional<String> json = redactJsonObject(trimmed);
            if (json.isPresent()) {
                return json.get();
            }
        }
        String[] lines = value.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            lines[i] = redactLine(lines[i]);
        }
        return String.join("\n", lines);
    }

    private static Optional<String> redactJsonObject(String json) {
        try {
            if (MAPPER.readValue(json, Object.class) instanceof Map<?, ?> map) {
                return Optional.of(MAPPER.writeValueAsString(redactMap(map)));
            }
        } catch (JacksonException _) {
            // Not JSON after all: fall back to line-by-line masking.
        }
        return Optional.empty();
    }

    private static String redactLine(String line) {
        int separator = firstSeparator(line);
        if (separator > 0) {
            String key = line.substring(0, separator).strip();
            if (!key.isEmpty() && hasNoWhitespace(key) && isSecretKey(key)) {
                return maskAfter(line, separator);
            }
        }
        return redactUriUserInfo(line);
    }

    private static String maskAfter(String line, int separator) {
        int end = line.endsWith("\r") ? line.length() - 1 : line.length();
        int valueStart = separator + 1;
        while (valueStart < end && Character.isWhitespace(line.charAt(valueStart))) {
            valueStart++;
        }
        if (valueStart == end) {
            // No value, for example a YAML parent key.
            return line;
        }
        return line.substring(0, valueStart) + REDACTED + line.substring(end);
    }

    private static int firstSeparator(String line) {
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '=' || c == ':') {
                return i;
            }
        }
        return -1;
    }

    private static boolean hasNoWhitespace(String key) {
        return key.chars().noneMatch(Character::isWhitespace);
    }

    // Masks the password of every scheme://user:password@host in the text.
    private static String redactUriUserInfo(String text) {
        StringBuilder out = new StringBuilder(text.length());
        int copied = 0;
        int from = 0;
        int scheme;
        while ((scheme = text.indexOf(SCHEME_SEPARATOR, from)) >= 0) {
            int start = scheme + SCHEME_SEPARATOR.length();
            int end = authorityEnd(text, start);
            int at = text.lastIndexOf('@', end - 1);
            int colon = text.indexOf(':', start);
            if (at >= start && colon >= 0 && colon + 1 < at) {
                out.append(text, copied, colon + 1).append(REDACTED);
                copied = at;
            }
            // The next "://" may start at the ':' just before the '/' that ended this authority.
            from = Math.max(end - 1, start);
        }
        return copied == 0 ? text : out.append(text, copied, text.length()).toString();
    }

    private static int authorityEnd(String text, int start) {
        int end = start;
        while (end < text.length() && !isAuthorityTerminator(text.charAt(end))) {
            end++;
        }
        return end;
    }

    private static boolean isAuthorityTerminator(char c) {
        return c == '/' || c == '?' || c == '#' || c == '"' || c == '\'' || Character.isWhitespace(c);
    }
}
