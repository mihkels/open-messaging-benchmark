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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ResultMetadataSecretKeyTest {
    /** The pattern before java:S8786 was fixed, used with {@code matches()}. */
    // Pre-#140 backtracking pattern, kept on purpose as a regression reference.
    @SuppressWarnings("java:S8786")
    private static final Pattern FORMER_SECRET_KEY =
            Pattern.compile(
                    ".*(password|passwd|passphrase|token|secret|credential|api.?key|access.?key|"
                            + "private.?key|sasl.?jaas|authorization|auth.?config).*",
                    Pattern.CASE_INSENSITIVE);

    private static final List<String> KEYS =
            List.of(
                    "password",
                    "PASSWORD",
                    "dbPassword",
                    "passwd",
                    "sslKeyPassphrase",
                    "authToken",
                    "token",
                    "clientSecret",
                    "credentials",
                    "apiKey",
                    "api_key",
                    "api.key",
                    "API-KEY",
                    "apikey",
                    "accessKey",
                    "access_key_id",
                    "privateKey",
                    "private.key",
                    "sasl.jaas.config",
                    "saslJaasConfig",
                    "Authorization",
                    "authConfig",
                    "auth.config",
                    "authconfig",
                    "bootstrap.servers",
                    "topic",
                    "api",
                    "key",
                    "apiXXkey",
                    "author",
                    "auth",
                    "access",
                    "sasl.mechanism",
                    "",
                    "x");

    @ParameterizedTest
    @ValueSource(
            strings = {
                "password",
                "dbPassword",
                "PASSWD",
                "passphrase",
                "authToken",
                "clientSecret",
                "credential",
                "apiKey",
                "api_key",
                "ApI-kEy",
                "accessKey",
                "private_key",
                "sasl.jaas.config",
                "saslJaasConfig",
                "authorization",
                "auth.config",
                "AuthConfig"
            })
    void flagsSecretKeys(String key) {
        assertThat(ResultMetadata.isSecretKey(key)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "bootstrap.servers",
                "topic",
                "api",
                "key",
                "apiXXkey",
                "author",
                "sasl.mechanism",
                ""
            })
    void keepsOrdinaryKeys(String key) {
        assertThat(ResultMetadata.isSecretKey(key)).isFalse();
    }

    @Test
    void agreesWithFormerPatternOnSingleLineKeys() {
        for (String key : KEYS) {
            assertThat(ResultMetadata.isSecretKey(key))
                    .as(key)
                    .isEqualTo(FORMER_SECRET_KEY.matcher(key).matches());
        }
    }

    @Test
    void flagsSecretFragmentEvenAcrossLineBreaks() {
        // The former ".*" could not cross a line terminator, so this key slipped through.
        assertThat(FORMER_SECRET_KEY.matcher("password\nsuffix").matches()).isFalse();
        assertThat(ResultMetadata.isSecretKey("password\nsuffix")).isTrue();
    }

    @Test
    void redactsSecretPropertiesLinesUriPasswordsAndJsonInStringValues() {
        Map<String, Object> redacted =
                ResultMetadata.redactDriverConfig(
                        Map.of(
                                "producerConfig",
                                "acks=all\nsasl.jaas.config=PlainLoginModule required password=\"x\";",
                                "serviceUrl",
                                "amqp://user:pass@host:5672",
                                "jms",
                                "{\"password\":\"x\",\"user\":\"u\"}",
                                "nested",
                                List.of(Map.of("url", "redis://:pass@redis:6379"))));

        assertThat(redacted)
                .containsEntry("producerConfig", "acks=all\nsasl.jaas.config=[REDACTED]")
                .containsEntry("serviceUrl", "amqp://user:[REDACTED]@host:5672")
                .containsEntry("jms", "{\"password\":\"[REDACTED]\",\"user\":\"u\"}")
                .containsEntry("nested", List.of(Map.of("url", "redis://:[REDACTED]@redis:6379")));
    }

    @Test
    void flagsPulsarAuthenticationBlock() {
        assertThat(ResultMetadata.isSecretKey("authentication")).isTrue();
    }

    @Test
    void redactsSecretJvmOptions() {
        assertThat(ResultMetadata.redactJvmOption("-Dkafka.sasl.jaas.config=secret"))
                .isEqualTo("-Dkafka.sasl.jaas.config=[REDACTED]");
        assertThat(ResultMetadata.redactJvmOption("--apiKey=abc")).isEqualTo("--apiKey=[REDACTED]");
        assertThat(ResultMetadata.redactJvmOption("-Xmx1g")).isEqualTo("-Xmx1g");
        assertThat(ResultMetadata.redactJvmOption("-Dtopic=password")).isEqualTo("-Dtopic=password");
    }

    @Test
    @Timeout(5)
    void handlesLongAdversarialKeysQuickly() {
        String almost = "api".repeat(50_000) + "access".repeat(50_000) + "auth".repeat(50_000);
        long start = System.nanoTime();
        assertThat(ResultMetadata.isSecretKey(almost)).isFalse();
        assertThat(ResultMetadata.isSecretKey(almost + "token")).isTrue();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
    }
}
