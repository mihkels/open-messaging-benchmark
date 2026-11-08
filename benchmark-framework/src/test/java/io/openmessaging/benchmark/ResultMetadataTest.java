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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.openmessaging.benchmark.driver.SecretRedaction;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ResultMetadataTest {
    /** The JVM option rule that {@link SecretRedaction#redactString(String)} replaced. */
    private static final Pattern FORMER_JVM_OPTION = Pattern.compile("^(--?D?)([^=]+)=(.*)$");

    private static final List<Path> NO_GIT = List.of(Path.of("/nonexistent/git"));

    private static String formerRedactJvmOption(String option) {
        Matcher matcher = FORMER_JVM_OPTION.matcher(option);
        if (!matcher.matches() || !SecretRedaction.isSecretKey(matcher.group(2))) {
            return option;
        }
        return matcher.group(1) + matcher.group(2) + "=" + SecretRedaction.REDACTED;
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "-Dkafka.sasl.jaas.config=secret",
                "-Dfoo.password=x",
                "-Dpassword=hunter2",
                "-DredisPass=x",
                "-Ddb.pwd=x",
                "-Dapi.key=a:b",
                "-Dtoken=a=b=c",
                "-Dsecret=amqp://u:p@h",
                "--apiKey=abc",
                "--client-secret=abc",
                "-authToken=abc",
                "--DaccessKey=abc",
                "-Xmx1g",
                "-XX:+UseZGC",
                "-XX:MaxRAMPercentage=75",
                "-Dtopic=password",
                "-Dfile.encoding=UTF-8",
                "-Dpassword",
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "-Xlog:gc*:file=gc.log",
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"
            })
    void redactStringMasksEveryJvmOptionTheFormerRuleMasked(String option) {
        assertThat(SecretRedaction.redactString(option)).isEqualTo(formerRedactJvmOption(option));
    }

    @Test
    void redactStringAlsoMasksUriPasswordsInJvmOptions() {
        assertThat(formerRedactJvmOption("-Dbroker.url=amqp://u:p@h:5672"))
                .isEqualTo("-Dbroker.url=amqp://u:p@h:5672");
        assertThat(SecretRedaction.redactString("-Dbroker.url=amqp://u:p@h:5672"))
                .isEqualTo("-Dbroker.url=amqp://u:[REDACTED]@h:5672");
    }

    @Test
    void constructorRedactsDriverConfigIntoAnUnmodifiableMapInOrder() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("name", "driver");
        config.put("password", "driver-password");
        config.put("authToken", "driver-token");
        config.put("nested", Map.of("clientSecret", "driver-secret", "username", "user"));

        ResultMetadata metadata =
                new ResultMetadata(
                        "run", "start", "end", "1.2.3", "abc", "25", "", 1, new Workload(), config);

        assertThat(metadata.driverConfig())
                .containsExactly(
                        Map.entry("name", "driver"),
                        Map.entry("password", "[REDACTED]"),
                        Map.entry("authToken", "[REDACTED]"),
                        Map.entry("nested", Map.of("clientSecret", "[REDACTED]", "username", "user")));
        assertThat(metadata.driverConfig()).isSameAs(metadata.driverConfig());
        Map<String, Object> redacted = metadata.driverConfig();
        assertThatThrownBy(() -> redacted.put("password", "x"))
                .isInstanceOf(UnsupportedOperationException.class);
        config.put("password", "changed");
        assertThat(metadata.driverConfig()).containsEntry("password", "[REDACTED]");
    }

    @Test
    void gitCommitPrefersPropertyThenGitCommitThenGithubSha() {
        Map<String, String> env = Map.of("GIT_COMMIT", "env-commit", "GITHUB_SHA", "github-sha");

        assertThat(ResultMetadata.resolveGitCommit("prop", env, NO_GIT)).isEqualTo("prop");
        assertThat(ResultMetadata.resolveGitCommit(null, env, NO_GIT)).isEqualTo("env-commit");
        assertThat(ResultMetadata.resolveGitCommit(null, Map.of("GITHUB_SHA", "github-sha"), NO_GIT))
                .isEqualTo("github-sha");
    }

    @Test
    void gitCommitFallsBackToGitWhenNothingIsConfigured() {
        assertThat(ResultMetadata.resolveGitCommit(null, Map.of(), NO_GIT)).isEqualTo("unknown");
        assertThat(ResultMetadata.resolveGitCommit(" ", Map.of("GIT_COMMIT", "env-commit"), NO_GIT))
                .isEqualTo("unknown");
        assertThat(ResultMetadata.resolveGitCommit(null, Map.of("GIT_COMMIT", ""), NO_GIT))
                .isEqualTo("unknown");
    }
}
