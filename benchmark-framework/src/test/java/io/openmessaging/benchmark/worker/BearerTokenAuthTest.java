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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.beust.jcommander.JCommander;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class BearerTokenAuthTest {
    private static final String TOKEN = "s3cret-tökén";

    @TempDir Path directory;

    private final BearerTokenAuth auth = new BearerTokenAuth(TOKEN, Set.of());

    @Test
    void acceptsTheConfiguredTokenWithAnyCaseScheme() {
        assertThat(auth.matches("Bearer " + TOKEN)).isTrue();
        assertThat(auth.matches("bearer " + TOKEN)).isTrue();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(
            strings = {
                "",
                "Bearer",
                "Bearer ",
                "Bearer wrong",
                "Bearer s3cret-tökén ",
                "Bearer s3cret-töké",
                "Basic czNjcmV0",
                "s3cret-tökén",
                "Bearer  s3cret-tökén"
            })
    void rejectsMissingWrongAndMalformedHeaders(String header) {
        assertThat(auth.matches(header)).isFalse();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "  "})
    void rejectsBlankTokens(String token) {
        assertThatThrownBy(() -> new BearerTokenAuth(token, Set.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void resolvesTokenFromEnvironmentTrimmed() {
        assertThat(BearerTokenAuth.resolveToken(Map.of(BearerTokenAuth.TOKEN_ENV, " abc\n"), null))
                .contains("abc");
        assertThat(BearerTokenAuth.resolveToken(Map.of(BearerTokenAuth.TOKEN_ENV, "  "), null))
                .isEmpty();
        assertThat(BearerTokenAuth.resolveToken(Map.of(), null)).isEmpty();
    }

    @Test
    void tokenFileOverridesEnvironmentAndIsTrimmed() throws Exception {
        Path file = Files.writeString(directory.resolve("token"), "  from-file\n");
        assertThat(BearerTokenAuth.resolveToken(Map.of(BearerTokenAuth.TOKEN_ENV, "env"), file))
                .contains("from-file");
    }

    @Test
    void rejectsEmptyOrMissingTokenFile() throws Exception {
        Path empty = Files.writeString(directory.resolve("empty"), " \n");
        assertThatThrownBy(() -> BearerTokenAuth.resolveToken(Map.of(), empty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
        Path missing = directory.resolve("missing");
        assertThatThrownBy(() -> BearerTokenAuth.resolveToken(Map.of(), missing))
                .isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void workerHasNoAuthWithoutToken() {
        assertThat(BenchmarkWorker.authFor(parse(), Map.of())).isNull();
    }

    @Test
    void workerReadsTokenFileFlag() throws Exception {
        Path file = Files.writeString(directory.resolve("token"), "abc");
        BearerTokenAuth fromFile =
                BenchmarkWorker.authFor(parse("--token-file", file.toString()), Map.of());
        assertThat(fromFile).isNotNull();
        assertThat(fromFile.matches("Bearer abc")).isTrue();
    }

    private static BenchmarkWorker.Arguments parse(String... args) {
        BenchmarkWorker.Arguments arguments = new BenchmarkWorker.Arguments();
        JCommander.newBuilder().addObject(arguments).build().parse(args);
        return arguments;
    }
}
