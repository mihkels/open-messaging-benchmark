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
package io.github.mihkels.omb.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.beust.jcommander.JCommander;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BenchmarkWorkerArgumentsTest {

    private static BenchmarkWorker.Arguments parse(String... args) {
        BenchmarkWorker.Arguments arguments = new BenchmarkWorker.Arguments();
        JCommander.newBuilder().addObject(arguments).build().parse(args);
        return arguments;
    }

    @Test
    void listensOnAllInterfacesByDefault() {
        BenchmarkWorker.Arguments arguments = parse();

        assertThat(arguments.getBindAddress()).isEqualTo("0.0.0.0");
        assertThat(arguments.getHttpPort()).isEqualTo(8080);
    }

    @ParameterizedTest
    @ValueSource(strings = {"-b", "--bind"})
    void acceptsABindAddress(String option) {
        BenchmarkWorker.Arguments arguments = parse(option, "127.0.0.1", "--port", "18081");

        assertThat(arguments.getBindAddress()).isEqualTo("127.0.0.1");
        assertThat(arguments.getHttpPort()).isEqualTo(18081);
    }

    @Test
    void acceptsInsecureNoAuth() throws Exception {
        assertThat(BenchmarkWorker.refuseUnauthenticated(parse("--insecure-no-auth"), null)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"127.0.0.1", "localhost", "::1"})
    void allowsNoTokenOnLoopback(String bind) throws Exception {
        assertThat(BenchmarkWorker.refuseUnauthenticated(parse("--bind", bind), null)).isEmpty();
    }

    @Test
    void refusesNoTokenOnAllInterfaces() throws Exception {
        assertThat(BenchmarkWorker.refuseUnauthenticated(parse(), null))
                .hasValueSatisfying(value ->
                        assertThat(value).contains("OMB_WORKER_TOKEN", "--token-file", "--insecure-no-auth"));
    }

    @Test
    void allowsATokenOnAllInterfaces() throws Exception {
        BenchmarkWorker.Arguments arguments = parse();
        BearerTokenAuth auth =
                BenchmarkWorker.authFor(arguments, Map.of(BearerTokenAuth.TOKEN_ENV, "secret"));

        assertThat(auth).isNotNull();
        assertThat(BenchmarkWorker.refuseUnauthenticated(arguments, auth)).isEmpty();
    }
}
