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
package io.github.mihkels.omb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WorkerDiscoveryTest {
    private final AtomicLong now = new AtomicLong();
    private final List<String> lookedUp = new ArrayList<>();

    @Test
    void buildsSortedDeduplicatedUrlsWithBracketedIpv6() throws Exception {
        List<String> urls =
                resolve(
                        "dns://omb-worker.ns.svc:8080",
                        3,
                        answers(List.of("10.0.0.2", "fd00::1", "10.0.0.10", "10.0.0.2")));

        assertThat(urls)
                .containsExactly(
                        "http://10.0.0.10:8080", "http://10.0.0.2:8080", "http://[fd00:0:0:0:0:0:0:1]:8080");
        assertThat(lookedUp).containsExactly("omb-worker.ns.svc");
    }

    @Test
    void waitsUntilTheExpectedWorkersResolve() throws Exception {
        List<String> urls =
                resolve(
                        "dns://workers:9090",
                        2,
                        answers(null, List.of("10.0.0.1"), List.of("10.0.0.1", "10.0.0.2")));

        assertThat(urls).containsExactly("http://10.0.0.1:9090", "http://10.0.0.2:9090");
        assertThat(lookedUp).hasSize(3);
        assertThat(now.get()).isEqualTo(2000);
    }

    @Test
    void failsWhenTooFewWorkersResolveBeforeTheTimeout() {
        assertThatThrownBy(
                        () -> resolve("dns://workers:8080", 3, host -> addresses(List.of("10.0.0.1"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Found 1 of 3 expected workers at dns://workers:8080 after PT5S:"
                                + " [http://10.0.0.1:8080]");
        assertThat(now.get()).isEqualTo(5000);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "dns://workers",
                "dns://workers:8080/path",
                "dns://workers:8080?x=1",
                "dns://:8080"
            })
    void rejectsAddressesWithoutHostAndPortOnly(String address) {
        assertThatThrownBy(() -> resolve(address, 2, host -> addresses(List.of("10.0.0.1"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dns://<host>:<port>");
    }

    @Test
    void convertsShortDurations() {
        var converter = new Benchmark.DurationConverter();
        assertThat(converter.convert("30s")).isEqualTo(Duration.ofSeconds(30));
        assertThat(converter.convert("5m")).isEqualTo(Duration.ofMinutes(5));
        assertThat(converter.convert("1h30m")).isEqualTo(Duration.ofMinutes(90));
        for (String invalid : List.of("0s", "-5m", "5", "five minutes")) {
            assertThatThrownBy(() -> converter.convert(invalid))
                    .hasMessageContaining("Invalid duration '" + invalid + "'");
        }
    }

    private List<String> resolve(String address, int expected, WorkerDiscovery.Resolver resolver)
            throws InterruptedException {
        return WorkerDiscovery.resolve(
                address,
                expected,
                Duration.ofSeconds(5),
                host -> {
                    lookedUp.add(host);
                    return resolver.resolve(host);
                },
                now::get,
                now::addAndGet);
    }

    // One answer per lookup; null stands for a name without records yet
    @SafeVarargs
    private static WorkerDiscovery.Resolver answers(List<String>... answers) {
        Deque<List<String>> remaining = new ArrayDeque<>();
        for (List<String> answer : answers) {
            remaining.add(answer == null ? List.of() : answer);
        }
        return host -> {
            List<String> answer = remaining.size() > 1 ? remaining.poll() : remaining.peek();
            if (answer.isEmpty()) {
                throw new UnknownHostException(host);
            }
            return addresses(answer);
        };
    }

    private static InetAddress[] addresses(List<String> ips) throws UnknownHostException {
        InetAddress[] addresses = new InetAddress[ips.size()];
        for (int i = 0; i < ips.size(); i++) {
            addresses[i] = InetAddress.getByName(ips.get(i));
        }
        return addresses;
    }
}
