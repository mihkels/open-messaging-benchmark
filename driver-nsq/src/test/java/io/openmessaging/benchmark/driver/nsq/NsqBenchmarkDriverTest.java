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
package io.openmessaging.benchmark.driver.nsq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;

class NsqBenchmarkDriverTest {
    @TempDir Path temp;

    @Test
    void defaultsToNsqdStandardHttpPort() {
        assertThat(new NsqConfig().nsqdHttpPort).isEqualTo(4151);
        NsqConfig config =
                new ObjectMapper(new YAMLFactory())
                        .readValue("nsqdHost: localhost\nlookupHost: localhost\n", NsqConfig.class);
        assertThat(config.nsqdHttpPort).isEqualTo(4151);
    }

    @Test
    void createsTopicsOnConfiguredHttpPort() throws Exception {
        AtomicReference<String> request = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/topic/create",
                exchange -> {
                    request.set(exchange.getRequestMethod() + " " + exchange.getRequestURI());
                    exchange.sendResponseHeaders(200, -1);
                    exchange.close();
                });
        server.start();
        try (NsqBenchmarkDriver driver = new NsqBenchmarkDriver()) {
            Path config =
                    Files.writeString(
                            temp.resolve("nsq.yaml"),
                            "nsqdHost: 127.0.0.1\nlookupHost: 127.0.0.1\nnsqdHttpPort: "
                                    + server.getAddress().getPort()
                                    + "\n");
            driver.initialize(config, null);
            driver.createTopic("benchmark-topic", 1).get(5, TimeUnit.SECONDS);
            assertThat(request.get()).isEqualTo("POST /topic/create?topic=benchmark-topic");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsInvalidHttpPort() throws Exception {
        Path config =
                Files.writeString(
                        temp.resolve("nsq.yaml"),
                        "nsqdHost: localhost\nlookupHost: localhost\nnsqdHttpPort: 65536\n");
        try (NsqBenchmarkDriver driver = new NsqBenchmarkDriver()) {
            assertThatThrownBy(() -> driver.initialize(config, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nsqdHttpPort");
        }
    }
}
