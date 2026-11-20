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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import io.javalin.Javalin;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Requests a browser could send on a web page's behalf, including after DNS rebinding. */
class BrowserRequestRejectionTest {
    private static final String TOKEN = "correct-horse-battery-staple";

    private final Worker worker = mock(Worker.class);
    private Javalin app;

    @AfterEach
    void stopServer() {
        if (app != null) {
            app.stop();
        }
    }

    private void start(@Nullable BearerTokenAuth auth) {
        WorkerHandler handler = new WorkerHandler(worker, auth);
        app =
                Javalin.create(config -> {
                    config.startup.showJavalinBanner = false;
                    handler.configure(config);
                });
        app.start("127.0.0.1", 0);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"evil.example:8080", "evil.example", "127.0.0.1.evil.example", "localhost.evil"})
    void tokenlessWorkerRejectsARebindingHost(String host) throws IOException {
        start(null);
        assertThat(stopAll(host, null, null)).isEqualTo(403);
        verifyNoInteractions(worker);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"127.0.0.1:8080", "10.1.2.3", "localhost:18081", "LOCALHOST", "[::1]:8080"})
    void tokenlessWorkerAcceptsAddressHosts(String host) throws IOException {
        start(null);
        assertThat(stopAll(host, null, null)).isEqualTo(200);
    }

    @Test
    void workerWithATokenAcceptsAnyHost() throws IOException {
        start(new BearerTokenAuth(TOKEN, Set.of()));
        assertThat(stopAll("worker-1:8080", null, TOKEN)).isEqualTo(200);
        assertThat(stopAll("evil.example", null, null)).isEqualTo(401);
    }

    @Test
    void rejectsAnyOriginHeader() throws IOException {
        start(null);
        assertThat(stopAll("127.0.0.1", "http://127.0.0.1", null)).isEqualTo(403);
        app.stop();
        start(new BearerTokenAuth(TOKEN, Set.of()));
        assertThat(stopAll("127.0.0.1", "http://evil.example", TOKEN)).isEqualTo(403);
        verifyNoInteractions(worker);
    }

    @Test
    void acceptsAMissingHost() {
        assertThat(WorkerHandler.isAddressHost(null)).isTrue();
    }

    // java.net.http refuses to set Host, so the request is written by hand
    private int stopAll(String host, @Nullable String origin, @Nullable String token)
            throws IOException {
        StringBuilder request =
                new StringBuilder("POST " + WorkerHandler.STOP_ALL + " HTTP/1.1\r\n")
                        .append("Host: ")
                        .append(host)
                        .append("\r\nContent-Type: application/json\r\nContent-Length: 0\r\n")
                        .append("Connection: close\r\n");
        if (origin != null) {
            request.append("Origin: ").append(origin).append("\r\n");
        }
        if (token != null) {
            request.append("Authorization: Bearer ").append(token).append("\r\n");
        }
        request.append("\r\n");
        try (Socket socket = new Socket("127.0.0.1", app.port())) {
            OutputStream out = socket.getOutputStream();
            out.write(request.toString().getBytes(StandardCharsets.US_ASCII));
            out.flush();
            InputStream in = socket.getInputStream();
            String statusLine = new String(in.readNBytes(12), StandardCharsets.US_ASCII);
            return Integer.parseInt(statusLine.substring(9, 12));
        }
    }
}
