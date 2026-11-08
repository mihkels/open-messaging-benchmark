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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.javalin.Javalin;
import io.openmessaging.benchmark.utils.SecureTempFiles;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class DriverProcessServerTest {
    private static final String TOKEN = DriverProcessServer.newToken();
    private static final String LOCALHOST = "localhost";

    @TempDir Path tempDir;

    private final Worker worker = mock(Worker.class);
    private Path workDir;
    private Path config;
    private DriverProcessServer server;
    private Javalin app;
    private String host;

    @BeforeEach
    void startServer() throws IOException {
        workDir = Files.createDirectory(tempDir.resolve("work"));
        config = Files.writeString(tempDir.resolve("driver.yaml"), "driverClass: x\n");
        server = new DriverProcessServer(worker, () -> "metric 1\n", TOKEN, workDir);
        app =
                Javalin.create(c -> {
                    c.startup.showJavalinBanner = false;
                    server.configure(c);
                });
        app.start(LOCALHOST, 0);
        host = "http://" + LOCALHOST + ":" + app.port();
    }

    @AfterEach
    void stopServer() throws Exception {
        app.stop();
        server.close();
    }

    private HttpResponse<String> send(String method, String path, String body, String authorization)
            throws Exception {
        HttpRequest.Builder request =
                HttpRequest.newBuilder(URI.create(host + path))
                        .header("Content-Type", "application/json")
                        .method(
                                method,
                                body == null
                                        ? HttpRequest.BodyPublishers.noBody()
                                        : HttpRequest.BodyPublishers.ofString(body));
        if (authorization != null) {
            request.header(BearerTokenAuth.AUTHORIZATION, authorization);
        }
        try (HttpClient http = HttpClient.newHttpClient()) {
            return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private static String json(Path path) {
        return "\"" + path.toString().replace("\\", "\\\\") + "\"";
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "Bearer ", "Bearer wrong-token", "Basic abc"})
    void rejectsRequestsWithoutTheProcessToken(String authorization) throws Exception {
        DriverProcessServer.authorizeConfig(workDir, config);

        assertThat(send("GET", DriverProcessServer.METRICS_PATH, null, authorization).statusCode())
                .isEqualTo(401);
        assertThat(send("GET", WorkerHandler.PERIOD_STATS, null, authorization).statusCode())
                .isEqualTo(401);
        assertThat(
                        send("POST", DriverProcessMain.INITIALIZE_DRIVER_LOCAL, json(config), authorization)
                                .statusCode())
                .isEqualTo(401);
        // The token check runs ahead of the Content-Type check
        assertThat(send("POST", WorkerHandler.STOP_ALL, null, authorization).statusCode())
                .isEqualTo(401);
        verifyNoInteractions(worker);
    }

    @Test
    void servesMetricsAndInitializesTheAuthorizedConfigurationWithTheToken() throws Exception {
        String bearer = BearerTokenAuth.BEARER_PREFIX + TOKEN;
        DriverProcessServer.authorizeConfig(workDir, config);

        HttpResponse<String> metrics = send("GET", DriverProcessServer.METRICS_PATH, null, bearer);
        assertThat(metrics.statusCode()).isEqualTo(200);
        assertThat(metrics.body()).isEqualTo("metric 1\n");

        assertThat(
                        send("POST", DriverProcessMain.INITIALIZE_DRIVER_LOCAL, json(config), bearer)
                                .statusCode())
                .isEqualTo(200);
        verify(worker).initializeDriver(config.toAbsolutePath());
    }

    @Test
    void rejectsConfigurationPathsTheSupervisorDidNotAuthorize() throws Exception {
        String bearer = BearerTokenAuth.BEARER_PREFIX + TOKEN;
        Path other = Files.writeString(tempDir.resolve("other.yaml"), "driverClass: y\n");
        Path missing = tempDir.resolve("missing.yaml");
        Path escape = workDir.resolve("..").resolve("other.yaml");

        // Nothing authorized yet
        assertThat(
                        send("POST", DriverProcessMain.INITIALIZE_DRIVER_LOCAL, json(config), bearer)
                                .statusCode())
                .isEqualTo(400);

        DriverProcessServer.authorizeConfig(workDir, config);
        for (String body : new String[] {json(other), json(missing), json(escape), "null", "{", "1"}) {
            HttpResponse<String> response =
                    send("POST", DriverProcessMain.INITIALIZE_DRIVER_LOCAL, body, bearer);
            assertThat(response.statusCode()).as(body).isEqualTo(400);
            assertThat(response.body()).contains("not authorized");
        }
        verify(worker, never()).initializeDriver(any());
    }

    @Test
    void followsSymbolicLinksBeforeComparingWithTheAuthorizedPath() throws Exception {
        Path link = tempDir.resolve("link.yaml");
        Path outside = Files.writeString(tempDir.resolve("outside.yaml"), "driverClass: z\n");
        Files.createSymbolicLink(link, outside);
        DriverProcessServer.authorizeConfig(workDir, config);

        assertThat(server.allowedConfig(json(link))).isNull();

        DriverProcessServer.authorizeConfig(workDir, link);
        assertThat(server.allowedConfig(json(link))).isEqualTo(link);
        assertThat(server.allowedConfig(json(outside))).isEqualTo(outside);
    }

    @Test
    void rejectsADirectoryEvenWhenAuthorized() throws IOException {
        Path directory = Files.createDirectory(tempDir.resolve("dir"));
        DriverProcessServer.authorizeConfig(workDir, directory);

        assertThat(server.allowedConfig(json(directory))).isNull();
    }

    @Test
    void recordsTheAuthorizedPathInAnOwnerOnlyFile() throws IOException {
        DriverProcessServer.authorizeConfig(workDir, config);
        DriverProcessServer.authorizeConfig(workDir, config);

        Path record = workDir.resolve(DriverProcessServer.CONFIG_PATH_FILE);
        assertThat(Files.readString(record, StandardCharsets.UTF_8))
                .isEqualTo(config.toRealPath().toString());
        if (Files.getFileStore(record).supportsFileAttributeView("posix")) {
            assertThat(Files.getPosixFilePermissions(record))
                    .isEqualTo(EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
        }
        assertThatThrownBy(() -> DriverProcessServer.authorizeConfig(workDir, tempDir.resolve("none")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void generatesDistinctUrlSafeTokensOfAtLeast32Bytes() {
        Set<String> tokens = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            String token = DriverProcessServer.newToken();
            assertThat(token).matches("[A-Za-z0-9_-]+");
            assertThat(Base64.getUrlDecoder().decode(token)).hasSizeGreaterThanOrEqualTo(32);
            tokens.add(token);
        }
        assertThat(tokens).hasSize(100);
    }

    @Test
    void readsAndDeletesTheTokenFile() throws IOException {
        Path tokenFile = SecureTempFiles.createFile(tempDir.resolve(DriverProcessServer.TOKEN_FILE));
        Files.writeString(tokenFile, TOKEN + "\n", StandardCharsets.US_ASCII);

        assertThat(DriverProcessServer.readToken(tokenFile)).isEqualTo(TOKEN);
        assertThat(tokenFile).doesNotExist();
        assertThatThrownBy(() -> DriverProcessServer.readToken(tokenFile))
                .isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void rejectsABlankTokenFile() throws IOException {
        Path tokenFile = Files.writeString(tempDir.resolve("blank"), " \n");

        assertThatThrownBy(() -> DriverProcessServer.readToken(tokenFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is empty");
    }

    @Test
    void secureTempFilesRefusesToReplaceAnExistingFile() throws IOException {
        Path existing = Files.writeString(tempDir.resolve("existing"), "x");

        assertThatThrownBy(() -> SecureTempFiles.createFile(existing)).isInstanceOf(IOException.class);
    }
}
