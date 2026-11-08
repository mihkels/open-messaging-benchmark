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

import io.javalin.config.JavalinConfig;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import io.openmessaging.benchmark.utils.SecureTempFiles;
import io.openmessaging.benchmark.worker.jackson.ObjectMappers;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Set;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * The HTTP API of a driver process started by {@link DriverProcess}: the {@link WorkerHandler}
 * routes, {@code /metrics} and {@value DriverProcessMain#INITIALIZE_DRIVER_LOCAL}, all behind a
 * per-process bearer token.
 *
 * <p>The supervisor hands over the token, and the one driver configuration path the process may
 * read, through files in its owner-only work directory, never through argv or the environment.
 */
final class DriverProcessServer implements AutoCloseable {
    static final String METRICS_PATH = "/metrics";
    static final String PORT_FILE = "port";
    static final String TOKEN_FILE = "token";
    static final String CONFIG_PATH_FILE = "driver-configuration.path";

    private static final int TOKEN_BYTES = 32;
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final ObjectMapper MAPPER = ObjectMappers.DEFAULT.mapper();

    private final Worker worker;
    private final Supplier<String> metrics;
    private final Path configPathFile;
    private final WorkerHandler handler;

    /**
     * Creates the API of a driver process.
     *
     * @param worker the worker that runs the driver
     * @param metrics produces the Prometheus metrics of this process
     * @param token the token every request must carry
     * @param workDir the supervisor's work directory, holding the {@value #CONFIG_PATH_FILE} file
     */
    DriverProcessServer(Worker worker, Supplier<String> metrics, String token, Path workDir) {
        this.worker = worker;
        this.metrics = metrics;
        this.configPathFile = workDir.resolve(CONFIG_PATH_FILE);
        this.handler = new WorkerHandler(worker, new BearerTokenAuth(token, Set.of()));
    }

    /**
     * Generates a token for one driver process.
     *
     * @return {@value #TOKEN_BYTES} random bytes, base64url-encoded without padding
     */
    static String newToken() {
        byte[] bytes = new byte[TOKEN_BYTES];
        RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /**
     * Reads the token the supervisor wrote and deletes its file, so the token stays on disk only
     * until the driver process has started.
     *
     * @param tokenFile the token file
     * @return the token
     * @throws UncheckedIOException if the file cannot be read or deleted
     * @throws IllegalArgumentException if the file is blank
     */
    static String readToken(Path tokenFile) {
        String token;
        try {
            token = Files.readString(tokenFile, StandardCharsets.US_ASCII).strip();
            Files.delete(tokenFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read driver process token file " + tokenFile, e);
        }
        if (token.isEmpty()) {
            throw new IllegalArgumentException("Driver process token file " + tokenFile + " is empty");
        }
        return token;
    }

    /**
     * Records the driver configuration the driver process may read, as its real path.
     *
     * @param workDir the supervisor's owner-only work directory
     * @param driverConfigFile the driver configuration
     * @throws IOException if the configuration does not exist or the record cannot be written
     */
    static void authorizeConfig(Path workDir, Path driverConfigFile) throws IOException {
        Path real = driverConfigFile.toRealPath();
        Path record = workDir.resolve(CONFIG_PATH_FILE);
        Files.deleteIfExists(record);
        SecureTempFiles.createFile(record);
        Files.writeString(record, real.toString(), StandardCharsets.UTF_8);
    }

    /**
     * Registers the driver process routes, with the token check ahead of every other handler.
     *
     * @param config the Javalin configuration
     */
    void configure(JavalinConfig config) {
        handler.configure(config);
        config.routes.get(METRICS_PATH, ctx -> ctx.result(metrics.get()));
        config.routes.post(DriverProcessMain.INITIALIZE_DRIVER_LOCAL, this::initializeFromLocalPath);
    }

    /**
     * Closes the worker handler and with it the worker.
     *
     * @throws Exception if the worker cannot be closed
     */
    @Override
    public void close() throws Exception {
        handler.close();
    }

    private void initializeFromLocalPath(Context ctx) throws Exception {
        Path config = allowedConfig(ctx.body());
        if (config == null) {
            ctx.status(HttpStatus.BAD_REQUEST)
                    .result("Driver configuration path was not authorized by the supervisor");
            return;
        }
        worker.initializeDriver(config);
    }

    /**
     * Resolves a driver configuration path sent by the supervisor. The path is kept as sent, so a
     * driver can still resolve files next to its configuration, but it must lead to the regular file
     * the supervisor recorded with {@link #authorizeConfig}.
     *
     * @param body the JSON string holding the path
     * @return the absolute path, or {@code null} if it is not the authorized configuration
     */
    @Nullable Path allowedConfig(String body) {
        try {
            String requested = MAPPER.readValue(body, String.class);
            if (requested == null) {
                return null;
            }
            Path path = Path.of(requested).toAbsolutePath();
            Path real = path.toRealPath();
            Path authorized = Path.of(Files.readString(configPathFile, StandardCharsets.UTF_8));
            return real.equals(authorized) && Files.isRegularFile(real) ? path : null;
        } catch (JacksonException | InvalidPathException | IOException _) {
            return null;
        }
    }
}
