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

import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * A Javalin {@code before} handler that answers 401 to any request without a matching {@code
 * Authorization: Bearer <token>} header.
 *
 * <p>Register it before every other {@code before} handler, so an unauthenticated caller learns
 * nothing about the API, not even which Content-Types it accepts.
 */
public final class BearerTokenAuth implements Handler {
    /** Environment variable that holds the shared worker token. */
    public static final String TOKEN_ENV = "OMB_WORKER_TOKEN";

    static final String AUTHORIZATION = "Authorization";
    static final String BEARER_PREFIX = "Bearer ";

    private final byte[] expected;
    private final Set<String> openPaths;

    /**
     * Creates a handler that requires {@code token} on every path except {@code openPaths}.
     *
     * @param token the expected token; must not be blank
     * @param openPaths exact request paths served without a token, such as {@code /metrics}
     */
    public BearerTokenAuth(String token, Set<String> openPaths) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("Worker token must not be blank");
        }
        this.expected = token.getBytes(StandardCharsets.UTF_8);
        this.openPaths = Set.copyOf(openPaths);
    }

    @Override
    public void handle(Context ctx) {
        if (openPaths.contains(ctx.path()) || matches(ctx.header(AUTHORIZATION))) {
            return;
        }
        ctx.header("WWW-Authenticate", "Bearer");
        ctx.status(HttpStatus.UNAUTHORIZED).result("Missing or invalid bearer token");
        ctx.skipRemainingHandlers();
    }

    boolean matches(@Nullable String authorization) {
        if (authorization == null
                || authorization.length() < BEARER_PREFIX.length()
                || !authorization.regionMatches(true, 0, BEARER_PREFIX, 0, BEARER_PREFIX.length())) {
            return false;
        }
        byte[] presented =
                authorization.substring(BEARER_PREFIX.length()).getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(expected, presented);
    }

    /**
     * Resolves the shared worker token from {@code tokenFile} if given, otherwise from the {@value
     * #TOKEN_ENV} environment variable. The token is trimmed; a blank result means no token.
     *
     * @param environment the process environment, usually {@link System#getenv()}
     * @param tokenFile a file holding the token, or {@code null}
     * @return the token, or empty when none is configured
     * @throws IllegalArgumentException when {@code tokenFile} is given but blank
     * @throws UncheckedIOException when {@code tokenFile} cannot be read
     */
    public static Optional<String> resolveToken(
            Map<String, String> environment, @Nullable Path tokenFile) {
        if (tokenFile != null) {
            return Optional.of(readTokenFile(tokenFile));
        }
        String token = environment.get(TOKEN_ENV);
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(token.strip());
    }

    /**
     * Reads a bearer token from {@code tokenFile} as UTF-8, stripped of surrounding whitespace.
     *
     * @param tokenFile a file holding the token
     * @return the token, never blank
     * @throws IllegalArgumentException when the file is blank
     * @throws UncheckedIOException when the file cannot be read
     */
    public static String readTokenFile(Path tokenFile) {
        String token;
        try {
            token = Files.readString(tokenFile, StandardCharsets.UTF_8).strip();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read token file " + tokenFile, e);
        }
        if (token.isEmpty()) {
            throw new IllegalArgumentException("Token file " + tokenFile + " is empty");
        }
        return token;
    }
}
