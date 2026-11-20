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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.beust.jcommander.JCommander;
import io.github.mihkels.omb.worker.commands.CountersStats;
import io.github.mihkels.omb.worker.commands.RunContext;
import io.github.mihkels.omb.worker.commands.TopicsInfo;
import io.github.mihkels.omb.worker.commands.WorkerHealth;
import io.github.mihkels.omb.worker.commands.WorkerHealth.DriverState;
import io.javalin.Javalin;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Exercises the worker API with a bearer token configured, as {@code benchmark-worker} serves it.
 */
class AuthenticatedWorkerHttpTest {
    private static final String TOKEN = "correct-horse-battery-staple";
    private static final String LOOPBACK = "127.0.0.1";

    private final Worker worker = mock(Worker.class);
    private final List<String> seenAuthorization = Collections.synchronizedList(new ArrayList<>());
    private Javalin app;
    private String host;

    @AfterEach
    void stopServer() {
        if (app != null) {
            app.stop();
        }
    }

    private void start(String... workerArgs) {
        BenchmarkWorker.Arguments arguments = new BenchmarkWorker.Arguments();
        JCommander.newBuilder().addObject(arguments).build().parse(workerArgs);
        BearerTokenAuth auth =
                BenchmarkWorker.authFor(arguments, Map.of(BearerTokenAuth.TOKEN_ENV, " " + TOKEN + "\n"));
        WorkerHandler handler = new WorkerHandler(worker, auth);
        app =
                Javalin.create(config -> {
                    config.startup.showJavalinBanner = false;
                    config.routes.before(
                            ctx -> seenAuthorization.add(String.valueOf(ctx.header("Authorization"))));
                    handler.configure(config);
                    config.routes.get(HttpWorkerClient.METRICS_PATH, ctx -> ctx.result("metric 1"));
                });
        app.start(LOOPBACK, 0);
        host = "http://" + LOOPBACK + ":" + app.port();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(
            strings = {"Bearer wrong", "Bearer", "Basic Y29ycmVjdA==", TOKEN, "Bearer  " + TOKEN})
    void rejectsRequestsWithoutTheRightTokenBeforeAnyOtherCheck(String authorization)
            throws Exception {
        start();
        HttpResponse<String> getStats = send(get(WorkerHandler.COUNTERS_STATS), authorization);
        assertThat(getStats.statusCode()).isEqualTo(401);
        assertThat(getStats.headers().firstValue("WWW-Authenticate")).contains("Bearer");
        // A text/plain POST would get 415 if the Content-Type check ran first
        assertThat(send(post(WorkerHandler.STOP_ALL, "text/plain"), authorization).statusCode())
                .isEqualTo(401);
        assertThat(send(post(WorkerHandler.STOP_ALL, "application/json"), authorization).statusCode())
                .isEqualTo(401);
        assertThat(send(get("/no-such-route"), authorization).statusCode()).isEqualTo(401);
        verifyNoInteractions(worker);
    }

    @Test
    void acceptsTheRightTokenAndStillChecksContentType() throws Exception {
        start();
        String bearer = "Bearer " + TOKEN;
        assertThat(send(post(WorkerHandler.STOP_ALL, "application/json"), bearer).statusCode())
                .isEqualTo(200);
        verify(worker).stopAll();
        assertThat(send(post(WorkerHandler.STOP_ALL, "text/plain"), bearer).statusCode())
                .isEqualTo(415);
    }

    @Test
    void leavesMetricsOpenByDefault() throws Exception {
        start();
        HttpResponse<String> response = send(get(HttpWorkerClient.METRICS_PATH), null);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("metric 1");
    }

    @Test
    void protectsMetricsWhenAsked() throws Exception {
        start("--protect-metrics");
        assertThat(send(get(HttpWorkerClient.METRICS_PATH), null).statusCode()).isEqualTo(401);
        assertThat(send(get(HttpWorkerClient.METRICS_PATH), "Bearer " + TOKEN).statusCode())
                .isEqualTo(200);
    }

    @Test
    void clientGetsTextWithItsTokenAndReportsFailedStatus() throws Exception {
        start("--protect-metrics");
        try (HttpWorkerClient client = new HttpWorkerClient(host, TOKEN)) {
            assertThat(client.scrapeMetrics()).isEqualTo("metric 1");
        }
        assertThat(seenAuthorization).containsExactly("Bearer " + TOKEN);
        try (HttpWorkerClient client = new HttpWorkerClient(host, "wrong")) {
            Duration timeout = Duration.ofSeconds(10);
            assertThatThrownBy(() -> client.getText(HttpWorkerClient.METRICS_PATH, timeout))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("HTTP get " + host + HttpWorkerClient.METRICS_PATH)
                    .hasMessageContaining("failed with code 401");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "--protect-metrics"})
    void servesHealthWithoutTokenButHidesTheLastError(String flag) throws Exception {
        start(flag.isEmpty() ? new String[0] : new String[] {flag});
        when(worker.health())
                .thenReturn(new WorkerHealth(true, DriverState.EXITED, 42, "broker-1:9092 refused"));
        HttpResponse<String> open = send(get(WorkerHandler.HEALTH), null);
        assertThat(open.statusCode()).isEqualTo(200);
        assertThat(open.headers().firstValue("Content-Type"))
                .hasValueSatisfying(type -> assertThat(type).startsWith("application/json"));
        assertThat(open.body())
                .containsPattern("\"driver\"\\s*:\\s*\"EXITED\"")
                .containsPattern("\"exitCode\"\\s*:\\s*42")
                .doesNotContain("broker-1");
        assertThat(send(get(WorkerHandler.HEALTH), "Bearer wrong").body()).doesNotContain("broker-1");
        try (HttpWorkerClient client = new HttpWorkerClient(host, TOKEN)) {
            long before = WorkerClock.nowMicros();
            WorkerHealth health = client.health();
            long after = WorkerClock.nowMicros();
            assertThat(health.clockMicros()).isBetween(before, after);
            assertThat(health)
                    .isEqualTo(
                            new WorkerHealth(
                                    true, DriverState.EXITED, 42, "broker-1:9092 refused", health.clockMicros()));
        }
    }

    @Test
    void healthAnswers503WhenTheWorkerCannotTakeADriver() throws Exception {
        start();
        when(worker.health()).thenReturn(new WorkerHealth(false, DriverState.NOT_STARTED, null, null));
        assertThat(send(get(WorkerHandler.HEALTH), null).statusCode()).isEqualTo(503);
        try (HttpWorkerClient client = new HttpWorkerClient(host, TOKEN)) {
            assertThatThrownBy(client::health).hasMessageContaining("failed with code 503");
        }
    }

    @Test
    void clientWithTokenRoundTripsAgainstAuthenticatedWorker() throws Exception {
        start();
        when(worker.createTopics(org.mockito.ArgumentMatchers.any())).thenReturn(List.of("t-1"));
        when(worker.getCountersStats()).thenReturn(new CountersStats(4, 5, 6));
        try (HttpWorkerClient client = new HttpWorkerClient(host, TOKEN)) {
            client.setRunContext(new RunContext("run-1", "workload-1"));
            assertThat(client.createTopics(new TopicsInfo(1, 1))).containsExactly("t-1");
            assertThat(client.getCountersStats()).isEqualTo(new CountersStats(4, 5, 6));
            client.stopAll();
        }
        verify(worker).setRunContext(new RunContext("run-1", "workload-1"));
        verify(worker).stopAll();
        assertThat(seenAuthorization).isNotEmpty().allMatch(("Bearer " + TOKEN)::equals);
    }

    @Test
    void clientWithoutTokenSendsNoHeaderAndIsRejected() throws Exception {
        start();
        try (HttpWorkerClient client = new HttpWorkerClient(host)) {
            assertThatThrownBy(client::resetStats)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("401");
        }
        assertThat(seenAuthorization).containsExactly("null");
        verifyNoInteractions(worker);
    }

    private HttpRequest.Builder get(String path) {
        return HttpRequest.newBuilder(URI.create(host + path)).GET();
    }

    private HttpRequest.Builder post(String path, String contentType) {
        return HttpRequest.newBuilder(URI.create(host + path))
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofString("{}"));
    }

    private static HttpResponse<String> send(HttpRequest.Builder request, String authorization)
            throws Exception {
        if (authorization != null) {
            request.header("Authorization", authorization);
        }
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }
    }
}
