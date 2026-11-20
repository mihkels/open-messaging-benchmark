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

import static io.github.mihkels.omb.worker.WorkerHandler.ADJUST_PUBLISH_RATE;
import static io.github.mihkels.omb.worker.WorkerHandler.COUNTERS_STATS;
import static io.github.mihkels.omb.worker.WorkerHandler.CREATE_CONSUMERS;
import static io.github.mihkels.omb.worker.WorkerHandler.CREATE_PRODUCERS;
import static io.github.mihkels.omb.worker.WorkerHandler.CREATE_TOPICS;
import static io.github.mihkels.omb.worker.WorkerHandler.CUMULATIVE_LATENCIES;
import static io.github.mihkels.omb.worker.WorkerHandler.INITIALIZE_DRIVER;
import static io.github.mihkels.omb.worker.WorkerHandler.JSON_CONTENT_TYPE;
import static io.github.mihkels.omb.worker.WorkerHandler.PAUSE_CONSUMERS;
import static io.github.mihkels.omb.worker.WorkerHandler.PERIOD_STATS;
import static io.github.mihkels.omb.worker.WorkerHandler.PROBE_PRODUCERS;
import static io.github.mihkels.omb.worker.WorkerHandler.RESET_STATS;
import static io.github.mihkels.omb.worker.WorkerHandler.RESUME_CONSUMERS;
import static io.github.mihkels.omb.worker.WorkerHandler.SET_PHASE;
import static io.github.mihkels.omb.worker.WorkerHandler.SET_RUN_ID;
import static io.github.mihkels.omb.worker.WorkerHandler.START_LOAD;
import static io.github.mihkels.omb.worker.WorkerHandler.STOP_ALL;
import static io.github.mihkels.omb.worker.WorkerHandler.YAML_CONTENT_TYPE;
import static org.asynchttpclient.Dsl.asyncHttpClient;

import io.github.mihkels.omb.worker.commands.ConsumerAssignment;
import io.github.mihkels.omb.worker.commands.CountersStats;
import io.github.mihkels.omb.worker.commands.CumulativeLatencies;
import io.github.mihkels.omb.worker.commands.PeriodStats;
import io.github.mihkels.omb.worker.commands.ProducerWorkAssignment;
import io.github.mihkels.omb.worker.commands.RunContext;
import io.github.mihkels.omb.worker.commands.RunPhase;
import io.github.mihkels.omb.worker.commands.TopicsInfo;
import io.github.mihkels.omb.worker.commands.WorkerHealth;
import io.github.mihkels.omb.worker.jackson.ObjectMappers;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLException;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;

public final class HttpWorkerClient implements Worker {
    /** The path of the Prometheus metrics of a benchmark worker or driver process. */
    public static final String METRICS_PATH = "/metrics";

    /** The environment variable naming the PEM certificate that {@code https://} workers present. */
    public static final String CA_FILE_ENV = "OMB_WORKER_CA_FILE";

    /** The request and read timeout of {@link #scrapeMetrics()}. */
    public static final Duration SCRAPE_TIMEOUT = Duration.ofSeconds(10);

    private static final Logger log = LoggerFactory.getLogger(HttpWorkerClient.class);

    private static final String CONTENT_TYPE = "Content-Type";
    private static final byte[] EMPTY_BODY = new byte[0];
    private static final int HTTP_OK = 200;
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);
    private static final ObjectMapper mapper = ObjectMappers.DEFAULT.mapper();
    private static final ObjectWriter writer = ObjectMappers.DEFAULT.writer();

    private final AsyncHttpClient httpClient;
    private final String host;
    private final @Nullable String authorization;
    private final long closeTimeoutNanos;
    private final Duration requestTimeout;
    private final Duration stopTimeout;
    private volatile boolean lastStopSucceeded;

    /**
     * Creates a client that sends no bearer token.
     *
     * @param host the worker's base URL
     */
    public HttpWorkerClient(String host) {
        this(host, null);
    }

    /**
     * Creates a client that sends {@code Authorization: Bearer <token>} on every request.
     *
     * @param host the worker's base URL
     * @param token the shared worker token, or {@code null} to send none
     */
    public HttpWorkerClient(String host, @Nullable String token) {
        this(host, token, null);
    }

    /**
     * Creates a client that sends the bearer token and, for {@code https://} workers, trusts only the
     * given certificate.
     *
     * @param host the worker's base URL
     * @param token the shared worker token, or {@code null} to send none
     * @param caFile the PEM certificate workers present, or {@code null} for plain HTTP workers
     */
    public HttpWorkerClient(String host, @Nullable String token, @Nullable Path caFile) {
        this(host, token, caFile, WorkerTimeouts.request(), WorkerTimeouts.stop(), false);
    }

    /**
     * Creates the coordinator's client for a remote {@code BenchmarkWorker}, which speaks HTTP/2: h2
     * by ALPN over TLS, and h2c by prior knowledge over plain HTTP. The worker's driver process and
     * other HTTP/1.1-only servers need one of the constructors instead.
     *
     * @param host the worker's base URL
     * @param token the shared worker token, or {@code null} to send none
     * @param caFile the PEM certificate workers present, or {@code null} for plain HTTP workers
     * @return the client
     */
    public static HttpWorkerClient forRemoteWorker(
            String host, @Nullable String token, @Nullable Path caFile) {
        return new HttpWorkerClient(
                host, token, caFile, WorkerTimeouts.request(), WorkerTimeouts.stop(), true);
    }

    private HttpWorkerClient(
            String host,
            @Nullable String token,
            @Nullable Path caFile,
            Duration requestTimeout,
            Duration stopTimeout,
            boolean h2c) {
        this(
                asyncHttpClient(clientConfig(caFile, requestTimeout).setHttp2CleartextEnabled(h2c)),
                host,
                token,
                CLOSE_TIMEOUT,
                requestTimeout,
                stopTimeout);
    }

    static DefaultAsyncHttpClientConfig.Builder clientConfig(
            @Nullable Path caFile, Duration requestTimeout) {
        DefaultAsyncHttpClientConfig.Builder config =
                Dsl.config().setReadTimeout(requestTimeout).setRequestTimeout(requestTimeout);
        if (caFile == null) {
            return config;
        }
        try {
            // shortcut: trusts exactly the workers' own certificate and skips the host name check,
            // because workers are reached by pod IP or ad-hoc names the certificate cannot list. The
            // bearer token still authenticates the coordinator. Upgrade to per-worker certificates with
            // full verification if the worker key may be shared outside the benchmark.
            return config
                    .setSslContext(
                            SslContextBuilder.forClient()
                                    .trustManager(caFile.toFile())
                                    // Netty 4.2 checks host names by default
                                    .endpointIdentificationAlgorithm(null)
                                    // AHC adds ALPN only to contexts it builds itself
                                    .applicationProtocolConfig(
                                            new ApplicationProtocolConfig(
                                                    ApplicationProtocolConfig.Protocol.ALPN,
                                                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                                                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                                                    ApplicationProtocolNames.HTTP_2,
                                                    ApplicationProtocolNames.HTTP_1_1))
                                    .build())
                    .setDisableHttpsEndpointIdentificationAlgorithm(true);
        } catch (SSLException | IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Cannot use worker CA file " + caFile + ": " + e.getMessage(), e);
        }
    }

    HttpWorkerClient(AsyncHttpClient httpClient, String host, Duration closeTimeout) {
        this(httpClient, host, null, closeTimeout, WorkerTimeouts.request(), WorkerTimeouts.stop());
    }

    HttpWorkerClient(
            AsyncHttpClient httpClient,
            String host,
            @Nullable String token,
            Duration closeTimeout,
            Duration requestTimeout,
            Duration stopTimeout) {
        this.httpClient = httpClient;
        this.requestTimeout = requestTimeout;
        this.stopTimeout = stopTimeout;
        this.host = host;
        this.authorization =
                token == null || token.isBlank() ? null : BearerTokenAuth.BEARER_PREFIX + token;
        this.closeTimeoutNanos = closeTimeout.toNanos();
        if (closeTimeoutNanos <= 0) {
            throw new IllegalArgumentException("HTTP client close timeout must be positive");
        }
    }

    @Override
    public void initializeDriver(Path configurationFile) throws IOException {
        byte[] confFileContent = Files.readAllBytes(configurationFile);
        sendPost(INITIALIZE_DRIVER, YAML_CONTENT_TYPE, confFileContent);
    }

    @Override
    public void setRunContext(RunContext run) {
        sendPost(SET_RUN_ID, writer.writeValueAsBytes(run));
    }

    @Override
    public void setPhase(RunPhase phase) {
        sendPost(SET_PHASE, writer.writeValueAsBytes(phase));
    }

    void initializeDriverFromLocalPath(Path configurationFile) {
        sendPost(
                DriverProcessMain.INITIALIZE_DRIVER_LOCAL,
                writer.writeValueAsBytes(configurationFile.toAbsolutePath().toString()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) {
        return post(CREATE_TOPICS, writer.writeValueAsBytes(topicsInfo), List.class);
    }

    @Override
    public void createProducers(List<String> topics) {
        sendPost(CREATE_PRODUCERS, writer.writeValueAsBytes(topics));
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) {
        sendPost(CREATE_CONSUMERS, writer.writeValueAsBytes(consumerAssignment));
    }

    @Override
    public void probeProducers() {
        sendPost(PROBE_PRODUCERS);
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        log.debug(
                "Setting worker assigned publish rate to {} msgs/sec",
                producerWorkAssignment.publishRate());
        sendPost(START_LOAD, writer.writeValueAsBytes(producerWorkAssignment));
    }

    @Override
    public void adjustPublishRate(double publishRate) {
        log.debug("Adjusting worker publish rate to {} msgs/sec", publishRate);
        sendPost(ADJUST_PUBLISH_RATE, writer.writeValueAsBytes(publishRate));
    }

    @Override
    public void pauseConsumers() {
        log.info("Pausing consumers");
        sendPost(PAUSE_CONSUMERS);
    }

    @Override
    public void resumeConsumers() {
        log.info("Resuming consumers");
        sendPost(RESUME_CONSUMERS);
    }

    @Override
    public CountersStats getCountersStats() {
        return get(COUNTERS_STATS, CountersStats.class);
    }

    @Override
    public PeriodStats getPeriodStats() {
        return get(PERIOD_STATS, PeriodStats.class);
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        return get(CUMULATIVE_LATENCIES, CumulativeLatencies.class);
    }

    /**
     * Scrapes the Prometheus metrics at {@value #METRICS_PATH}, within {@link #SCRAPE_TIMEOUT}.
     *
     * @return the metrics in the Prometheus text format
     * @throws IllegalStateException if the response status is not 200
     */
    public String scrapeMetrics() {
        return getText(METRICS_PATH, SCRAPE_TIMEOUT);
    }

    /**
     * Sends an authorized GET to {@code path} and returns the response body as text, for example a
     * Prometheus scrape of {@value #METRICS_PATH}.
     *
     * @param path the request path, starting with {@code /}
     * @param timeout the request and read timeout for this call
     * @return the response body
     * @throws IllegalStateException if the response status is not 200
     */
    public String getText(String path, Duration timeout) {
        return await("get", path, timeout, fetchText(path, timeout));
    }

    /**
     * Fetches the worker's {@code /health}, within {@link #SCRAPE_TIMEOUT}.
     *
     * @return the worker's health
     * @throws IllegalStateException if the worker cannot be reached or is not ready
     */
    @Override
    public WorkerHealth health() {
        return mapper.readValue(getText(WorkerHandler.HEALTH, SCRAPE_TIMEOUT), WorkerHealth.class);
    }

    @Override
    public void resetStats() {
        sendPost(RESET_STATS);
    }

    @Override
    public void stopAll() {
        lastStopSucceeded = false;
        await(
                "post",
                STOP_ALL,
                stopTimeout,
                postText(STOP_ALL, JSON_CONTENT_TYPE, EMPTY_BODY, stopTimeout));
        lastStopSucceeded = true;
    }

    // A worker that confirmed stopAll has no driver or workload left over from a failed run
    @Override
    public boolean canReuseAfterFailure() {
        return lastStopSucceeded;
    }

    @Override
    public String id() {
        return host;
    }

    @Override
    public void close() throws Exception {
        FutureTask<Void> task =
                new FutureTask<>(() -> {
                    httpClient.close();
                    return null;
                });
        Thread.ofVirtual().name("worker-http-client-close").start(task);
        try {
            task.get(closeTimeoutNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            task.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        } catch (TimeoutException e) {
            task.cancel(true);
            throw new IOException("Timed out closing HTTP worker client " + host, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception exception) {
                throw exception;
            }
            if (e.getCause() instanceof Error error) {
                throw error;
            }
            throw new IOException("Failed to close HTTP worker client " + host, e.getCause());
        }
    }

    private BoundRequestBuilder authorized(BoundRequestBuilder request) {
        return authorization == null
                ? request
                : request.setHeader(BearerTokenAuth.AUTHORIZATION, authorization);
    }

    private void sendPost(String path) {
        sendPost(path, EMPTY_BODY);
    }

    private void sendPost(String path, byte[] body) {
        sendPost(path, JSON_CONTENT_TYPE, body);
    }

    private void sendPost(String path, String contentType, byte[] body) {
        await("post", path, requestTimeout, postText(path, contentType, body, requestTimeout));
    }

    private <T> T get(String path, Class<T> clazz) {
        return await(
                "get",
                path,
                requestTimeout,
                fetchText(path, requestTimeout)
                        .thenApply(body -> {
                            var value = mapper.readValue(body, clazz);
                            log.debug("Received response: {}", value);
                            return value;
                        }));
    }

    // An authorized GET of path whose body is returned once the status is 200
    private CompletableFuture<String> fetchText(String path, Duration timeout) {
        return timed(authorized(httpClient.prepareGet(host + path)), timeout)
                .execute()
                .toCompletableFuture()
                .thenApply(response -> {
                    checkResponse("get", path, response);
                    return response.getResponseBody();
                });
    }

    private <T> T post(String path, byte[] body, Class<T> clazz) {
        return await(
                "post",
                path,
                requestTimeout,
                postText(path, JSON_CONTENT_TYPE, body, requestTimeout)
                        .thenApply(text -> mapper.readValue(text, clazz)));
    }

    // An authorized POST of body to path whose response body is returned once the status is 200
    private CompletableFuture<String> postText(
            String path, String contentType, byte[] body, Duration timeout) {
        return timed(authorized(httpClient.preparePost(host + path)), timeout)
                .setHeader(CONTENT_TYPE, contentType)
                .setBody(body)
                .execute()
                .toCompletableFuture()
                .thenApply(response -> {
                    checkResponse("post", path, response);
                    return response.getResponseBody();
                });
    }

    private static BoundRequestBuilder timed(BoundRequestBuilder request, Duration timeout) {
        return request.setRequestTimeout(timeout).setReadTimeout(timeout);
    }

    // Rethrows a request's failure without the ExecutionException wrapper, so callers see the HTTP
    // or connection failure itself. Timeouts and connection failures name the worker, path and
    // timeout, since the client's own message does not say which call it was. Waiting stops on an
    // interrupt, so a run the watchdog aborts does not sit out the request timeout.
    private <T> T await(String method, String path, Duration timeout, CompletableFuture<T> request) {
        String call = "HTTP " + method + " " + host + path;
        try {
            return request.get();
        } catch (InterruptedException e) {
            request.cancel(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException(call + " interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            if (cause instanceof TimeoutException) {
                throw new IllegalStateException(
                        call + " timed out after " + timeout.toMillis() + " ms", cause);
            }
            throw new IllegalStateException(call + " failed", cause);
        }
    }

    private void checkResponse(String method, String path, Response response) {
        if (response.getStatusCode() != HTTP_OK) {
            String body = response.getResponseBody();
            log.error(
                    "Failed to do HTTP {} request to {}{} -- code: {} -- {}",
                    method,
                    host,
                    path,
                    response.getStatusCode(),
                    body);
            throw new IllegalStateException(
                    "HTTP "
                            + method
                            + " "
                            + host
                            + path
                            + " failed with code "
                            + response.getStatusCode()
                            + (body == null || body.isBlank() ? "" : ": " + body));
        }
    }
}
