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

import static io.openmessaging.benchmark.worker.WorkerHandler.ADJUST_PUBLISH_RATE;
import static io.openmessaging.benchmark.worker.WorkerHandler.COUNTERS_STATS;
import static io.openmessaging.benchmark.worker.WorkerHandler.CREATE_CONSUMERS;
import static io.openmessaging.benchmark.worker.WorkerHandler.CREATE_PRODUCERS;
import static io.openmessaging.benchmark.worker.WorkerHandler.CREATE_TOPICS;
import static io.openmessaging.benchmark.worker.WorkerHandler.CUMULATIVE_LATENCIES;
import static io.openmessaging.benchmark.worker.WorkerHandler.INITIALIZE_DRIVER;
import static io.openmessaging.benchmark.worker.WorkerHandler.JSON_CONTENT_TYPE;
import static io.openmessaging.benchmark.worker.WorkerHandler.PAUSE_CONSUMERS;
import static io.openmessaging.benchmark.worker.WorkerHandler.PERIOD_STATS;
import static io.openmessaging.benchmark.worker.WorkerHandler.PROBE_PRODUCERS;
import static io.openmessaging.benchmark.worker.WorkerHandler.RESET_STATS;
import static io.openmessaging.benchmark.worker.WorkerHandler.RESUME_CONSUMERS;
import static io.openmessaging.benchmark.worker.WorkerHandler.SET_RUN_ID;
import static io.openmessaging.benchmark.worker.WorkerHandler.START_LOAD;
import static io.openmessaging.benchmark.worker.WorkerHandler.STOP_ALL;
import static io.openmessaging.benchmark.worker.WorkerHandler.YAML_CONTENT_TYPE;
import static org.asynchttpclient.Dsl.asyncHttpClient;

import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import io.openmessaging.benchmark.worker.jackson.ObjectMappers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;

public final class HttpWorkerClient implements Worker {
    private static final Logger log = LoggerFactory.getLogger(HttpWorkerClient.class);

    private static final String CONTENT_TYPE = "Content-Type";
    private static final byte[] EMPTY_BODY = new byte[0];
    private static final int HTTP_OK = 200;
    private static final Duration STOP_TIMEOUT = Duration.ofMinutes(3);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);
    private static final ObjectMapper mapper = ObjectMappers.DEFAULT.mapper();
    private static final ObjectWriter writer = ObjectMappers.DEFAULT.writer();

    private final AsyncHttpClient httpClient;
    private final String host;
    private final @Nullable String authorization;
    private final long closeTimeoutNanos;

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
        this(
                asyncHttpClient(
                        Dsl.config()
                                .setReadTimeout(Duration.ofMinutes(10))
                                .setRequestTimeout(Duration.ofMinutes(10))),
                host,
                token,
                CLOSE_TIMEOUT);
    }

    HttpWorkerClient(AsyncHttpClient httpClient, String host) {
        this(httpClient, host, null, CLOSE_TIMEOUT);
    }

    HttpWorkerClient(AsyncHttpClient httpClient, String host, Duration closeTimeout) {
        this(httpClient, host, null, closeTimeout);
    }

    HttpWorkerClient(
            AsyncHttpClient httpClient, String host, @Nullable String token, Duration closeTimeout) {
        this.httpClient = httpClient;
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
        byte[] confFileContent = Files.readAllBytes(Paths.get(configurationFile.toString()));
        sendPost(INITIALIZE_DRIVER, YAML_CONTENT_TYPE, confFileContent);
    }

    @Override
    public void setRunId(String runId) {
        sendPost(SET_RUN_ID, writer.writeValueAsBytes(runId));
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

    @Override
    public void resetStats() {
        sendPost(RESET_STATS);
    }

    @Override
    public void stopAll() {
        await(
                authorized(httpClient.preparePost(host + STOP_ALL))
                        .setHeader(CONTENT_TYPE, JSON_CONTENT_TYPE)
                        .setBody(EMPTY_BODY)
                        .setRequestTimeout(STOP_TIMEOUT)
                        .setReadTimeout(STOP_TIMEOUT)
                        .execute()
                        .toCompletableFuture()
                        .thenAccept(response -> checkResponse("post", STOP_ALL, response)));
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
        await(
                authorized(httpClient.preparePost(host + path))
                        .setHeader(CONTENT_TYPE, contentType)
                        .setBody(body)
                        .execute()
                        .toCompletableFuture()
                        .thenApply(response -> {
                            checkResponse("post", path, response);
                            return null;
                        }));
    }

    private <T> T get(String path, Class<T> clazz) {
        return await(
                authorized(httpClient.prepareGet(host + path))
                        .execute()
                        .toCompletableFuture()
                        .thenApply(r -> {
                            checkResponse("get", path, r);
                            var value = mapper.readValue(r.getResponseBody(), clazz);
                            log.debug("Received response: {}", value);
                            return value;
                        }));
    }

    private <T> T post(String path, byte[] body, Class<T> clazz) {
        return await(
                authorized(httpClient.preparePost(host + path))
                        .setHeader(CONTENT_TYPE, JSON_CONTENT_TYPE)
                        .setBody(body)
                        .execute()
                        .toCompletableFuture()
                        .thenApply(response -> {
                            checkResponse("post", path, response);
                            return mapper.readValue(response.getResponseBody(), clazz);
                        }));
    }

    // Rethrows a request's failure without the CompletionException wrapper, so callers see the HTTP
    // or connection failure itself.
    private <T> T await(CompletableFuture<T> request) {
        try {
            return request.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("HTTP request to worker " + host + " failed", cause);
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
