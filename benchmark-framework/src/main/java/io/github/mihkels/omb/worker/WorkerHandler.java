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

import io.github.mihkels.omb.plugin.PluginException;
import io.github.mihkels.omb.utils.SecureTempFiles;
import io.github.mihkels.omb.worker.commands.ConsumerAssignment;
import io.github.mihkels.omb.worker.commands.ProducerWorkAssignment;
import io.github.mihkels.omb.worker.commands.RunContext;
import io.github.mihkels.omb.worker.commands.RunPhase;
import io.github.mihkels.omb.worker.commands.TopicsInfo;
import io.github.mihkels.omb.worker.commands.WorkerHealth;
import io.github.mihkels.omb.worker.jackson.ObjectMappers;
import io.javalin.config.JavalinConfig;
import io.javalin.config.RoutesConfig;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.HttpStatus;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;

public class WorkerHandler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WorkerHandler.class);

    private static final ObjectMapper mapper = ObjectMappers.DEFAULT.mapper();
    private static final ObjectWriter writer = ObjectMappers.DEFAULT.writer();
    private static final Pattern IPV4 = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3}");

    public static final String INITIALIZE_DRIVER = "/initialize-driver";
    public static final String SET_RUN_ID = "/set-run-id";
    public static final String SET_PHASE = "/set-phase";
    public static final String CREATE_TOPICS = "/create-topics";
    public static final String CREATE_PRODUCERS = "/create-producers";
    public static final String PROBE_PRODUCERS = "/probe-producers";
    public static final String CREATE_CONSUMERS = "/create-consumers";
    public static final String PAUSE_CONSUMERS = "/pause-consumers";
    public static final String RESUME_CONSUMERS = "/resume-consumers";
    public static final String START_LOAD = "/start-load";
    public static final String ADJUST_PUBLISH_RATE = "/adjust-publish-rate";
    public static final String STOP_ALL = "/stop-all";
    public static final String PERIOD_STATS = "/period-stats";
    public static final String CUMULATIVE_LATENCIES = "/cumulative-latencies";
    public static final String COUNTERS_STATS = "/counters-stats";
    public static final String RESET_STATS = "/reset-stats";
    // Served without the bearer token so probes work; the last error needs the token
    public static final String HEALTH = "/health";

    // Browsers send text/plain, form and multipart POSTs cross-site without a CORS preflight, so
    // accepting only these types stops a web page from driving a worker on localhost or the LAN
    static final String JSON_CONTENT_TYPE = "application/json";
    static final String YAML_CONTENT_TYPE = "application/yaml";
    private static final Set<String> ACCEPTED_POST_TYPES =
            Set.of(JSON_CONTENT_TYPE, YAML_CONTENT_TYPE);
    private final Worker worker;
    private final @Nullable BearerTokenAuth auth;

    /**
     * Creates a handler for serving {@code worker} over HTTP.
     *
     * @param worker the worker to expose
     */
    public WorkerHandler(Worker worker) {
        this(worker, null);
    }

    /**
     * Creates a handler for serving {@code worker} over HTTP that requires a bearer token.
     *
     * @param worker the worker to expose
     * @param auth checks every request before any other handler runs, or {@code null} for none
     */
    public WorkerHandler(Worker worker, @Nullable BearerTokenAuth auth) {
        this.worker = worker;
        this.auth = auth;
    }

    public void configure(JavalinConfig config) {
        config.jetty.modifyHttpConfiguration(
                http -> http.setIdleTimeout(WorkerTimeouts.serverIdle().toMillis()));
        registerRoutes(config.routes);
    }

    public void registerRoutes(RoutesConfig routes) {
        routes.exception(
                Exception.class,
                (e, ctx) -> {
                    log.error("Request {} {} failed", ctx.method(), ctx.path(), e);
                    ctx.status(isClientError(e) ? HttpStatus.BAD_REQUEST : HttpStatus.INTERNAL_SERVER_ERROR)
                            .result(describe(e));
                });
        // Runs first so an unauthenticated caller learns nothing, not even the accepted types.
        // Before handlers apply to every path, including routes callers add after these.
        if (auth != null) {
            routes.before(auth);
        }
        // Blocks web pages, including ones that reach a tokenless worker through DNS rebinding
        boolean checkHost = auth == null;
        routes.before(ctx -> rejectBrowserRequests(ctx, checkHost));
        // Applies to every POST, including routes that callers add after these, such as the
        // driver process's /initialize-driver-local
        routes.before(WorkerHandler::rejectUnsupportedPostTypes);

        routes.post(INITIALIZE_DRIVER, this::handleInitializeDriver);
        routes.post(SET_RUN_ID, this::handleSetRunId);
        routes.post(SET_PHASE, ctx -> worker.setPhase(mapper.readValue(ctx.body(), RunPhase.class)));
        routes.post(CREATE_TOPICS, this::handleCreateTopics);
        routes.post(CREATE_PRODUCERS, this::handleCreateProducers);
        routes.post(PROBE_PRODUCERS, _ -> worker.probeProducers());
        routes.post(CREATE_CONSUMERS, this::handleCreateConsumers);
        routes.post(PAUSE_CONSUMERS, _ -> worker.pauseConsumers());
        routes.post(RESUME_CONSUMERS, _ -> worker.resumeConsumers());
        routes.post(START_LOAD, this::handleStartLoad);
        routes.post(ADJUST_PUBLISH_RATE, this::handleAdjustPublishRate);
        routes.post(STOP_ALL, this::handleStopAll);
        routes.get(PERIOD_STATS, ctx -> ctx.result(writer.writeValueAsString(worker.getPeriodStats())));
        routes.get(
                CUMULATIVE_LATENCIES,
                ctx -> ctx.result(writer.writeValueAsString(worker.getCumulativeLatencies())));
        routes.get(COUNTERS_STATS, this::handleCountersStats);
        routes.post(RESET_STATS, this::handleResetStats);
        routes.get(HEALTH, this::handleHealth);
    }

    @Override
    public void close() throws Exception {
        worker.close();
    }

    /**
     * Rejects requests a browser sends on a web page's behalf. The coordinator, the driver process
     * client, kubelet and Prometheus never send {@code Origin}. Without a token, a page can also
     * become same-origin with the worker by rebinding its own host name to the worker's address, so
     * the {@code Host} must then be an IP literal or {@code localhost}.
     *
     * @param ctx the request
     * @param checkHost whether to check the {@code Host} header
     */
    static void rejectBrowserRequests(Context ctx, boolean checkHost) {
        String reason = null;
        if (ctx.header("Origin") != null) {
            reason = "Requests with an Origin header are not accepted";
        } else if (checkHost && !isAddressHost(ctx.header("Host"))) {
            reason = "Without a worker token, the Host must be an IP address or localhost";
        }
        if (reason != null) {
            ctx.status(HttpStatus.FORBIDDEN).result(reason);
            ctx.skipRemainingHandlers();
        }
    }

    static boolean isAddressHost(@Nullable String host) {
        if (host == null) {
            return true; // HTTP/1.0 clients; browsers always send Host
        }
        if (host.startsWith("[")) {
            return host.indexOf(']') > 1; // bracketed IPv6 literal
        }
        int colon = host.indexOf(':');
        String name = colon < 0 ? host : host.substring(0, colon);
        return name.equalsIgnoreCase("localhost") || IPV4.matcher(name).matches();
    }

    static void rejectUnsupportedPostTypes(Context ctx) {
        if (ctx.method() != HandlerType.POST) {
            return;
        }
        boolean accepted =
                Optional.ofNullable(ctx.contentType())
                        .map(WorkerHandler::mediaType)
                        .filter(ACCEPTED_POST_TYPES::contains)
                        .isPresent();
        if (!accepted) {
            ctx.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                    .result("POST requests must use Content-Type application/json or application/yaml");
            ctx.skipRemainingHandlers();
        }
    }

    private static String mediaType(String contentType) {
        int parameters = contentType.indexOf(';');
        String mediaType = parameters < 0 ? contentType : contentType.substring(0, parameters);
        return mediaType.strip().toLowerCase(Locale.ROOT);
    }

    // Invalid configuration, plugin resolution errors and unsupported workloads fail the same way on
    // every retry, so report them as HTTP 400 rather than a 500 server failure.
    static boolean isClientError(Throwable failure) {
        Deque<Throwable> pending = new ArrayDeque<>(List.of(failure));
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        while (!pending.isEmpty()) {
            Throwable next = pending.pop();
            if (!seen.add(next)) {
                continue;
            }
            if (next instanceof IllegalArgumentException
                    || next instanceof JacksonException
                    || next instanceof PluginException
                    || next instanceof WorkerRejectedException) {
                return true;
            }
            Throwable cause = next.getCause();
            if (cause != null) {
                pending.push(cause);
            }
            pending.addAll(Arrays.asList(next.getSuppressed()));
        }
        return false;
    }

    private static String describe(Throwable e) {
        StringBuilder message = new StringBuilder();
        appendDescription(message, e, "", Collections.newSetFromMap(new IdentityHashMap<>()));
        return message.toString();
    }

    private static void appendDescription(
            StringBuilder message, @Nullable Throwable failure, String relation, Set<Throwable> visited) {
        if (failure == null || !visited.add(failure)) {
            return;
        }
        if (!message.isEmpty()) {
            message.append(" | ");
        }
        message.append(relation).append(failure.getClass().getName());
        if (failure.getMessage() != null) {
            message.append(": ").append(failure.getMessage());
        }
        appendDescription(message, failure.getCause(), "caused by: ", visited);
        for (Throwable suppressed : failure.getSuppressed()) {
            appendDescription(message, suppressed, "suppressed: ", visited);
        }
    }

    private void handleInitializeDriver(Context ctx) throws Exception {
        // Save config to the temp file
        var tempFile = SecureTempFiles.createFile("driver-configuration", ".yaml");
        try {
            Files.write(tempFile, ctx.bodyAsBytes());
            worker.initializeDriver(tempFile);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    private void handleSetRunId(Context ctx) {
        JsonNode body = mapper.readTree(ctx.body());
        // Coordinators without the workload tag send the run id as a JSON string
        worker.setRunContext(
                body.isString()
                        ? new RunContext(body.asString(), null)
                        : mapper.treeToValue(body, RunContext.class));
    }

    private void handleCreateTopics(Context ctx) {
        TopicsInfo topicsInfo = mapper.readValue(ctx.body(), TopicsInfo.class);
        log.atInfo()
                .setMessage("Received create topics request for topics: {}")
                .addArgument(ctx::body)
                .log();
        List<String> topics = worker.createTopics(topicsInfo);
        ctx.result(writer.writeValueAsString(topics));
    }

    private void handleCreateProducers(Context ctx) {
        var topics = mapper.readValue(ctx.body(), new TypeReference<List<String>>() {});
        log.info("Received create producers request for topics: {}", topics);
        worker.createProducers(topics);
    }

    private void handleCreateConsumers(Context ctx) {
        var consumerAssignment = mapper.readValue(ctx.body(), ConsumerAssignment.class);

        log.info(
                "Received create consumers request for topics: {}", consumerAssignment.topicsSubscriptions);
        worker.createConsumers(consumerAssignment);
    }

    private void handleStartLoad(Context ctx) {
        ProducerWorkAssignment producerWorkAssignment =
                mapper.readValue(ctx.body(), ProducerWorkAssignment.class);

        // The worker validates the payloads, so an empty list must not fail here first
        log.info(
                "Start load publish-rate: {} msg/s -- payloads: {}",
                producerWorkAssignment.publishRate(),
                producerWorkAssignment.payloads().size());

        worker.startLoad(producerWorkAssignment);
    }

    private void handleAdjustPublishRate(Context ctx) {
        Double publishRate = mapper.readValue(ctx.body(), Double.class);
        log.info("Adjust publish-rate: {} msg/s", publishRate);
        worker.adjustPublishRate(publishRate);
    }

    private void handleStopAll(Context ctx) {
        log.info("Stop All");
        worker.stopAll();
    }

    private void handleCountersStats(Context ctx) {
        var stats = worker.getCountersStats();
        log.info(
                "Worker returning stats - sent: {}, errors: {}, received: {}",
                stats.messagesSent(),
                stats.messageSendErrors(),
                stats.messagesReceived());
        ctx.result(writer.writeValueAsString(stats));
    }

    private void handleHealth(Context ctx) {
        // Sampled first, so the coordinator's clock-offset probe sees as little handling time as
        // possible
        long clockMicros = WorkerClock.nowMicros();
        WorkerHealth health = worker.health().withClockMicros(clockMicros);
        // The error text can name brokers and configuration, so only token holders see it
        if (auth != null && !auth.matches(ctx.header(BearerTokenAuth.AUTHORIZATION))) {
            health = health.withoutError();
        }
        ctx.status(health.ready() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE)
                .contentType(JSON_CONTENT_TYPE)
                .result(writer.writeValueAsString(health));
    }

    private void handleResetStats(Context ctx) {
        log.info("Reset stats");
        worker.resetStats();
    }
}
