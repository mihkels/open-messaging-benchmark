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
import io.javalin.config.RoutesConfig;
import io.javalin.http.Context;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.utils.SecureTempFiles;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import io.openmessaging.benchmark.worker.jackson.ObjectMappers;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;

public class WorkerHandler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WorkerHandler.class);
    private static final Duration REQUEST_IDLE_TIMEOUT = Duration.ofMinutes(10);

    private static final ObjectMapper mapper = ObjectMappers.DEFAULT.mapper();
    private static final ObjectWriter writer = ObjectMappers.DEFAULT.writer();

    public static final String INITIALIZE_DRIVER = "/initialize-driver";
    public static final String SET_RUN_ID = "/set-run-id";
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
    private final Worker worker;

    public WorkerHandler(PrometheusMeterRegistry statsLogger) {
        this(new LocalWorker(statsLogger));
    }

    /**
     * Creates a handler for serving {@code worker} over HTTP.
     *
     * @param worker the worker to expose
     */
    public WorkerHandler(Worker worker) {
        this.worker = worker;
    }

    public void configure(JavalinConfig config) {
        config.jetty.modifyHttpConfiguration(
                http -> http.setIdleTimeout(REQUEST_IDLE_TIMEOUT.toMillis()));
        registerRoutes(config.routes);
    }

    public void registerRoutes(RoutesConfig routes) {
        routes.exception(
                Exception.class,
                (e, ctx) -> {
                    log.error("Request {} {} failed", ctx.method(), ctx.path(), e);
                    ctx.status(500).result(describe(e));
                });

        routes.post(INITIALIZE_DRIVER, this::handleInitializeDriver);
        routes.post(SET_RUN_ID, this::handleSetRunId);
        routes.post(CREATE_TOPICS, this::handleCreateTopics);
        routes.post(CREATE_PRODUCERS, this::handleCreateProducers);
        routes.post(PROBE_PRODUCERS, this::handleProbeProducers);
        routes.post(CREATE_CONSUMERS, this::handleCreateConsumers);
        routes.post(PAUSE_CONSUMERS, this::handlePauseConsumers);
        routes.post(RESUME_CONSUMERS, this::handleResumeConsumers);
        routes.post(START_LOAD, this::handleStartLoad);
        routes.post(ADJUST_PUBLISH_RATE, this::handleAdjustPublishRate);
        routes.post(STOP_ALL, this::handleStopAll);
        routes.get(PERIOD_STATS, this::handlePeriodStats);
        routes.get(CUMULATIVE_LATENCIES, this::handleCumulativeLatencies);
        routes.get(COUNTERS_STATS, this::handleCountersStats);
        routes.post(RESET_STATS, this::handleResetStats);
    }

    @Override
    public void close() throws Exception {
        worker.close();
    }

    private static String describe(Throwable e) {
        StringBuilder message = new StringBuilder();
        appendDescription(message, e, "", Collections.newSetFromMap(new IdentityHashMap<>()));
        return message.toString();
    }

    private static void appendDescription(
            StringBuilder message, Throwable failure, String relation, Set<Throwable> visited) {
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
        worker.setRunId(ctx.body());
    }

    private void handleCreateTopics(Context ctx) {
        TopicsInfo topicsInfo = mapper.readValue(ctx.body(), TopicsInfo.class);
        log.atInfo()
                .setMessage("Received create topics request for topics: {}")
                .addArgument(() -> ctx.body())
                .log();
        List<String> topics = worker.createTopics(topicsInfo);
        ctx.result(writer.writeValueAsString(topics));
    }

    private void handleCreateProducers(Context ctx) {
        var topics = mapper.readValue(ctx.body(), new TypeReference<List<String>>() {});
        log.info("Received create producers request for topics: {}", topics);
        worker.createProducers(topics);
    }

    private void handleProbeProducers(Context ctx) {
        worker.probeProducers();
    }

    private void handleCreateConsumers(Context ctx) {
        var consumerAssignment = mapper.readValue(ctx.body(), ConsumerAssignment.class);

        log.info(
                "Received create consumers request for topics: {}", consumerAssignment.topicsSubscriptions);
        worker.createConsumers(consumerAssignment);
    }

    private void handlePauseConsumers(Context ctx) {
        worker.pauseConsumers();
    }

    private void handleResumeConsumers(Context ctx) {
        worker.resumeConsumers();
    }

    private void handleStartLoad(Context ctx) {
        ProducerWorkAssignment producerWorkAssignment =
                mapper.readValue(ctx.body(), ProducerWorkAssignment.class);

        // The worker validates the payloads, so an empty list must not fail here first
        log.info(
                "Start load publish-rate: {} msg/s -- payloads: {}",
                producerWorkAssignment.publishRate(),
                producerWorkAssignment.payloadData().size());

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

    private void handlePeriodStats(Context ctx) {
        ctx.result(writer.writeValueAsString(worker.getPeriodStats()));
    }

    private void handleCumulativeLatencies(Context ctx) {
        ctx.result(writer.writeValueAsString(worker.getCumulativeLatencies()));
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

    private void handleResetStats(Context ctx) {
        log.info("Reset stats");
        worker.resetStats();
    }
}
