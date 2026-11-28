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
package io.github.mihkels.omb.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mihkels.omb.utils.distributor.KeyDistributorType;
import io.github.mihkels.omb.worker.BearerTokenAuth;
import io.github.mihkels.omb.worker.HttpWorkerClient;
import io.github.mihkels.omb.worker.LocalWorker;
import io.github.mihkels.omb.worker.Worker;
import io.github.mihkels.omb.worker.WorkerHandler;
import io.github.mihkels.omb.worker.commands.ConsumerAssignment;
import io.github.mihkels.omb.worker.commands.CountersStats;
import io.github.mihkels.omb.worker.commands.CumulativeLatencies;
import io.github.mihkels.omb.worker.commands.PeriodStats;
import io.github.mihkels.omb.worker.commands.ProducerWorkAssignment;
import io.github.mihkels.omb.worker.commands.RunPhase;
import io.github.mihkels.omb.worker.commands.TopicSubscription;
import io.github.mihkels.omb.worker.commands.TopicsInfo;
import io.javalin.Javalin;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/**
 * The cross-worker contract of ADR 0021: the same workload with the loopback driver against the
 * Java worker and {@code worker-rs}, read by the Java worker client, must agree.
 *
 * <p>Tolerances: each worker's achieved rate is within {@value #RATE_TOLERANCE} of the target and
 * of the other worker's; every sent message is received once per subscription; the latency
 * histograms count every send since the statistics reset within {@value #COUNT_TOLERANCE}; and
 * loopback publish delay and latency medians stay under a millisecond.
 *
 * <p>Runs only with {@code -Domb.workerRs=<path to the worker-rs binary>}, so the Java build does
 * not need Rust.
 */
@EnabledIfSystemProperty(named = "omb.workerRs", matches = ".+")
class CrossWorkerContractTest {
    private static final String TOKEN = "contract-test-token";
    private static final double RATE = 50_000;
    private static final int PRODUCERS = 2;
    private static final int CONSUMERS = 2;
    private static final int SUBSCRIPTIONS = 2;
    private static final int PAYLOAD_BYTES = 1024;
    private static final Duration WARMUP = Duration.ofSeconds(3);
    private static final Duration MEASURE = Duration.ofSeconds(10);
    private static final Duration SETTLE = Duration.ofMillis(500);
    private static final double RATE_TOLERANCE = 0.02;
    private static final double COUNT_TOLERANCE = 0.01;

    /**
     * What one worker reported for the measured window.
     *
     * @param runtime the runtime the worker reports on {@code /health}
     * @param achievedRate acknowledged sends per second
     * @param sent acknowledged sends after the statistics reset returned
     * @param sentSinceReset acknowledged sends since just before the statistics reset
     * @param received messages received across all subscriptions after the reset returned
     * @param receivedSinceReset messages received since just before the reset
     * @param errors failed sends
     * @param publishSamples publish latency samples in the period statistics
     * @param cumulativePublishSamples publish latency samples in the cumulative latencies
     * @param endToEndSamples end-to-end latency samples in the cumulative latencies
     * @param delayP50Micros median publish delay
     * @param publishP50Micros median publish latency
     */
    record Measurement(
            String runtime,
            double achievedRate,
            long sent,
            long sentSinceReset,
            long received,
            long receivedSinceReset,
            long errors,
            long publishSamples,
            long cumulativePublishSamples,
            long endToEndSamples,
            long delayP50Micros,
            long publishP50Micros) {}

    @Test
    void javaAndRustWorkersAgreeOnTheSameLoopbackWorkload(@TempDir Path directory) throws Exception {
        Path driver = directory.resolve("synthetic.yaml");
        Files.writeString(
                driver,
                "name: synthetic\ndriverClass: " + SyntheticDriver.class.getName() + "\nmode: sync\n");

        Measurement java = measureJava(driver);
        Measurement rust = measureRust(driver, Path.of(System.getProperty("omb.workerRs")));

        for (Measurement m : List.of(java, rust)) {
            String who = m.runtime() + " worker: " + m;
            assertTrue(Math.abs(m.achievedRate() - RATE) <= RATE * RATE_TOLERANCE, who);
            assertEquals(0, m.errors(), who);
            assertWithin(m.sent() * SUBSCRIPTIONS, m.received(), who + " received");
            assertBetween(
                    m.sent(), m.sentSinceReset(), m.publishSamples(), who + " period publish samples");
            assertBetween(
                    m.sent(),
                    m.sentSinceReset(),
                    m.cumulativePublishSamples(),
                    who + " cumulative publish samples");
            assertBetween(
                    m.received(), m.receivedSinceReset(), m.endToEndSamples(), who + " end-to-end samples");
            assertTrue(m.delayP50Micros() < 1_000, who);
            assertTrue(m.publishP50Micros() < 1_000, who);
        }
        assertEquals("java", java.runtime());
        assertEquals("rust", rust.runtime());
        assertTrue(
                Math.abs(java.achievedRate() - rust.achievedRate()) <= RATE * RATE_TOLERANCE,
                "rates differ: " + java + " vs " + rust);
    }

    // The histograms start at the reset, which falls between the two counter readings around it
    private static void assertBetween(long low, long high, long actual, String what) {
        assertTrue(
                actual >= low * (1 - COUNT_TOLERANCE) && actual <= high * (1 + COUNT_TOLERANCE),
                what + ": expected " + low + " to " + high + ", got " + actual);
    }

    private static void assertWithin(long expected, long actual, String what) {
        assertTrue(
                Math.abs(expected - actual) <= Math.max(1, expected * COUNT_TOLERANCE),
                what + ": expected about " + expected + ", got " + actual);
    }

    private static Measurement measureJava(Path driver) throws Exception {
        try (var handler =
                new WorkerHandler(
                        new LocalWorker(), new BearerTokenAuth(TOKEN, Set.of(WorkerHandler.HEALTH)))) {
            Javalin app =
                    Javalin.create(
                                    config -> {
                                        config.startup.showJavalinBanner = false;
                                        handler.configure(config);
                                    })
                            .start("127.0.0.1", 0);
            try (var client = new HttpWorkerClient("http://127.0.0.1:" + app.port(), TOKEN)) {
                return measure(client, driver);
            } finally {
                app.stop();
            }
        }
    }

    private static Measurement measureRust(Path driver, Path binary) throws Exception {
        ProcessBuilder builder =
                new ProcessBuilder(binary.toString(), "--bind", "127.0.0.1", "--port", "0")
                        .redirectError(ProcessBuilder.Redirect.INHERIT);
        builder.environment().put(BearerTokenAuth.TOKEN_ENV, TOKEN);
        Process process = builder.start();
        try {
            String url = awaitListening(process);
            // The coordinator's client: HTTP/2 by prior knowledge
            try (var client = HttpWorkerClient.forRemoteWorker(url, TOKEN, null)) {
                return measure(client, driver);
            }
        } finally {
            process.destroy();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        }
    }

    // worker-rs prints "worker-rs listening on <address>" once it accepts connections
    private static String awaitListening(Process process) throws Exception {
        var reader =
                new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        String line =
                CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        return reader.readLine();
                                    } catch (java.io.IOException e) {
                                        throw new java.io.UncheckedIOException(e);
                                    }
                                })
                        .get(30, TimeUnit.SECONDS);
        String prefix = "worker-rs listening on ";
        if (line == null || !line.startsWith(prefix)) {
            throw new IllegalStateException("worker-rs did not start: " + line);
        }
        return "http://" + line.substring(prefix.length()).strip();
    }

    private static Measurement measure(Worker worker, Path driver) throws Exception {
        String runtime = worker.health().runtime();
        worker.initializeDriver(driver);
        try {
            List<String> topics = worker.createTopics(new TopicsInfo(1, 1));
            var assignment = new ConsumerAssignment();
            assignment.topicsSubscriptions = new ArrayList<>();
            for (int s = 0; s < SUBSCRIPTIONS; s++) {
                for (int c = 0; c < CONSUMERS; c++) {
                    assignment.topicsSubscriptions.add(new TopicSubscription(topics.getFirst(), "sub-" + s));
                }
            }
            worker.createConsumers(assignment);
            worker.createProducers(java.util.Collections.nCopies(PRODUCERS, topics.getFirst()));
            worker.probeProducers();
            worker.startLoad(
                    new ProducerWorkAssignment(
                            List.of(new byte[PAYLOAD_BYTES]), RATE, KeyDistributorType.NO_KEY));
            Thread.sleep(WARMUP);
            // The Java worker's reset can take a while, so it is bracketed by counter readings
            CountersStats beforeReset = worker.getCountersStats();
            worker.resetStats();
            CountersStats start = worker.getCountersStats();
            long started = System.nanoTime();
            long publishSamples = 0;
            long deadline = started + MEASURE.toNanos();
            while (System.nanoTime() < deadline) {
                Thread.sleep(1000);
                publishSamples += worker.getPeriodStats().publishLatency().getTotalCount();
            }
            // Stops the sends, so the counters and histograms read below cover the same sends
            worker.setPhase(RunPhase.DELIVERY_DRAIN);
            double elapsed = (System.nanoTime() - started) / 1e9;
            Thread.sleep(SETTLE);
            CountersStats end = worker.getCountersStats();
            PeriodStats last = worker.getPeriodStats();
            publishSamples += last.publishLatency().getTotalCount();
            CumulativeLatencies cumulative = worker.getCumulativeLatencies();
            CountersStats window = end.minus(start);
            CountersStats sinceReset = end.minus(beforeReset);
            return new Measurement(
                    runtime,
                    window.messagesSent() / elapsed,
                    window.messagesSent(),
                    sinceReset.messagesSent(),
                    window.messagesReceived(),
                    sinceReset.messagesReceived(),
                    window.messageSendErrors(),
                    publishSamples,
                    cumulative.publishLatency.getTotalCount(),
                    cumulative.endToEndLatency.getTotalCount(),
                    cumulative.publishDelayLatency.getValueAtPercentile(50),
                    cumulative.publishLatency.getValueAtPercentile(50));
        } finally {
            worker.stopAll();
        }
    }
}
