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
package io.openmessaging.benchmark.e2e;

import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.worker.ProcessWorker;
import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;

/** Sends a small number of messages through the same isolated worker used by the benchmark. */
public final class FunctionalSmoke {
    private static final int MESSAGES_PER_TOPIC = 10;
    private static final Duration DELIVERY_TIMEOUT = Duration.ofSeconds(30);
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper JSON = new ObjectMapper();

    private FunctionalSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected driver config, workload and output paths");
        }
        Workload workload = YAML.readValue(Path.of(args[1]).toFile(), Workload.class);
        SmokeResult result;
        try (ProcessWorker worker = new ProcessWorker()) {
            worker.initializeDriver(Path.of(args[0]));
            result = run(worker, workload);
        }
        Files.writeString(Path.of(args[2]), JSON.writeValueAsString(result));
    }

    static SmokeResult run(Worker worker, Workload workload) throws Exception {
        return run(worker, workload, DELIVERY_TIMEOUT);
    }

    static SmokeResult run(Worker worker, Workload workload, Duration timeout) throws Exception {
        if (workload.topics <= 0
                || workload.partitionsPerTopic <= 0
                || workload.producersPerTopic <= 0
                || workload.subscriptionsPerTopic <= 0
                || workload.consumerPerSubscription <= 0
                || workload.messageSize <= 0
                || workload.producerRate <= 0
                || (workload.useRandomizedPayloads
                        && (workload.randomizedPayloadPoolSize <= 0
                                || !Double.isFinite(workload.randomBytesRatio)
                                || workload.randomBytesRatio < 0
                                || workload.randomBytesRatio > 1))
                || timeout.isZero()
                || timeout.isNegative()) {
            throw new IllegalArgumentException("Invalid smoke workload settings or timeout");
        }

        try {
            List<String> topics =
                    worker.createTopics(new TopicsInfo(workload.topics, workload.partitionsPerTopic));
            if (topics.size() != workload.topics) {
                throw new IllegalStateException(
                        "Expected " + workload.topics + " topics but the worker created " + topics.size());
            }
            ConsumerAssignment consumers = new ConsumerAssignment();
            List<String> producers = new ArrayList<>();
            for (String topic : topics) {
                for (int i = 0; i < workload.subscriptionsPerTopic; i++) {
                    String subscription = "smoke-" + i + "-" + UUID.randomUUID();
                    for (int j = 0; j < workload.consumerPerSubscription; j++) {
                        consumers.topicsSubscriptions.add(new TopicSubscription(topic, subscription));
                    }
                }
                for (int i = 0; i < workload.producersPerTopic; i++) {
                    producers.add(topic);
                }
            }
            worker.createConsumers(consumers);
            worker.createProducers(producers);
            long deadline = System.nanoTime() + timeout.toNanos();
            worker.probeProducers();
            CountersStats baseline =
                    awaitMessages(
                            worker,
                            new CountersStats(),
                            producers.size(),
                            (long) producers.size() * workload.subscriptionsPerTopic,
                            deadline,
                            "readiness probes");
            worker.resetStats();
            worker.startLoad(
                    new ProducerWorkAssignment(
                            payloads(workload), workload.producerRate, workload.keyDistributor));

            long minimumSent = minimumMessages(workload);
            long minimumReceived = minimumSent * workload.subscriptionsPerTopic;
            CountersStats stats =
                    awaitMessages(worker, baseline, minimumSent, minimumReceived, deadline, "smoke messages");
            double latency = worker.getCumulativeLatencies().endToEndLatency.getMean() / 1000.0;
            return new SmokeResult(
                    stats.messagesSent() - baseline.messagesSent(),
                    stats.messageSendErrors() - baseline.messageSendErrors(),
                    stats.messagesReceived() - baseline.messagesReceived(),
                    latency);
        } finally {
            worker.stopAll();
        }
    }

    private static CountersStats awaitMessages(
            Worker worker,
            CountersStats baseline,
            long minimumSent,
            long minimumReceived,
            long deadline,
            String phase)
            throws IOException, InterruptedException {
        CountersStats stats;
        do {
            stats = worker.getCountersStats();
            if (stats.messageSendErrors() != baseline.messageSendErrors()) {
                throw new IllegalStateException("Smoke run encountered publish errors: " + stats);
            }
            if (stats.messagesSent() - baseline.messagesSent() >= minimumSent
                    && stats.messagesReceived() - baseline.messagesReceived() >= minimumReceived) {
                return stats;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException(
                "Timed out waiting for "
                        + minimumSent
                        + " published and "
                        + minimumReceived
                        + " delivered "
                        + phase
                        + "; baseline: "
                        + baseline
                        + ", last counters: "
                        + stats);
    }

    static long minimumMessages(Workload workload) {
        return (long) MESSAGES_PER_TOPIC * workload.topics;
    }

    private static List<byte[]> payloads(Workload workload) {
        int count =
                workload.useRandomizedPayloads ? Math.max(1, workload.randomizedPayloadPoolSize) : 1;
        List<byte[]> payloads = new ArrayList<>();
        Random random = new Random(0);
        for (int i = 0; i < count; i++) {
            byte[] payload = new byte[workload.messageSize];
            if (workload.useRandomizedPayloads) {
                int randomBytes = (int) (workload.messageSize * workload.randomBytesRatio);
                byte[] prefix = new byte[randomBytes];
                random.nextBytes(prefix);
                System.arraycopy(prefix, 0, payload, 0, prefix.length);
            }
            payloads.add(payload);
        }
        return payloads;
    }
}
