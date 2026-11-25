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

import io.github.mihkels.omb.driver.BenchmarkConsumer;
import io.github.mihkels.omb.driver.BenchmarkDriver;
import io.github.mihkels.omb.driver.BenchmarkProducer;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLMapper;

/**
 * An in-memory broker with known behaviour, so tests can check that the framework reports what was
 * injected. Each send is acknowledged after {@code ackDelayMillis} and delivered to every
 * subscription of its topic after {@code deliveryDelayMillis}. Faults hit fixed send sequence
 * numbers rather than random ones, so their counts are exact: every {@code dropEvery}-th send is
 * acknowledged but never delivered, and every {@code duplicateEvery}-th delivered send arrives
 * twice. Zero turns a fault off.
 */
final class LoopbackDriver implements BenchmarkDriver {
    record Config(long ackDelayMillis, long deliveryDelayMillis, int dropEvery, int duplicateEvery) {}

    // The file also holds the framework's own keys, such as driverClass
    private static final ObjectMapper mapper =
            YAMLMapper.builder().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

    // One thread keeps deliveries of equal delay in send order, like a single partition
    private final ScheduledExecutorService clock = Executors.newSingleThreadScheduledExecutor();
    // topic -> subscription -> consumers sharing it
    private final Map<String, Map<String, List<ConsumerCallback>>> topics = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();
    private Config config = new Config(0, 0, 0, 0);

    @Override
    public void initialize(Path configurationFile, MeterRegistry statsLogger) throws IOException {
        try (var input = Files.newInputStream(configurationFile)) {
            config = mapper.readValue(input, Config.class);
        }
    }

    @Override
    public String getTopicNamePrefix() {
        return "loopback";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        topics.computeIfAbsent(topic, t -> new ConcurrentHashMap<>());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        return CompletableFuture.completedFuture(
                new BenchmarkProducer() {
                    @Override
                    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
                        return send(topic, payload);
                    }

                    @Override
                    public void close() {}
                });
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        List<ConsumerCallback> shared =
                topics
                        .computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
                        .computeIfAbsent(subscriptionName, s -> new CopyOnWriteArrayList<>());
        shared.add(consumerCallback);
        return CompletableFuture.completedFuture(() -> shared.remove(consumerCallback));
    }

    private CompletableFuture<Void> send(String topic, byte[] payload) {
        long n = sequence.incrementAndGet();
        long publishTimestamp = System.currentTimeMillis();
        CompletableFuture<Void> ack = new CompletableFuture<>();
        clock.schedule(() -> ack.complete(null), config.ackDelayMillis(), TimeUnit.MILLISECONDS);
        if (!hits(n, config.dropEvery())) {
            int copies = hits(n, config.duplicateEvery()) ? 2 : 1;
            for (int i = 0; i < copies; i++) {
                clock.schedule(
                        () -> deliver(topic, payload, publishTimestamp, n),
                        config.deliveryDelayMillis(),
                        TimeUnit.MILLISECONDS);
            }
        }
        return ack;
    }

    private void deliver(String topic, byte[] payload, long publishTimestamp, long n) {
        for (List<ConsumerCallback> shared : topics.getOrDefault(topic, Map.of()).values()) {
            if (!shared.isEmpty()) {
                // Round-robin within a shared subscription
                shared.get((int) (n % shared.size())).messageReceived(payload, publishTimestamp);
            }
        }
    }

    private static boolean hits(long n, int every) {
        return every > 0 && n % every == 0;
    }

    @Override
    public void close() throws InterruptedException {
        // Delayed tasks still run after shutdown, so pending sends settle instead of hanging
        clock.shutdown();
        if (!clock.awaitTermination(5, TimeUnit.SECONDS)) {
            clock.shutdownNow();
        }
    }
}
