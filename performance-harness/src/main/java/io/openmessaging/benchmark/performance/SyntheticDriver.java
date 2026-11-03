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
package io.openmessaging.benchmark.performance;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Phaser;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.dataformat.yaml.YAMLMapper;

/**
 * Harness-only fixture, not a broker. Loopback fans out once per subscription, round-robin within
 * each subscription, and borrows the caller's read-only payload until send completion.
 *
 * <p>YAML keys: mode (sync, async, send-only; default sync), queueCapacity (1024), threads (1),
 * receiveRate (0), receivePayloadSize (1024), closeTimeoutMillis (5000). Positive receiveRate
 * creates an independent stream at that total rate per driver/consumer worker, shared round-robin
 * across its consumers. It does not connect producer and consumer JVMs or model broker
 * latency/backlog.
 */
public final class SyntheticDriver implements BenchmarkDriver {
    public static final class Configuration {
        public String mode = "sync";
        public int queueCapacity = 1024;
        public int threads = 1;
        public double receiveRate;
        public int receivePayloadSize = 1024;
        public long closeTimeoutMillis = 5000;
    }

    private final Object lifecycle = new Object();
    private final Map<String, Map<String, Subscription>> topics = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Consumer> consumers = new CopyOnWriteArrayList<>();
    private final AtomicLong receiveCursor = new AtomicLong();
    private final Phaser sends = new Phaser(1);
    private volatile boolean initialized;
    private volatile boolean closed;
    private Configuration configuration;
    private ThreadPoolExecutor executor;
    private ScheduledThreadPoolExecutor receiver;
    private Counter sent;
    private Counter received;
    private Counter rejected;
    private Counter receiveErrors;

    public SyntheticDriver() {}

    @Override
    public void initialize(Path configurationFile, MeterRegistry registry) throws IOException {
        Configuration config =
                YAMLMapper.builder()
                        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                        .build()
                        .readValue(configurationFile.toFile(), Configuration.class);
        if (config == null
                || !List.of("sync", "async", "send-only").contains(config.mode == null ? "" : config.mode)
                || config.queueCapacity <= 0
                || config.threads <= 0
                || !Double.isFinite(config.receiveRate)
                || config.receiveRate < 0
                || config.receivePayloadSize <= 0
                || config.closeTimeoutMillis <= 0) {
            throw new IllegalArgumentException("Invalid synthetic driver configuration");
        }
        synchronized (lifecycle) {
            if (initialized || closed) {
                throw new IllegalStateException("Driver already initialized or closed");
            }
            configuration = config;
            sent = registry.counter("synthetic.sends");
            received = registry.counter("synthetic.receives");
            rejected = registry.counter("synthetic.rejections");
            receiveErrors = registry.counter("synthetic.receive.errors");
            if ("async".equals(config.mode)) {
                executor =
                        new ThreadPoolExecutor(
                                config.threads,
                                config.threads,
                                0,
                                TimeUnit.MILLISECONDS,
                                new ArrayBlockingQueue<>(config.queueCapacity),
                                daemonFactory("synthetic-send"));
            }
            initialized = true;
            if (config.receiveRate > 0) {
                byte[] payload = new byte[config.receivePayloadSize];
                receiver = new ScheduledThreadPoolExecutor(1, daemonFactory("synthetic-receive"));
                receiver.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
                receiver.scheduleAtFixedRate(
                        () -> independentReceive(payload),
                        0,
                        Math.max(1, Math.round(1_000_000_000d / config.receiveRate)),
                        TimeUnit.NANOSECONDS);
            }
        }
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicLong ids = new AtomicLong();
        return task -> {
            Thread thread = new Thread(task, prefix + "-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    @Override
    public String getTopicNamePrefix() {
        return "synthetic";
    }

    private boolean available() {
        return initialized && !closed;
    }

    private static <T> CompletableFuture<T> unavailable() {
        return CompletableFuture.failedFuture(new IllegalStateException("Driver not running"));
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        synchronized (lifecycle) {
            if (!available()) {
                return unavailable();
            }
            if (partitions <= 0) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("partitions must be positive"));
            }
            topics.computeIfAbsent(topic, ignored -> new ConcurrentHashMap<>());
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        synchronized (lifecycle) {
            if (!available()) {
                return unavailable();
            }
            Map<String, Subscription> subscriptions =
                    topics.computeIfAbsent(topic, ignored -> new ConcurrentHashMap<>());
            return CompletableFuture.completedFuture(
                    new BenchmarkProducer() {
                        @Override
                        public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
                            return send(subscriptions, payload);
                        }

                        @Override
                        public void close() {
                            // Producers own no resources; the driver owns the executor.
                        }
                    });
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback callback) {
        synchronized (lifecycle) {
            if (!available()) {
                return unavailable();
            }
            Subscription subscription =
                    topics
                            .computeIfAbsent(topic, ignored -> new ConcurrentHashMap<>())
                            .computeIfAbsent(subscriptionName, ignored -> new Subscription());
            Consumer consumer = new Consumer(subscription, callback);
            subscription.members.add(consumer);
            consumers.add(consumer);
            return CompletableFuture.completedFuture(consumer);
        }
    }

    private CompletableFuture<Void> send(Map<String, Subscription> subscriptions, byte[] payload) {
        synchronized (lifecycle) {
            if (!available()) {
                return unavailable();
            }
            sends.register();
        }
        Send task = new Send(subscriptions, payload, System.currentTimeMillis());
        if (executor == null) {
            task.run();
        } else {
            try {
                executor.execute(task);
            } catch (RejectedExecutionException error) {
                rejected.increment();
                task.fail(error);
            }
        }
        return task.completion;
    }

    private final class Send implements Runnable {
        private final Map<String, Subscription> subscriptions;
        private byte[] payload;
        private final long timestamp;
        // Cancellation must not release caller-owned storage while a callback still uses it.
        private final CompletableFuture<Void> completion =
                new CompletableFuture<>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }
                };

        Send(Map<String, Subscription> subscriptions, byte[] payload, long timestamp) {
            this.subscriptions = subscriptions;
            this.payload = payload;
            this.timestamp = timestamp;
        }

        @Override
        public void run() {
            try {
                if (!"send-only".equals(configuration.mode)) {
                    for (Subscription subscription : subscriptions.values()) {
                        subscription.deliver(payload, timestamp);
                    }
                }
                sent.increment();
                payload = null;
                completion.complete(null);
            } catch (Throwable failure) {
                payload = null;
                completion.completeExceptionally(failure);
            } finally {
                sends.arriveAndDeregister();
            }
        }

        void fail(Throwable failure) {
            payload = null;
            completion.completeExceptionally(failure);
            sends.arriveAndDeregister();
        }
    }

    private final class Subscription {
        private final CopyOnWriteArrayList<Consumer> members = new CopyOnWriteArrayList<>();
        private final AtomicLong cursor = new AtomicLong();

        void deliver(byte[] payload, long timestamp) {
            Object[] snapshot = members.toArray();
            if (snapshot.length == 0) {
                return;
            }
            int start = (int) Math.floorMod(cursor.getAndIncrement(), (long) snapshot.length);
            for (int i = 0; i < snapshot.length; i++) {
                if (((Consumer) snapshot[(start + i) % snapshot.length]).deliver(payload, timestamp)) {
                    return;
                }
            }
        }
    }

    private final class Consumer implements BenchmarkConsumer {
        private final Subscription subscription;
        private final ConsumerCallback callback;
        private boolean stopped;

        Consumer(Subscription subscription, ConsumerCallback callback) {
            this.subscription = subscription;
            this.callback = callback;
        }

        synchronized boolean deliver(byte[] payload, long timestamp) {
            if (stopped) {
                return false;
            }
            callback.messageReceived(payload, timestamp);
            received.increment();
            return true;
        }

        @Override
        public synchronized void close() {
            stopped = true;
            subscription.members.remove(this);
            consumers.remove(this);
        }
    }

    private void independentReceive(byte[] payload) {
        Object[] snapshot = consumers.toArray();
        if (snapshot.length == 0) {
            return;
        }
        int index = (int) Math.floorMod(receiveCursor.getAndIncrement(), (long) snapshot.length);
        try {
            ((Consumer) snapshot[index]).deliver(payload, System.currentTimeMillis());
        } catch (Throwable failure) {
            receiveErrors.increment();
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        synchronized (lifecycle) {
            if (closed) {
                return;
            }
            closed = true;
            if (!initialized) {
                return;
            }
        }
        long deadline =
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(configuration.closeTimeoutMillis);
        if (receiver != null) {
            receiver.shutdownNow();
        }
        if (executor != null) {
            executor.shutdown();
        }
        int phase = sends.arriveAndDeregister();
        try {
            sends.awaitAdvanceInterruptibly(
                    phase, Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            if (executor != null
                    && !executor.awaitTermination(
                            Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                throw new TimeoutException("Send executor still running");
            }
            if (receiver != null
                    && !receiver.awaitTermination(
                            Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                throw new TimeoutException("Receive callback still running");
            }
        } catch (TimeoutException | InterruptedException failure) {
            if (executor != null) {
                for (Runnable pending : executor.shutdownNow()) {
                    ((Send) pending).fail(failure);
                }
            }
            if (failure instanceof InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            // Running callbacks retain their send future/payload until they return, even after timeout.
            throw new IOException("Synthetic driver did not drain before its close deadline", failure);
        } finally {
            consumers.clear();
            topics.clear();
        }
    }
}
