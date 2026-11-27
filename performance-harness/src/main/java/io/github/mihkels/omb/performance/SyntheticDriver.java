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

import io.github.mihkels.omb.driver.BenchmarkConsumer;
import io.github.mihkels.omb.driver.BenchmarkDriver;
import io.github.mihkels.omb.driver.BenchmarkProducer;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import org.jspecify.annotations.Nullable;
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

    // One counter per 128-byte line, so concurrent producers never write the same cache line.
    private static final int PAD = 16;
    // A power of two of at least twice the processors, so stripe() can mask the thread id.
    private static final int STRIPES =
            Integer.highestOneBit(Runtime.getRuntime().availableProcessors() * 2 - 1) << 1;

    private final Object lifecycle = new Object();
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final AtomicLong receiveCursor = new AtomicLong();
    private final AtomicLong producerIds = new AtomicLong();
    // Sends register here before they check closed; close sets closed and then waits for every
    // stripe.
    private final AtomicLongArray inFlight = new AtomicLongArray(STRIPES * PAD);
    private volatile Consumer[] consumers = new Consumer[0];
    private volatile boolean initialized;
    private volatile boolean closed;
    private boolean deliver;
    private Configuration configuration;
    private @Nullable ThreadPoolExecutor executor;
    private @Nullable ScheduledThreadPoolExecutor receiver;
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
            deliver = !"send-only".equals(config.mode);
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
            topics.computeIfAbsent(topic, ignored -> new Topic());
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        synchronized (lifecycle) {
            if (!available()) {
                return unavailable();
            }
            Topic target = topics.computeIfAbsent(topic, ignored -> new Topic());
            // Each producer round-robins on its own cursor; distinct starts keep producers on different
            // consumers.
            AtomicLong cursor = new AtomicLong(producerIds.getAndIncrement());
            return CompletableFuture.completedFuture(
                    new BenchmarkProducer() {
                        @Override
                        public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
                            return send(target, cursor, payload);
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
            Topic target = topics.computeIfAbsent(topic, ignored -> new Topic());
            Subscription subscription = target.byName.get(subscriptionName);
            if (subscription == null) {
                subscription = new Subscription();
                target.byName.put(subscriptionName, subscription);
                target.subscriptions = with(target.subscriptions, subscription);
            }
            Consumer consumer = new Consumer(subscription, callback);
            subscription.members = with(subscription.members, consumer);
            consumers = with(consumers, consumer);
            return CompletableFuture.completedFuture(consumer);
        }
    }

    private static <T> T[] with(T[] array, T element) {
        T[] copy = Arrays.copyOf(array, array.length + 1);
        copy[array.length] = element;
        return copy;
    }

    private static <T> T[] without(T[] array, T element) {
        return Arrays.stream(array)
                .filter(member -> member != element)
                .toArray(length -> Arrays.copyOf(array, length));
    }

    private static int stripe() {
        return (int) (Thread.currentThread().threadId() & (STRIPES - 1)) * PAD;
    }

    private CompletableFuture<Void> send(Topic topic, AtomicLong cursor, byte[] payload) {
        int stripe = stripe();
        inFlight.getAndIncrement(stripe);
        if (!available()) {
            inFlight.getAndDecrement(stripe);
            return unavailable();
        }
        Send task =
                new Send(topic, stripe, cursor.getAndIncrement(), payload, System.currentTimeMillis());
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
        private final Topic topic;
        private final int stripe;
        private final long sequence;
        // Swapped in once the send settles so the retained future no longer pins the caller's payload
        private static final byte[] RELEASED = new byte[0];
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

        Send(Topic topic, int stripe, long sequence, byte[] payload, long timestamp) {
            this.topic = topic;
            this.stripe = stripe;
            this.sequence = sequence;
            this.payload = payload;
            this.timestamp = timestamp;
        }

        @Override
        public void run() {
            try {
                if (deliver) {
                    for (Subscription subscription : topic.subscriptions) {
                        subscription.deliver(sequence, payload, timestamp);
                    }
                }
                sent.increment();
                payload = RELEASED;
                completion.complete(null);
            } catch (Throwable failure) {
                payload = RELEASED;
                completion.completeExceptionally(failure);
            } finally {
                inFlight.getAndDecrement(stripe);
            }
        }

        void fail(Throwable failure) {
            payload = RELEASED;
            completion.completeExceptionally(failure);
            inFlight.getAndDecrement(stripe);
        }
    }

    private static final class Topic {
        // Written under lifecycle; the send path only reads the published arrays.
        private final Map<String, Subscription> byName = new HashMap<>();
        private volatile Subscription[] subscriptions = new Subscription[0];
    }

    private static final class Subscription {
        private volatile Consumer[] members = new Consumer[0];

        void deliver(long sequence, byte[] payload, long timestamp) {
            Consumer[] snapshot = members;
            if (snapshot.length == 0) {
                return;
            }
            int start = (int) Math.floorMod(sequence, (long) snapshot.length);
            for (int i = 0; i < snapshot.length; i++) {
                if (snapshot[(start + i) % snapshot.length].deliver(payload, timestamp)) {
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
            synchronized (lifecycle) {
                subscription.members = without(subscription.members, this);
                consumers = without(consumers, this);
            }
        }
    }

    private void independentReceive(byte[] payload) {
        Consumer[] snapshot = consumers;
        if (snapshot.length == 0) {
            return;
        }
        int index = (int) Math.floorMod(receiveCursor.getAndIncrement(), (long) snapshot.length);
        try {
            snapshot[index].deliver(payload, System.currentTimeMillis());
        } catch (Throwable _) {
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
        try {
            awaitSends(deadline);
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
            synchronized (lifecycle) {
                consumers = new Consumer[0];
            }
            topics.clear();
        }
    }

    // A stripe seen at zero after closed is set stays zero: a later send sees closed and backs out.
    private void awaitSends(long deadline) throws InterruptedException, TimeoutException {
        for (int stripe = 0; stripe < STRIPES * PAD; stripe += PAD) {
            while (inFlight.get(stripe) != 0) {
                if (deadline - System.nanoTime() <= 0) {
                    throw new TimeoutException("Sends still in flight");
                }
                Thread.sleep(1);
            }
        }
    }
}
