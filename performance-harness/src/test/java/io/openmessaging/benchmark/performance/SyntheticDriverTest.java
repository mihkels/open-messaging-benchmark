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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(15)
class SyntheticDriverTest {
    private static final AtomicInteger FILE_IDS = new AtomicInteger();
    private final SyntheticDriver driver = new SyntheticDriver();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private Path configFile;

    private CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscription, BiConsumer<byte[], Long> callback) {
        return driver.createConsumer(
                topic,
                subscription,
                new ConsumerCallback() {
                    @Override
                    public void messageReceived(byte[] payload, long timestamp) {
                        callback.accept(payload, timestamp);
                    }

                    @Override
                    public void messageReceived(ByteBuffer payload, long timestamp) {
                        byte[] bytes = new byte[payload.remaining()];
                        payload.duplicate().get(bytes);
                        callback.accept(bytes, timestamp);
                    }
                });
    }

    private void initialize(String yaml) throws Exception {
        Path directory = Path.of("target", "synthetic-driver-tests");
        Files.createDirectories(directory);
        configFile = directory.resolve("config-" + FILE_IDS.incrementAndGet() + ".yaml");
        Files.writeString(configFile, yaml);
        driver.initialize(configFile, registry);
    }

    @AfterEach
    void cleanup() throws Exception {
        try {
            driver.close();
        } finally {
            registry.close();
            if (configFile != null) {
                Files.deleteIfExists(configFile);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sync", "async"})
    void loopbackFansOutAndRoundRobinsWithoutCopyingPayload(String mode) throws Exception {
        initialize(
                "mode: "
                        + mode
                        + "\nqueueCapacity: 32\nthreads: 1\nname: synthetic\ndriverClass: ignored\n");
        driver.createTopic("topic", 1).join();
        List<Integer> order = new ArrayList<>();
        AtomicInteger broadcast = new AtomicInteger();
        byte[] payload = new byte[128];
        long before = System.currentTimeMillis();
        for (int id = 0; id < 3; id++) {
            int consumerId = id;
            createConsumer(
                            "topic",
                            "group",
                            (data, timestamp) -> {
                                assertSame(payload, data);
                                assertTrue(timestamp >= before && timestamp <= System.currentTimeMillis());
                                order.add(consumerId);
                            })
                    .join();
        }
        createConsumer("topic", "other-group", (data, timestamp) -> broadcast.incrementAndGet()).join();
        BenchmarkProducer producer = driver.createProducer("topic").join();
        producer.close();
        for (int i = 0; i < 9; i++) {
            producer.sendAsync(Optional.empty(), payload).get(2, TimeUnit.SECONDS);
        }
        assertEquals(List.of(0, 1, 2, 0, 1, 2, 0, 1, 2), order);
        assertEquals(9, broadcast.get());
        assertEquals(9, registry.get("synthetic.sends").counter().count());
        assertEquals(18, registry.get("synthetic.receives").counter().count());
    }

    @Test
    void sendOnlyDoesNotDeliver() throws Exception {
        initialize("mode: send-only\n");
        AtomicInteger count = new AtomicInteger();
        createConsumer("topic", "sub", (data, timestamp) -> count.incrementAndGet()).join();
        driver.createProducer("topic").join().sendAsync(Optional.empty(), new byte[1]).join();
        assertEquals(0, count.get());
        assertEquals(1, registry.get("synthetic.sends").counter().count());
    }

    @Test
    void closedConsumersAreRemovedAndTopicsAreIsolated() throws Exception {
        initialize("mode: sync\n");
        AtomicInteger count = new AtomicInteger();
        BenchmarkConsumer removed =
                createConsumer("topic", "sub", (data, timestamp) -> count.incrementAndGet()).join();
        removed.close();
        removed.close();
        createConsumer("different-topic", "sub", (data, timestamp) -> count.incrementAndGet()).join();
        driver.createProducer("topic").join().sendAsync(Optional.empty(), new byte[1]).join();
        assertEquals(0, count.get());
        createConsumer("topic", "sub", (data, timestamp) -> count.incrementAndGet()).join();
        driver.createProducer("topic").join().sendAsync(Optional.empty(), new byte[1]).join();
        assertEquals(1, count.get());
    }

    @Test
    void asyncQueueIsBoundedAndCloseDrainsAcceptedSends() throws Exception {
        initialize("mode: async\nqueueCapacity: 1\nthreads: 1\n");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        createConsumer(
                        "topic",
                        "sub",
                        (data, timestamp) -> {
                            entered.countDown();
                            await(release);
                        })
                .join();
        BenchmarkProducer producer = driver.createProducer("topic").join();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[1]);
        try {
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertFalse(first.cancel(true));
            CompletableFuture<Void> queued = producer.sendAsync(Optional.empty(), new byte[1]);
            CompletableFuture<Void> rejected = producer.sendAsync(Optional.empty(), new byte[1]);
            assertTrue(
                    assertThrows(CompletionException.class, rejected::join).getCause()
                            instanceof RejectedExecutionException);
            assertFalse(first.isDone());
            assertFalse(queued.isDone());
            release.countDown();
            driver.close();
            assertTrue(first.isDone());
            assertTrue(queued.isDone());
            first.join();
            queued.join();
            assertEquals(2, registry.get("synthetic.sends").counter().count());
            assertEquals(1, registry.get("synthetic.rejections").counter().count());
        } finally {
            release.countDown();
        }
    }

    @Test
    void boundedCloseFailsQueuedWorkButDoesNotReleaseRunningPayloadEarly() throws Exception {
        initialize("mode: async\nqueueCapacity: 1\nthreads: 1\ncloseTimeoutMillis: 100\n");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        createConsumer(
                        "topic",
                        "sub",
                        (data, timestamp) -> {
                            entered.countDown();
                            await(release);
                        })
                .join();
        BenchmarkProducer producer = driver.createProducer("topic").join();
        CompletableFuture<Void> running = producer.sendAsync(Optional.empty(), new byte[1]);
        try {
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            CompletableFuture<Void> queued = producer.sendAsync(Optional.empty(), new byte[1]);
            long start = System.nanoTime();
            assertThrows(IOException.class, driver::close);
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2));
            assertTrue(queued.isCompletedExceptionally());
            assertFalse(running.isDone());
        } finally {
            release.countDown();
            running.get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void boundedCloseAlsoAccountsForSynchronousCallbacks() throws Exception {
        initialize("mode: sync\ncloseTimeoutMillis: 100\n");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        createConsumer(
                        "topic",
                        "sub",
                        (data, timestamp) -> {
                            entered.countDown();
                            await(release);
                        })
                .join();
        BenchmarkProducer producer = driver.createProducer("topic").join();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread sender =
                Thread.ofPlatform()
                        .start(
                                () -> {
                                    try {
                                        producer.sendAsync(Optional.empty(), new byte[1]).join();
                                    } catch (Throwable error) {
                                        failure.set(error);
                                    }
                                });
        try {
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertThrows(IOException.class, driver::close);
        } finally {
            release.countDown();
            sender.join(2000);
        }
        assertFalse(sender.isAlive());
        assertEquals(null, failure.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {"sync", "async"})
    void callbackFailuresCompleteTheSendExceptionally(String mode) throws Exception {
        initialize("mode: " + mode + "\n");
        IllegalStateException failure = new IllegalStateException("callback failure");
        createConsumer(
                        "topic",
                        "sub",
                        (data, timestamp) -> {
                            throw failure;
                        })
                .join();
        CompletableFuture<Void> send =
                driver.createProducer("topic").join().sendAsync(Optional.empty(), new byte[1]);
        assertSame(failure, assertThrows(CompletionException.class, send::join).getCause());
    }

    @Test
    void independentReceiveStreamNeedsNoLocalProducerAndReusesItsPayload() throws Exception {
        initialize("mode: send-only\nreceiveRate: 500\nreceivePayloadSize: 64\n");
        CountDownLatch delivered = new CountDownLatch(6);
        AtomicReference<byte[]> first = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int i = 0; i < 2; i++) {
            createConsumer(
                            "remote-topic",
                            "sub",
                            (data, timestamp) -> {
                                try {
                                    first.compareAndSet(null, data);
                                    assertSame(first.get(), data);
                                    assertEquals(64, data.length);
                                    assertTrue(timestamp <= System.currentTimeMillis());
                                } catch (Throwable error) {
                                    failure.set(error);
                                } finally {
                                    delivered.countDown();
                                }
                            })
                    .join();
        }
        assertTrue(delivered.await(2, TimeUnit.SECONDS));
        driver.close();
        assertNotNull(first.get());
        assertEquals(null, failure.get());
        assertTrue(registry.get("synthetic.receives").counter().count() >= 6);
        assertEquals(0, registry.get("synthetic.sends").counter().count());
    }

    @Test
    void closeRejectsAllNewWorkWithoutClosingHostRegistry() throws Exception {
        initialize("mode: sync\n");
        BenchmarkProducer producer = driver.createProducer("topic").join();
        driver.close();
        driver.close();
        assertThrows(
                CompletionException.class, () -> producer.sendAsync(Optional.empty(), new byte[1]).join());
        assertThrows(CompletionException.class, () -> driver.createTopic("new", 1).join());
        assertThrows(CompletionException.class, () -> driver.createProducer("new").join());
        assertThrows(
                CompletionException.class,
                () -> createConsumer("new", "sub", (data, timestamp) -> {}).join());
        registry.counter("host.still.open").increment();
        assertEquals(1, registry.get("host.still.open").counter().count());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "mode: unknown",
                "mode: null",
                "queueCapacity: 0",
                "queueCapacity: -1",
                "threads: 0",
                "receiveRate: -1",
                "receiveRate: .inf",
                "receivePayloadSize: 0",
                "closeTimeoutMillis: 0"
            })
    void rejectsInvalidConfiguration(String yaml) {
        assertThrows(IllegalArgumentException.class, () -> initialize(yaml + "\n"));
    }

    @Test
    void rejectsCreationBeforeInitializationAndInvalidPartitions() throws Exception {
        assertThrows(CompletionException.class, () -> driver.createProducer("topic").join());
        initialize("mode: sync\n");
        assertThrows(CompletionException.class, () -> driver.createTopic("topic", 0).join());
        assertThrows(IllegalStateException.class, () -> driver.initialize(configFile, registry));
    }

    private static void await(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (latch.getCount() != 0) {
                try {
                    if (!latch.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Test callback was not released");
                    }
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
