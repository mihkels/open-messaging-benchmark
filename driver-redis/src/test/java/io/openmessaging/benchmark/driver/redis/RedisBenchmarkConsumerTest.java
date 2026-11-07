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
package io.openmessaging.benchmark.driver.redis;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntryBinary;

class RedisBenchmarkConsumerTest {
    @Test
    void closeReportsTimeoutForReaderIgnoringInterruption() throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(jedis.xreadGroupBinary(
                        any(byte[].class), any(byte[].class), any(XReadGroupParams.class), anyMap()))
                .thenAnswer(invocation -> {
                    started.countDown();
                    boolean interrupted = false;
                    while (true) {
                        try {
                            release.await();
                            break;
                        } catch (InterruptedException _) {
                            interrupted = true;
                        }
                    }
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    return List.of();
                });
        RedisBenchmarkConsumer consumer =
                new RedisBenchmarkConsumer(
                        "consumer",
                        "topic",
                        "subscription",
                        pool,
                        mock(ConsumerCallback.class),
                        Duration.ofMillis(100));
        try {
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(consumer::close).isInstanceOf(TimeoutException.class);
        } finally {
            release.countDown();
            consumer.close();
        }
        verify(jedis).close();
        verify(pool, never()).close();
    }

    @Test
    void interruptedCloseStillStopsReaderAndRestoresInterrupt() throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        CountDownLatch started = new CountDownLatch(1);
        when(jedis.xreadGroupBinary(
                        any(byte[].class), any(byte[].class), any(XReadGroupParams.class), anyMap()))
                .thenAnswer(invocation -> {
                    started.countDown();
                    new CountDownLatch(1).await();
                    return List.of();
                });
        RedisBenchmarkConsumer consumer =
                new RedisBenchmarkConsumer(
                        "consumer", "topic", "subscription", pool, mock(ConsumerCallback.class));
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(consumer::close).isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        verify(jedis).close();
    }

    @Test
    void closeFromCallbackIsRejectedAndCanBeRetried() throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        CountDownLatch allowRead = new CountDownLatch(1);
        CountDownLatch callbackFinished = new CountDownLatch(1);
        AtomicBoolean firstRead = new AtomicBoolean(true);
        StreamEntryBinary entry =
                new StreamEntryBinary(
                        new StreamEntryID(1, 0), Map.of("payload".getBytes(UTF_8), new byte[] {1}));
        when(jedis.xreadGroupBinary(
                        any(byte[].class), any(byte[].class), any(XReadGroupParams.class), anyMap()))
                .thenAnswer(invocation -> {
                    if (firstRead.getAndSet(false)) {
                        allowRead.await(5, TimeUnit.SECONDS);
                        return List.of(Map.entry("topic".getBytes(UTF_8), List.of(entry)));
                    }
                    new CountDownLatch(1).await();
                    return List.of();
                });
        ConsumerCallback callback = mock();
        AtomicReference<RedisBenchmarkConsumer> holder = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            try {
                                holder.get().close();
                            } catch (Exception e) {
                                failure.set(e);
                            } finally {
                                callbackFinished.countDown();
                            }
                            return null;
                        })
                .when(callback)
                .messageReceived(any(byte[].class), anyLong());
        RedisBenchmarkConsumer consumer =
                new RedisBenchmarkConsumer("consumer", "topic", "subscription", pool, callback);
        holder.set(consumer);
        try {
            allowRead.countDown();
            assertThat(callbackFinished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isInstanceOf(IllegalStateException.class);
        } finally {
            allowRead.countDown();
            consumer.close();
        }
    }

    @Test
    void rejectsNonPositiveCloseTimeout() {
        JedisPool pool = mock(JedisPool.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);

        assertThatThrownBy(() ->
                        new RedisBenchmarkConsumer(
                                "consumer", "topic", "subscription", pool, callback, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readLoopSurvivesEmptyReadsAndReadFailuresAndAcknowledgesDeliveredEntries() throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        Map<byte[], byte[]> fields = new LinkedHashMap<>();
        fields.put("other".getBytes(UTF_8), new byte[] {9});
        fields.put("payload".getBytes(UTF_8), new byte[] {1, 2});
        StreamEntryBinary entry = new StreamEntryBinary(new StreamEntryID(42, 0), fields);
        StreamEntryBinary missingPayload =
                new StreamEntryBinary(
                        new StreamEntryID(43, 0), Map.of("other".getBytes(UTF_8), new byte[] {9}));
        CountDownLatch blocked = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        when(jedis.xreadGroupBinary(
                        any(byte[].class), any(byte[].class), any(XReadGroupParams.class), anyMap()))
                .thenAnswer(invocation ->
                        switch (reads.getAndIncrement()) {
                            case 0 -> null;
                            case 1 -> List.of();
                            case 2 -> List.of(Map.entry("topic".getBytes(UTF_8), List.of(entry)));
                            case 3 -> List.of(Map.entry("topic".getBytes(UTF_8), List.of(missingPayload)));
                            case 4 -> throw new JedisDataException("ERR unexpected");
                            case 5 -> throw new IllegalStateException("connection lost");
                            default -> {
                                blocked.countDown();
                                new CountDownLatch(1).await();
                                yield List.of();
                            }
                        });
        ConsumerCallback callback = mock();
        RedisBenchmarkConsumer consumer =
                new RedisBenchmarkConsumer("consumer", "topic", "subscription", pool, callback);

        assertThat(blocked.await(5, TimeUnit.SECONDS)).isTrue();
        consumer.close();

        verify(callback).messageReceived(new byte[] {1, 2}, 42L);
        verify(jedis).xack("topic", "subscription", new StreamEntryID(42, 0));
        verify(jedis, never()).xack("topic", "subscription", new StreamEntryID(43, 0));
    }

    @Test
    void readFailuresDuringCloseAreIgnored() throws Exception {
        assertReadFailureDuringCloseIsIgnored(new JedisDataException("NOGROUP No such key"));
        assertReadFailureDuringCloseIsIgnored(new JedisDataException("ERR other"));
        assertReadFailureDuringCloseIsIgnored(new JedisDataException((String) null));
        assertReadFailureDuringCloseIsIgnored(new IllegalStateException("closed pool"));
    }

    private static void assertReadFailureDuringCloseIsIgnored(RuntimeException failure)
            throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        CountDownLatch blocked = new CountDownLatch(1);
        when(jedis.xreadGroupBinary(
                        any(byte[].class), any(byte[].class), any(XReadGroupParams.class), anyMap()))
                .thenAnswer(invocation -> {
                    blocked.countDown();
                    try {
                        new CountDownLatch(1).await();
                    } catch (InterruptedException _) {
                        throw failure;
                    }
                    return List.of();
                });
        RedisBenchmarkConsumer consumer =
                new RedisBenchmarkConsumer(
                        "consumer", "topic", "subscription", pool, mock(ConsumerCallback.class));

        assertThat(blocked.await(5, TimeUnit.SECONDS)).isTrue();
        consumer.close();
        verify(jedis).close();
    }

    @Test
    void readerThatDiedIsReportedOnClose() throws Exception {
        RedisBenchmarkConsumer consumer = deadReader();

        assertThatThrownBy(consumer::close)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(AssertionError.class);
    }

    @Test
    void readerFailureIsSuppressedIntoInterruptedClose() throws Exception {
        RedisBenchmarkConsumer consumer = deadReader();
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(consumer::close)
                    .isInstanceOf(InterruptedException.class)
                    .satisfies(error ->
                            assertThat(error.getSuppressed())
                                    .singleElement()
                                    .isInstanceOf(ExecutionException.class));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void timeoutIsSuppressedIntoInterruptedClose() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        RedisBenchmarkConsumer consumer = stubbornReader(release, Duration.ofMillis(100));
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(consumer::close)
                    .isInstanceOf(InterruptedException.class)
                    .satisfies(error ->
                            assertThat(error.getSuppressed())
                                    .singleElement()
                                    .isInstanceOf(TimeoutException.class));
        } finally {
            Thread.interrupted();
            release.countDown();
            consumer.close();
        }
    }

    @Test
    void interruptionWhileWaitingForReaderIsReported() throws Exception {
        assertInterruptedWhileWaiting(false);
        assertInterruptedWhileWaiting(true);
    }

    private static void assertInterruptedWhileWaiting(boolean interruptedBeforeClose)
            throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        RedisBenchmarkConsumer consumer = stubbornReader(release, Duration.ofSeconds(10));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptedAfterClose = new AtomicBoolean();
        Thread closer =
                Thread.ofPlatform()
                        .start(() -> {
                            if (interruptedBeforeClose) {
                                Thread.currentThread().interrupt();
                            }
                            try {
                                consumer.close();
                            } catch (Throwable error) {
                                failure.set(error);
                            } finally {
                                interruptedAfterClose.set(Thread.currentThread().isInterrupted());
                            }
                        });
        try {
            long firstWait = awaitWaitCount(closer, 1);
            closer.interrupt();
            awaitWaitCount(closer, firstWait + 1);
        } finally {
            release.countDown();
        }
        closer.join(5000);
        assertThat(closer.isAlive()).isFalse();
        assertThat(failure.get()).isInstanceOf(InterruptedException.class);
        assertThat(interruptedAfterClose).isTrue();
        if (interruptedBeforeClose) {
            assertThat(failure.get().getSuppressed())
                    .singleElement()
                    .isInstanceOf(InterruptedException.class);
        }
    }

    @Test
    void closeDuringDeliveryStopsTheRestOfTheBatch() throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        StreamEntryBinary first =
                new StreamEntryBinary(
                        new StreamEntryID(1, 0), Map.of("payload".getBytes(UTF_8), new byte[] {1}));
        StreamEntryBinary second =
                new StreamEntryBinary(
                        new StreamEntryID(2, 0), Map.of("payload".getBytes(UTF_8), new byte[] {2}));
        when(jedis.xreadGroupBinary(
                        any(byte[].class), any(byte[].class), any(XReadGroupParams.class), anyMap()))
                .thenReturn(List.of(Map.entry("topic".getBytes(UTF_8), List.of(first, second))));
        AtomicReference<RedisBenchmarkConsumer> holder = new AtomicReference<>();
        AtomicReference<Thread> closer = new AtomicReference<>();
        ConsumerCallback callback = mock();
        doAnswer(
                        invocation -> {
                            RedisBenchmarkConsumer consumer = holder.get();
                            closer.set(Thread.ofPlatform().start(() -> closeQuietly(consumer)));
                            awaitClosing(consumer);
                            return null;
                        })
                .when(callback)
                .messageReceived(any(byte[].class), anyLong());
        CountDownLatch created = new CountDownLatch(1);
        when(pool.getResource())
                .thenAnswer(invocation -> {
                    created.await(5, TimeUnit.SECONDS);
                    return jedis;
                });
        RedisBenchmarkConsumer consumer =
                new RedisBenchmarkConsumer("consumer", "topic", "subscription", pool, callback);
        holder.set(consumer);
        created.countDown();

        verify(callback, org.mockito.Mockito.timeout(5000)).messageReceived(new byte[] {1}, 1L);
        while (closer.get() == null) {
            Thread.onSpinWait();
        }
        closer.get().join(5000);
        assertThat(closer.get().isAlive()).isFalse();
        verify(callback, never()).messageReceived(new byte[] {2}, 2L);
        verify(jedis).xack("topic", "subscription", new StreamEntryID(1, 0));
        verify(jedis, never()).xack("topic", "subscription", new StreamEntryID(2, 0));
    }

    @Test
    void interruptedWaitForTheFinishedReaderIsReported() throws Exception {
        RedisBenchmarkConsumer consumer = deadReader();
        Future<?> task = mock();
        when(task.get()).thenThrow(new InterruptedException("interrupted while collecting"));
        Field field = RedisBenchmarkConsumer.class.getDeclaredField("consumerTask");
        field.setAccessible(true);
        field.set(consumer, task);

        try {
            assertThatThrownBy(consumer::close).isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }

        RedisBenchmarkConsumer finished = deadReader();
        field.set(finished, mock(Future.class));
        finished.close();
    }

    private static void closeQuietly(RedisBenchmarkConsumer consumer) {
        try {
            consumer.close();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    // Spins instead of sleeping: close() interrupts the reader thread, and an interrupted sleep would
    // make the callback fail before the entry is acknowledged.
    private static void awaitClosing(RedisBenchmarkConsumer consumer) throws Exception {
        Field closing = RedisBenchmarkConsumer.class.getDeclaredField("closing");
        closing.setAccessible(true);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!closing.getBoolean(consumer)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("consumer did not start closing");
            }
            Thread.onSpinWait();
        }
    }

    @SuppressWarnings("java:S2925") // polls thread wait counters, which expose no completion hook
    private static long awaitWaitCount(Thread thread, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.threadId());
            if (info == null) {
                break;
            }
            if (info.getWaitedCount() >= expected) {
                return info.getWaitedCount();
            }
            Thread.sleep(1);
        }
        throw new AssertionError("Thread did not enter the expected wait");
    }

    private static RedisBenchmarkConsumer stubbornReader(CountDownLatch release, Duration timeout)
            throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        CountDownLatch started = new CountDownLatch(1);
        when(jedis.xreadGroupBinary(
                        any(byte[].class), any(byte[].class), any(XReadGroupParams.class), anyMap()))
                .thenAnswer(invocation -> {
                    started.countDown();
                    while (true) {
                        try {
                            release.await();
                            return List.of();
                        } catch (InterruptedException _) {
                            // Simulate a client call that ignores interruption.
                        }
                    }
                });
        RedisBenchmarkConsumer consumer =
                new RedisBenchmarkConsumer(
                        "consumer", "topic", "subscription", pool, mock(ConsumerCallback.class), timeout);
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        return consumer;
    }

    private static RedisBenchmarkConsumer deadReader() throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.xreadGroupBinary(
                        any(byte[].class), any(byte[].class), any(XReadGroupParams.class), anyMap()))
                .thenThrow(new AssertionError("reader died"));
        RedisBenchmarkConsumer consumer =
                new RedisBenchmarkConsumer(
                        "consumer", "topic", "subscription", pool, mock(ConsumerCallback.class));
        Field field = RedisBenchmarkConsumer.class.getDeclaredField("consumerTask");
        field.setAccessible(true);
        Future<?> task = (Future<?>) field.get(consumer);
        try {
            task.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException _) {
            // The reader is expected to die with the stubbed failure.
        }
        return consumer;
    }
}
