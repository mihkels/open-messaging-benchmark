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
                .thenAnswer(
                        invocation -> {
                            started.countDown();
                            boolean interrupted = false;
                            while (true) {
                                try {
                                    release.await();
                                    break;
                                } catch (InterruptedException e) {
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
                .thenAnswer(
                        invocation -> {
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
                .thenAnswer(
                        invocation -> {
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
        assertThatThrownBy(
                        () ->
                                new RedisBenchmarkConsumer(
                                        "consumer",
                                        "topic",
                                        "subscription",
                                        mock(JedisPool.class),
                                        mock(ConsumerCallback.class),
                                        Duration.ZERO))
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
                .thenAnswer(
                        invocation ->
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
                .thenAnswer(
                        invocation -> {
                            blocked.countDown();
                            try {
                                new CountDownLatch(1).await();
                            } catch (InterruptedException interrupted) {
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
                    .satisfies(
                            error ->
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
                    .satisfies(
                            error ->
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
                        .start(
                                () -> {
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
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (closer.getState() != Thread.State.TIMED_WAITING) {
                assertThat(System.nanoTime()).isLessThan(deadline);
                Thread.sleep(5);
            }
            closer.interrupt();
            while (closer.getState() != Thread.State.TIMED_WAITING && closer.isAlive()) {
                assertThat(System.nanoTime()).isLessThan(deadline);
                Thread.sleep(5);
            }
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

    private static RedisBenchmarkConsumer stubbornReader(CountDownLatch release, Duration timeout)
            throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        CountDownLatch started = new CountDownLatch(1);
        when(jedis.xreadGroupBinary(
                        any(byte[].class), any(byte[].class), any(XReadGroupParams.class), anyMap()))
                .thenAnswer(
                        invocation -> {
                            started.countDown();
                            while (true) {
                                try {
                                    release.await();
                                    return List.of();
                                } catch (InterruptedException ignored) {
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
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!task.isDone()) {
            assertThat(System.nanoTime()).isLessThan(deadline);
            Thread.sleep(5);
        }
        return consumer;
    }
}
