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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
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
}
