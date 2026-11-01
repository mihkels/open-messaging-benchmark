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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.XAddParams;

class RedisBenchmarkProducerTest {
    @Test
    void closeWaitsForAnActiveSendWithoutClosingTheSharedPool() throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(jedis.xadd(any(byte[].class), anyMap(), any(XAddParams.class)))
                .thenAnswer(
                        invocation -> {
                            started.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting for send release");
                            }
                            return new byte[0];
                        });
        RedisBenchmarkProducer producer = new RedisBenchmarkProducer(pool, "topic");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<CompletableFuture<Void>> send =
                    executor.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {1}));
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            CountDownLatch closing = new CountDownLatch(1);
            Future<?> close =
                    executor.submit(
                            () -> {
                                closing.countDown();
                                producer.close();
                                return null;
                            });
            assertThat(closing.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> close.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            release.countDown();
            send.get(5, TimeUnit.SECONDS).join();
            close.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {2}).join())
                    .hasCauseInstanceOf(IllegalStateException.class);
            verify(pool, never()).close();
            verify(jedis).close();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void sendFailureCompletesExceptionally() {
        JedisPool pool = mock();
        RuntimeException cause = new RuntimeException("connection failed");
        when(pool.getResource()).thenThrow(cause);
        RedisBenchmarkProducer producer = new RedisBenchmarkProducer(pool, "topic");

        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(cause);
    }

    @Test
    void reentrantCloseIsRejectedWithoutClosingTheProducer() throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        RedisBenchmarkProducer producer = new RedisBenchmarkProducer(pool, "topic");
        doAnswer(
                        invocation -> {
                            assertThatThrownBy(producer::close).isInstanceOf(IllegalStateException.class);
                            return new byte[0];
                        })
                .when(jedis)
                .xadd(any(byte[].class), anyMap(), any(XAddParams.class));

        producer.sendAsync(Optional.empty(), new byte[] {1}).join();
        producer.sendAsync(Optional.empty(), new byte[] {2}).join();
        producer.close();
        verify(pool, never()).close();
    }

    @Test
    void interruptedCloseCanBeRetriedAfterSendFinishes() throws Exception {
        JedisPool pool = mock();
        Jedis jedis = mock();
        when(pool.getResource()).thenReturn(jedis);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(jedis.xadd(any(byte[].class), anyMap(), any(XAddParams.class)))
                .thenAnswer(
                        invocation -> {
                            started.countDown();
                            release.await(5, TimeUnit.SECONDS);
                            return new byte[0];
                        });
        RedisBenchmarkProducer producer = new RedisBenchmarkProducer(pool, "topic");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<CompletableFuture<Void>> send =
                    executor.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {1}));
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.currentThread().interrupt();
            assertThatThrownBy(producer::close).isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            Thread.interrupted();
            release.countDown();
            send.get(5, TimeUnit.SECONDS).join();
            producer.close();
        } finally {
            Thread.interrupted();
            release.countDown();
            executor.shutdownNow();
        }
    }
}
