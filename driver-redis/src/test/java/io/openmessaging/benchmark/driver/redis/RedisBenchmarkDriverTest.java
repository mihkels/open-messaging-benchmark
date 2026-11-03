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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;

class RedisBenchmarkDriverTest {
    @Test
    void aclAuthenticationSendsUserBeforePassword(@TempDir Path temp) throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<List<String>> auth = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> serveRedis(server, auth));
            Path config = temp.resolve("redis.yaml");
            Files.writeString(
                    config,
                    "redisHost: 127.0.0.1\nredisPort: "
                            + server.getLocalPort()
                            + "\nredisUser: benchmark\nredisPass: secret\n"
                            + "jedisPoolMaxTotal: 1\njedisPoolMaxIdle: 1\n");
            RedisBenchmarkDriver driver = new RedisBenchmarkDriver();
            driver.initialize(config, mock(MeterRegistry.class));
            try {
                driver.createProducer("topic").join();
                try (Jedis ignored = driver.jedisPool.getResource()) {
                    assertThat(auth.get(5, TimeUnit.SECONDS)).containsExactly("AUTH", "benchmark", "secret");
                }
            } finally {
                driver.close();
            }
        }
    }

    // Minimal RESP server: answers every command with OK and records the first AUTH command
    private static void serveRedis(ServerSocket server, CompletableFuture<List<String>> auth) {
        try (Socket socket = server.accept()) {
            BufferedReader in =
                    new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            OutputStream out = socket.getOutputStream();
            while (true) {
                String header = in.readLine();
                if (header == null) {
                    auth.completeExceptionally(new IllegalStateException("Connection closed before AUTH"));
                    return;
                }
                List<String> command = new ArrayList<>();
                for (int i = Integer.parseInt(header.substring(1)); i > 0; i--) {
                    in.readLine();
                    command.add(in.readLine());
                }
                out.write("+OK\r\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                if (command.getFirst().equalsIgnoreCase("AUTH")) {
                    auth.complete(command);
                }
            }
        } catch (Exception e) {
            auth.completeExceptionally(e);
        }
    }

    @Test
    void concurrentCreationSharesOnePool(@TempDir Path temp) throws Exception {
        Path config = temp.resolve("redis.yaml");
        Files.writeString(
                config,
                "redisHost: localhost\nredisPort: 6379\njedisPoolMaxTotal: 8\njedisPoolMaxIdle: 8\n");
        RedisBenchmarkDriver driver = new RedisBenchmarkDriver();
        driver.initialize(config, mock(MeterRegistry.class));
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<BenchmarkProducer>> creations = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                creations.add(
                        executor.submit(
                                () -> {
                                    start.await();
                                    return driver.createProducer("topic").join();
                                }));
            }
            start.countDown();
            List<BenchmarkProducer> producers = new ArrayList<>();
            for (Future<BenchmarkProducer> creation : creations) {
                producers.add(creation.get(5, TimeUnit.SECONDS));
            }
            Field poolField = RedisBenchmarkProducer.class.getDeclaredField("pool");
            poolField.setAccessible(true);
            JedisPool shared = driver.jedisPool;
            assertThat(shared).isNotNull();
            for (BenchmarkProducer producer : producers) {
                assertThat(poolField.get(producer)).isSameAs(shared);
                producer.close();
            }
        } finally {
            start.countDown();
            executor.shutdownNow();
            driver.close();
        }
    }

    @Test
    void groupCreationFailureClosesTheBorrowedConnection() {
        RedisBenchmarkDriver driver = new RedisBenchmarkDriver();
        JedisPool pool = mock();
        Jedis jedis = mock();
        driver.jedisPool = pool;
        when(pool.getResource()).thenReturn(jedis);
        JedisDataException failure = new JedisDataException("NOAUTH");
        doThrow(failure).when(jedis).xgroupCreate(eq("topic"), eq("subscription"), isNull(), eq(true));

        assertThatThrownBy(
                        () ->
                                driver.createConsumer("topic", "subscription", mock(ConsumerCallback.class)).join())
                .hasCause(failure);
        verify(jedis).close();
    }

    @Test
    void driverCloseOwnsThePoolAndRejectsLaterCreation() {
        RedisBenchmarkDriver driver = new RedisBenchmarkDriver();
        JedisPool pool = mock();
        driver.jedisPool = pool;

        driver.close();
        driver.close();

        verify(pool, times(1)).close();
        assertThatThrownBy(() -> driver.createProducer("topic").join())
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                        () ->
                                driver.createConsumer("topic", "subscription", mock(ConsumerCallback.class)).join())
                .hasCauseInstanceOf(IllegalStateException.class);
    }
}
