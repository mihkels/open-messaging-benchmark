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
package io.openmessaging.benchmark.driver.nsq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.brainlag.nsq.Connection;
import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.ServerAddress;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.sun.net.httpserver.HttpServer;
import io.openmessaging.benchmark.driver.AbandonableCreation;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreationCompletion;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;

class NsqBenchmarkDriverTest {
    @TempDir Path temp;

    @Test
    void defaultsToNsqdStandardHttpPort() {
        assertThat(new NsqConfig().nsqdHttpPort).isEqualTo(4151);
        NsqConfig config =
                new ObjectMapper(new YAMLFactory())
                        .readValue("nsqdHost: localhost\nlookupHost: localhost\n", NsqConfig.class);
        assertThat(config.nsqdHttpPort).isEqualTo(4151);
    }

    @Test
    void createsTopicsOnConfiguredHttpPort() throws Exception {
        AtomicReference<String> request = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/topic/create",
                exchange -> {
                    request.set(exchange.getRequestMethod() + " " + exchange.getRequestURI());
                    exchange.sendResponseHeaders(200, -1);
                    exchange.close();
                });
        server.start();
        try (NsqBenchmarkDriver driver = new NsqBenchmarkDriver()) {
            Path config =
                    Files.writeString(
                            temp.resolve("nsq.yaml"),
                            "nsqdHost: 127.0.0.1\nlookupHost: 127.0.0.1\nnsqdHttpPort: "
                                    + server.getAddress().getPort()
                                    + "\n");
            driver.initialize(config, null);
            driver.createTopic("benchmark-topic", 1).get(5, TimeUnit.SECONDS);
            assertThat(request.get()).isEqualTo("POST /topic/create?topic=benchmark-topic");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsInvalidHttpPort() throws Exception {
        Path config =
                Files.writeString(
                        temp.resolve("nsq.yaml"),
                        "nsqdHost: localhost\nlookupHost: localhost\nnsqdHttpPort: 65536\n");
        try (NsqBenchmarkDriver driver = new NsqBenchmarkDriver()) {
            assertThatThrownBy(() -> driver.initialize(config, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nsqdHttpPort");
        }
    }

    @Test
    void startupFailuresCompleteExceptionallyAndClosePartialClients() throws Exception {
        NSQProducer producer = mock(NSQProducer.class);
        NsqBenchmarkConsumer consumer = mock(NsqBenchmarkConsumer.class);
        RuntimeException producerFailure = new RuntimeException("producer start failed");
        RuntimeException consumerFailure = new RuntimeException("consumer start failed");
        RuntimeException cleanupFailure = new RuntimeException("producer shutdown failed");
        RuntimeException consumerCleanupFailure = new RuntimeException("consumer shutdown failed");
        doThrow(producerFailure).when(producer).start();
        doThrow(cleanupFailure).when(producer).shutdown();
        doThrow(consumerFailure).when(consumer).startAsync();
        doThrow(consumerCleanupFailure).when(consumer).close();
        Path inModule = Path.of("nsq.yaml");
        Path config = Files.exists(inModule) ? inModule : Path.of("driver-nsq", "nsq.yaml");
        try (NsqBenchmarkDriver driver =
                new NsqBenchmarkDriver() {
                    @Override
                    NSQProducer newProducer() {
                        return producer;
                    }

                    @Override
                    NsqBenchmarkConsumer newConsumer(
                            NsqLookup lookup,
                            String topic,
                            String subscription,
                            ConsumerCallback callback,
                            NsqEventLoopGroup scope) {
                        return consumer;
                    }
                }) {
            driver.initialize(config, null);
            assertThatThrownBy(() -> driver.createProducer("topic").join()).hasCause(producerFailure);
            assertThat(producerFailure.getSuppressed()).containsExactly(cleanupFailure);
            assertThatThrownBy(
                            () -> driver.createConsumer("topic", "group", mock(ConsumerCallback.class)).join())
                    .hasCause(consumerFailure);
            assertThat(consumerFailure.getSuppressed()).containsExactly(consumerCleanupFailure);
            verify(producer).shutdown();
            verify(consumer).close();
        }
    }

    @Test
    void driverCloseCancelsPendingConsumerCreation() throws Exception {
        AtomicReference<NsqEventLoopGroup> scope = new AtomicReference<>();
        NSQLookup emptyLookup =
                new NSQLookup() {
                    @Override
                    public Set<ServerAddress> lookup(String topic) {
                        return Set.of();
                    }

                    @Override
                    public void addLookupAddress(String address, int port) {}
                };
        Path inModule = Path.of("nsq.yaml");
        Path config = Files.exists(inModule) ? inModule : Path.of("driver-nsq", "nsq.yaml");
        try (NsqBenchmarkDriver driver =
                new NsqBenchmarkDriver() {
                    @Override
                    NsqBenchmarkConsumer newConsumer(
                            NsqLookup lookup,
                            String topic,
                            String subscription,
                            ConsumerCallback callback,
                            NsqEventLoopGroup consumerScope) {
                        scope.set(consumerScope);
                        return new NsqBenchmarkConsumer(
                                emptyLookup,
                                topic,
                                subscription,
                                callback,
                                addresses -> true,
                                consumerScope,
                                new NsqBenchmarkConsumer.Settings(5000, 60_000, 25, Connection::new));
                    }
                }) {
            driver.initialize(config, null);
            CompletableFuture<BenchmarkConsumer> creation =
                    driver.createConsumer("topic", "group", mock(ConsumerCallback.class));
            assertThat(creation).isNotDone();
            driver.close();
            assertThat(creation).isCancelled();
            assertThat(scope.get().terminationFuture().await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void defaultBatchCancellationPropagatesThroughDriverCreation() throws Exception {
        CountDownLatch lookedUp = new CountDownLatch(1);
        AtomicReference<NsqEventLoopGroup> scope = new AtomicReference<>();
        NSQLookup emptyLookup =
                new NSQLookup() {
                    @Override
                    public Set<ServerAddress> lookup(String topic) {
                        lookedUp.countDown();
                        return Set.of();
                    }

                    @Override
                    public void addLookupAddress(String address, int port) {}
                };
        Path inModule = Path.of("nsq.yaml");
        Path config = Files.exists(inModule) ? inModule : Path.of("driver-nsq", "nsq.yaml");
        try (NsqBenchmarkDriver driver =
                new NsqBenchmarkDriver() {
                    @Override
                    NsqBenchmarkConsumer newConsumer(
                            NsqLookup lookup,
                            String topic,
                            String subscription,
                            ConsumerCallback callback,
                            NsqEventLoopGroup consumerScope) {
                        scope.set(consumerScope);
                        return new NsqBenchmarkConsumer(
                                emptyLookup,
                                topic,
                                subscription,
                                callback,
                                addresses -> true,
                                consumerScope,
                                new NsqBenchmarkConsumer.Settings(30_000, 60_000, 25, Connection::new));
                    }
                }) {
            driver.initialize(config, null);
            CompletableFuture<List<BenchmarkConsumer>> batch =
                    driver.createConsumers(
                            List.of(
                                    new BenchmarkDriver.ConsumerInfo(
                                            1, "topic", "group", mock(ConsumerCallback.class))));
            assertThat(lookedUp.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(batch.cancel(false)).isTrue();
            ((ResourceCreationCompletion) batch).quiescence().get(5, TimeUnit.SECONDS);
            assertThat(scope.get().isTerminated()).isTrue();
        }
    }

    @Test
    void driverCloseBeforeCreationIsTrackedClosesAlreadyCreatedConsumer() throws Exception {
        NsqBenchmarkConsumer consumer = mock(NsqBenchmarkConsumer.class);
        RuntimeException cleanupFailure = new RuntimeException("consumer cleanup failed");
        AtomicReference<NsqBenchmarkDriver> owner = new AtomicReference<>();
        when(consumer.startAsync())
                .thenAnswer(
                        invocation -> {
                            owner.get().close();
                            return CompletableFuture.completedFuture(consumer);
                        });
        doThrow(cleanupFailure).when(consumer).close();
        Path inModule = Path.of("nsq.yaml");
        Path config = Files.exists(inModule) ? inModule : Path.of("driver-nsq", "nsq.yaml");
        try (NsqBenchmarkDriver driver =
                new NsqBenchmarkDriver() {
                    @Override
                    NsqBenchmarkConsumer newConsumer(
                            NsqLookup lookup,
                            String topic,
                            String subscription,
                            ConsumerCallback callback,
                            NsqEventLoopGroup scope) {
                        return consumer;
                    }
                }) {
            owner.set(driver);
            driver.initialize(config, null);

            CompletableFuture<BenchmarkConsumer> creation =
                    driver.createConsumer("topic", "group", mock(ConsumerCallback.class));

            Throwable failure = creation.handle((handle, error) -> error).join();
            assertThat(failure)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("driver is closed");
            assertThat(failure.getSuppressed()).containsExactly(cleanupFailure);
            verify(consumer).close();
        }
    }

    @Test
    void driverCloseBeforeStartupReturnsFailureWithCleanupBarrier() throws Exception {
        CountDownLatch constructed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean lookupCalled = new AtomicBoolean();
        AtomicReference<NsqEventLoopGroup> scope = new AtomicReference<>();
        NSQLookup lookup =
                new NSQLookup() {
                    @Override
                    public Set<ServerAddress> lookup(String topic) {
                        lookupCalled.set(true);
                        return Set.of();
                    }

                    @Override
                    public void addLookupAddress(String address, int port) {}
                };
        Path inModule = Path.of("nsq.yaml");
        Path config = Files.exists(inModule) ? inModule : Path.of("driver-nsq", "nsq.yaml");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (NsqBenchmarkDriver driver =
                new NsqBenchmarkDriver() {
                    @Override
                    NsqBenchmarkConsumer newConsumer(
                            NsqLookup ignored,
                            String topic,
                            String subscription,
                            ConsumerCallback callback,
                            NsqEventLoopGroup consumerScope) {
                        scope.set(consumerScope);
                        NsqBenchmarkConsumer consumer =
                                new NsqBenchmarkConsumer(
                                        lookup, topic, subscription, callback, addresses -> true, consumerScope);
                        constructed.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                        return consumer;
                    }
                }) {
            driver.initialize(config, null);
            Future<CompletableFuture<BenchmarkConsumer>> call =
                    executor.submit(
                            () -> driver.createConsumer("topic", "group", mock(ConsumerCallback.class)));
            assertThat(constructed.await(5, TimeUnit.SECONDS)).isTrue();
            driver.close();
            release.countDown();
            CompletableFuture<BenchmarkConsumer> creation = call.get(5, TimeUnit.SECONDS);

            assertThat(creation).isInstanceOf(AbandonableCreation.class);
            assertThatThrownBy(creation::join)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("NSQ driver is closed");
            ((ResourceCreationCompletion) creation).quiescence().get(5, TimeUnit.SECONDS);
            assertThat(lookupCalled).isFalse();
            assertThat(scope.get().isTerminated()).isTrue();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void driverCloseDoesNotCloseAHandedOffConsumer() throws Exception {
        NsqBenchmarkConsumer consumer = mock(NsqBenchmarkConsumer.class);
        when(consumer.startAsync()).thenReturn(CompletableFuture.completedFuture(consumer));
        Path inModule = Path.of("nsq.yaml");
        Path config = Files.exists(inModule) ? inModule : Path.of("driver-nsq", "nsq.yaml");
        try (NsqBenchmarkDriver driver =
                new NsqBenchmarkDriver() {
                    @Override
                    NsqBenchmarkConsumer newConsumer(
                            NsqLookup lookup,
                            String topic,
                            String subscription,
                            ConsumerCallback callback,
                            NsqEventLoopGroup scope) {
                        return consumer;
                    }
                }) {
            driver.initialize(config, null);
            CompletableFuture<BenchmarkConsumer> creation =
                    driver.createConsumer("topic", "group", mock(ConsumerCallback.class));
            driver.close();
            assertThat(creation).isCompletedWithValue(consumer);
            verify(consumer, never()).close();
        }
    }
}
