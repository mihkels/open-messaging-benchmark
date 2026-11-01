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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.brainlag.nsq.Connection;
import com.github.brainlag.nsq.NSQCommand;
import com.github.brainlag.nsq.ServerAddress;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.ResponseFrame;
import com.github.brainlag.nsq.lookup.NSQLookup;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.openmessaging.benchmark.driver.AbandonableCreation;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreationCompletion;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class NsqBenchmarkConsumerTest {
    private final EventLoopGroup io =
            new MultiThreadIoEventLoopGroup(
                    1,
                    task -> {
                        Thread thread = new Thread(task, "nsq-test-io");
                        thread.setDaemon(true);
                        return thread;
                    },
                    NioIoHandler.newFactory());
    private final List<NsqBenchmarkConsumer> consumers = new ArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        try {
            for (NsqBenchmarkConsumer consumer : consumers) {
                consumer.close();
            }
        } finally {
            io.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
        }
    }

    @Test
    void waitsForActualSubAckAndReceivesAndFinishesMessages() throws Exception {
        try (StubBroker broker = new StubBroker(true, false, false)) {
            CountDownLatch received = new CountDownLatch(1);
            AtomicReference<byte[]> payload = new AtomicReference<>();
            AtomicReference<Long> timestamp = new AtomicReference<>();
            NsqBenchmarkConsumer consumer =
                    consumer(
                            new MutableLookup(Set.of(broker.address())),
                            (data, time) -> {
                                payload.set(data);
                                timestamp.set(time);
                                received.countDown();
                            });
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();

            assertThat(broker.subReceived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation).isNotDone();
            assertThat(broker.commands).noneMatch(command -> command.startsWith("RDY"));

            broker.releaseSub.countDown();
            assertThat(creation.get(5, TimeUnit.SECONDS)).isSameAs(consumer);
            assertThat(quiescence(creation).get(5, TimeUnit.SECONDS)).isNull();
            ((AbandonableCreation) creation).abandonCreation();
            assertThat(creation.cancel(true)).isFalse();
            assertThat(broker.awaitCommand("RDY ")).isEqualTo("RDY 200");
            long published = System.currentTimeMillis();
            broker.sendMessage("hello".getBytes(StandardCharsets.UTF_8), published);
            assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(payload.get()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
            assertThat(timestamp.get()).isEqualTo(published);
            assertThat(broker.awaitCommand("FIN ")).isEqualTo("FIN 0123456789abcdef");
            consumer.close();
            assertThat(broker.awaitCommand("CLS")).isEqualTo("CLS");
            assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void rejectedSubscriptionPreservesCauseAndClosesConnection() throws Exception {
        try (StubBroker broker = new StubBroker(false, true, false)) {
            NsqBenchmarkConsumer consumer =
                    consumer(new MutableLookup(Set.of(broker.address())), (data, time) -> {});
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();

            assertThatThrownBy(() -> creation.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(NSQException.class)
                    .hasRootCauseMessage("E_BAD_TOPIC rejected");
            assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(broker.commands).noneMatch(command -> command.startsWith("RDY"));
        }
    }

    @Test
    void emptyDiscoveryTimesOutAndReleasesOwnedScope() throws Exception {
        NsqEventLoopGroup scope = new NsqEventLoopGroup(io);
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of()),
                        (data, time) -> {},
                        addresses -> true,
                        scope,
                        300,
                        50,
                        Connection::new);
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();

        assertThatThrownBy(() -> creation.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TimeoutException.class);
        assertThat(scope.terminationFuture().await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(io.isShuttingDown()).isFalse();
    }

    @Test
    void cancellingDefaultBatchAbandonsNoNodeCreationWithoutWaitingForDeadline() throws Exception {
        MutableLookup lookup = new MutableLookup(Set.of());
        NsqEventLoopGroup scope = new NsqEventLoopGroup(io);
        NsqBenchmarkConsumer consumer =
                consumer(lookup, (data, time) -> {}, addresses -> true, scope, 30_000, 50, Connection::new);
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        try (NsqBenchmarkDriver driver = batchDriver(creation)) {
            CompletableFuture<List<BenchmarkConsumer>> batch = createBatch(driver);
            assertThat(lookup.emptyObserved.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(batch.cancel(false)).isTrue();
            quiescence(batch).get(5, TimeUnit.SECONDS);

            assertThat(creation).isCompletedExceptionally();
            assertThat(scope.isTerminated()).isTrue();
            assertThat(io.isShuttingDown()).isFalse();
        }
    }

    @Test
    void cancellingDefaultBatchClosesPendingSubConnection() throws Exception {
        try (StubBroker broker = new StubBroker(true, false, false)) {
            NsqBenchmarkConsumer consumer =
                    consumer(new MutableLookup(Set.of(broker.address())), (data, time) -> {});
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
            try (NsqBenchmarkDriver driver = batchDriver(creation)) {
                CompletableFuture<List<BenchmarkConsumer>> batch = createBatch(driver);
                assertThat(broker.subReceived.await(5, TimeUnit.SECONDS)).isTrue();

                assertThat(batch.cancel(false)).isTrue();
                broker.releaseSub.countDown();
                quiescence(batch).get(5, TimeUnit.SECONDS);

                assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(broker.commands).noneMatch(command -> command.startsWith("RDY"));
                assertThat(io.isShuttingDown()).isFalse();
            }
        }
    }

    @Test
    void batchCancellationIsNonblockingAndWaitsForLateConnectionCleanup() throws Exception {
        CountDownLatch connecting = new CountDownLatch(1);
        CountDownLatch releaseConnection = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        Connection late = mock(Connection.class);
        doAnswer(
                        invocation -> {
                            closing.countDown();
                            awaitIgnoringInterrupts(releaseClose);
                            return null;
                        })
                .when(late)
                .close();
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("localhost", 4150))),
                        (data, time) -> {},
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        30_000,
                        50,
                        (address, config) -> {
                            connecting.countDown();
                            awaitIgnoringInterrupts(releaseConnection);
                            return late;
                        });
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        ExecutorService canceller = Executors.newSingleThreadExecutor();
        try (NsqBenchmarkDriver driver = batchDriver(creation)) {
            CompletableFuture<List<BenchmarkConsumer>> batch = createBatch(driver);
            assertThat(connecting.await(5, TimeUnit.SECONDS)).isTrue();
            Future<Boolean> cancelled = canceller.submit(() -> batch.cancel(false));
            assertThat(cancelled.get(2, TimeUnit.SECONDS)).isTrue();
            assertThat(quiescence(batch)).isNotDone();

            releaseConnection.countDown();
            assertThat(closing.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> disposableView = quiescence(creation);
            disposableView.complete(null);
            assertThat(quiescence(creation)).isNotDone();
            assertThat(quiescence(batch)).isNotDone();

            releaseClose.countDown();
            quiescence(batch).get(5, TimeUnit.SECONDS);
            verify(late).close();
        } finally {
            releaseConnection.countDown();
            releaseClose.countDown();
            canceller.shutdownNow();
        }
    }

    @Test
    void batchCancellationDoesNotWaitForAnInProgressReadyWrite() throws Exception {
        CountDownLatch writingReady = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        Connection connection = mock(Connection.class);
        ChannelFuture write = mock(ChannelFuture.class);
        ResponseFrame ok = new ResponseFrame();
        ok.setData("OK".getBytes(StandardCharsets.UTF_8));
        when(connection.isConnected()).thenReturn(true);
        when(connection.isHeartbeatStatusOK()).thenReturn(true);
        when(connection.commandAndWait(any(NSQCommand.class))).thenReturn(ok);
        when(connection.command(any(NSQCommand.class)))
                .thenAnswer(
                        invocation -> {
                            writingReady.countDown();
                            awaitIgnoringInterrupts(releaseWrite);
                            return write;
                        });
        when(write.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(write.isSuccess()).thenReturn(true);
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("one", 1))),
                        (data, time) -> {},
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        30_000,
                        50,
                        (address, config) -> connection);
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        ExecutorService canceller = Executors.newSingleThreadExecutor();
        try (NsqBenchmarkDriver driver = batchDriver(creation)) {
            CompletableFuture<List<BenchmarkConsumer>> batch = createBatch(driver);
            assertThat(writingReady.await(5, TimeUnit.SECONDS)).isTrue();
            Future<Boolean> cancelled = canceller.submit(() -> batch.cancel(false));
            assertThat(cancelled.get(2, TimeUnit.SECONDS)).isTrue();
            assertThat(quiescence(batch)).isNotDone();

            releaseWrite.countDown();
            quiescence(batch).get(5, TimeUnit.SECONDS);
            verify(connection).close();
        } finally {
            releaseWrite.countDown();
            canceller.shutdownNow();
        }
    }

    @Test
    void batchQuiescenceReportsLateConnectionCleanupFailure() throws Exception {
        CountDownLatch connecting = new CountDownLatch(1);
        CountDownLatch releaseConnection = new CountDownLatch(1);
        Connection late = mock(Connection.class);
        RuntimeException cleanupFailure = new RuntimeException("late connection cleanup failed");
        doThrow(cleanupFailure).when(late).close();
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("localhost", 4150))),
                        (data, time) -> {},
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        30_000,
                        50,
                        (address, config) -> {
                            connecting.countDown();
                            awaitIgnoringInterrupts(releaseConnection);
                            return late;
                        });
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        try (NsqBenchmarkDriver driver = batchDriver(creation)) {
            CompletableFuture<List<BenchmarkConsumer>> batch = createBatch(driver);
            assertThat(connecting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(batch.cancel(false)).isTrue();
            assertThat(quiescence(batch)).isNotDone();
            releaseConnection.countDown();

            assertThatThrownBy(() -> quiescence(batch).get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCause(cleanupFailure);
            assertThatThrownBy(consumer::close).isSameAs(cleanupFailure);
            consumers.remove(consumer);
            verify(late).close();
        } finally {
            releaseConnection.countDown();
        }
    }

    @Test
    void batchQuiescenceWaitsForLateRejectedChannelCleanup() throws Exception {
        CountDownLatch connecting = new CountDownLatch(1);
        CountDownLatch releaseConnection = new CountDownLatch(1);
        CountDownLatch registered = new CountDownLatch(1);
        Channel channel = mock(Channel.class);
        Channel.Unsafe unsafe = mock(Channel.Unsafe.class);
        ChannelPromise channelClosed =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        AtomicBoolean open = new AtomicBoolean(true);
        when(channel.closeFuture()).thenReturn(channelClosed);
        when(channel.isOpen()).thenAnswer(invocation -> open.get());
        when(channel.unsafe()).thenReturn(unsafe);
        RuntimeException cleanupFailure = new RuntimeException("late channel force close failed");
        doThrow(cleanupFailure).when(unsafe).closeForcibly();
        Connection late = mock(Connection.class);
        NsqEventLoopGroup scope = new NsqEventLoopGroup(io);
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("localhost", 4150))),
                        (data, time) -> {},
                        addresses -> true,
                        scope,
                        30_000,
                        50,
                        (address, config) -> {
                            connecting.countDown();
                            awaitIgnoringInterrupts(releaseConnection);
                            try {
                                config.getEventLoopGroup().register(channel);
                            } finally {
                                registered.countDown();
                            }
                            return late;
                        });
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        try (NsqBenchmarkDriver driver = batchDriver(creation)) {
            CompletableFuture<List<BenchmarkConsumer>> batch = createBatch(driver);
            assertThat(connecting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(batch.cancel(false)).isTrue();
            assertThat(scope.terminationFuture().await(5, TimeUnit.SECONDS)).isTrue();
            releaseConnection.countDown();
            assertThat(registered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(quiescence(batch)).isNotDone();

            open.set(false);
            channelClosed.setSuccess();
            assertThatThrownBy(() -> quiescence(batch).get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCause(cleanupFailure);
            assertThatThrownBy(consumer::close).isSameAs(cleanupFailure);
            consumers.remove(consumer);
            verify(late).close();
        } finally {
            releaseConnection.countDown();
            open.set(false);
            channelClosed.trySuccess();
        }
    }

    @Test
    void quiescenceWaitsForStartupCompletionCallbacksAndSchedulerExit() throws Exception {
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of()),
                        (data, time) -> {},
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        250,
                        50,
                        Connection::new);
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        creation.whenComplete(
                (handle, failure) -> {
                    callbackEntered.countDown();
                    awaitIgnoringInterrupts(releaseCallback);
                });
        try {
            assertThat(callbackEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation).isCompletedExceptionally();
            assertThat(quiescence(creation)).isNotDone();
        } finally {
            releaseCallback.countDown();
        }
        quiescence(creation).get(5, TimeUnit.SECONDS);
    }

    @Test
    void consumerCanCloseFromItsCreationCompletionCallback() throws Exception {
        try (StubBroker broker = new StubBroker(true, false, false)) {
            NsqBenchmarkConsumer consumer =
                    consumer(new MutableLookup(Set.of(broker.address())), (data, time) -> {});
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
            CompletableFuture<Void> closed =
                    creation.thenAccept(
                            handle -> {
                                try {
                                    handle.close();
                                } catch (Exception failure) {
                                    throw new IllegalStateException(failure);
                                }
                            });
            assertThat(broker.subReceived.await(5, TimeUnit.SECONDS)).isTrue();
            broker.releaseSub.countDown();

            closed.get(5, TimeUnit.SECONDS);
            quiescence(creation).get(5, TimeUnit.SECONDS);
            assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void startupFailureCallbackCanCloseWithoutBlockingSchedulerTermination() throws Exception {
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of()),
                        (data, time) -> {},
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        250,
                        50,
                        Connection::new);
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        CompletableFuture<Void> closed =
                creation.handle(
                        (handle, failure) -> {
                            try {
                                consumer.close();
                            } catch (Exception closeFailure) {
                                throw new IllegalStateException(closeFailure);
                            }
                            return null;
                        });

        closed.get(5, TimeUnit.SECONDS);
        quiescence(creation).get(5, TimeUnit.SECONDS);
    }

    @Test
    void quiescenceWaitsForAnInterruptedButStillRunningMessageCallback() throws Exception {
        try (StubBroker broker = new StubBroker(false, false, false)) {
            CountDownLatch identityCheck = new CountDownLatch(1);
            CountDownLatch releaseIdentity = new CountDownLatch(1);
            CountDownLatch callbackEntered = new CountDownLatch(1);
            CountDownLatch releaseCallback = new CountDownLatch(1);
            NsqBenchmarkConsumer consumer =
                    consumer(
                            new MutableLookup(Set.of(broker.address())),
                            (data, time) -> {
                                callbackEntered.countDown();
                                awaitIgnoringInterrupts(releaseCallback);
                            },
                            addresses -> {
                                identityCheck.countDown();
                                awaitIgnoringInterrupts(releaseIdentity);
                                return true;
                            },
                            new NsqEventLoopGroup(io),
                            30_000,
                            50,
                            Connection::new);
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
            try {
                assertThat(identityCheck.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(broker.awaitCommand("RDY ")).isEqualTo("RDY 200");
                broker.sendMessage(new byte[] {1}, System.currentTimeMillis());
                assertThat(callbackEntered.await(5, TimeUnit.SECONDS)).isTrue();

                assertThat(creation.cancel(true)).isTrue();
                releaseIdentity.countDown();
                assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(quiescence(creation)).isNotDone();
            } finally {
                releaseIdentity.countDown();
                releaseCallback.countDown();
            }
            quiescence(creation).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void missingPublishingNodeCannotMakeConsumerReady() throws Exception {
        try (StubBroker broker = new StubBroker(false, false, false)) {
            NsqBenchmarkConsumer consumer =
                    consumer(
                            new MutableLookup(Set.of(broker.address())),
                            (data, time) -> {},
                            addresses -> false,
                            new NsqEventLoopGroup(io),
                            800,
                            50,
                            Connection::new);
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();

            assertThat(broker.subReceived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> creation.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TimeoutException.class);
            assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void connectionLostDuringPublisherIdentityCheckDoesNotCompleteReadiness() throws Exception {
        Connection connection = mock(Connection.class);
        ChannelFuture write = mock(ChannelFuture.class);
        AtomicBoolean connected = new AtomicBoolean(true);
        when(connection.isConnected()).thenAnswer(ignored -> connected.get());
        when(connection.isHeartbeatStatusOK()).thenReturn(true);
        ResponseFrame ok = new ResponseFrame();
        ok.setData("OK".getBytes(StandardCharsets.UTF_8));
        when(connection.commandAndWait(any(NSQCommand.class))).thenReturn(ok);
        when(connection.command(any(NSQCommand.class))).thenReturn(write);
        when(write.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(write.isSuccess()).thenReturn(true);
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("one", 1))),
                        (data, time) -> {},
                        addresses -> {
                            connected.set(false);
                            return true;
                        },
                        new NsqEventLoopGroup(io),
                        500,
                        50,
                        (address, config) -> connection);

        assertThatThrownBy(() -> consumer.startAsync().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TimeoutException.class);
        verify(connection, timeout(5000).atLeastOnce()).close();
    }

    @Test
    void cancellationDuringSubClosesTheActualConnection() throws Exception {
        try (StubBroker broker = new StubBroker(true, false, false)) {
            NsqBenchmarkConsumer consumer =
                    consumer(new MutableLookup(Set.of(broker.address())), (data, time) -> {});
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();

            assertThat(broker.subReceived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(true)).isTrue();
            broker.releaseSub.countDown();
            assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation).isCancelled();
            assertThat(broker.commands).noneMatch(command -> command.startsWith("RDY"));
            assertThat(io.isShuttingDown()).isFalse();
        }
    }

    @Test
    void cancellingOneConsumerDoesNotCloseAnotherConsumersSharedEventLoop() throws Exception {
        try (StubBroker first = new StubBroker(false, false, false);
                StubBroker second = new StubBroker(true, false, false)) {
            CountDownLatch delivered = new CountDownLatch(1);
            NsqBenchmarkConsumer established =
                    consumer(
                            new MutableLookup(Set.of(first.address())), (data, time) -> delivered.countDown());
            established.startAsync().get(5, TimeUnit.SECONDS);
            first.awaitCommand("RDY ");
            NsqBenchmarkConsumer pending =
                    consumer(new MutableLookup(Set.of(second.address())), (data, time) -> {});
            CompletableFuture<BenchmarkConsumer> creation = pending.startAsync();
            assertThat(second.subReceived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(true)).isTrue();
            second.releaseSub.countDown();
            assertThat(second.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
            first.sendMessage(new byte[] {1}, System.currentTimeMillis());
            assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(first.awaitCommand("FIN ")).isEqualTo("FIN 0123456789abcdef");
            established.close();
            assertThat(io.isShuttingDown()).isFalse();
        }
    }

    @Test
    void missingSubAckTimesOutAndClosesTheConnection() throws Exception {
        try (StubBroker broker = new StubBroker(true, false, false)) {
            NsqBenchmarkConsumer consumer =
                    consumer(
                            new MutableLookup(Set.of(broker.address())),
                            (data, time) -> {},
                            addresses -> true,
                            new NsqEventLoopGroup(io),
                            1500,
                            50,
                            Connection::new);
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();

            assertThat(broker.subReceived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> creation.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TimeoutException.class);
            broker.releaseSub.countDown();
            assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void cancellationClosesSocketWhileConnectionConstructorIsWaitingForIdentify() throws Exception {
        try (StubBroker broker = new StubBroker(false, false, true)) {
            NsqBenchmarkConsumer consumer =
                    consumer(new MutableLookup(Set.of(broker.address())), (data, time) -> {});
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();

            assertThat(broker.identifyReceived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(true)).isTrue();
            broker.releaseIdentify.countDown();
            assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(broker.commands).noneMatch(command -> command.startsWith("SUB "));
            assertThat(io.isShuttingDown()).isFalse();
        }
    }

    @Test
    void closesConnectionThatAppearsAfterCancellation() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Connection late = mock(Connection.class);
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("localhost", 4150))),
                        (data, time) -> {},
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        5000,
                        50,
                        (address, config) -> {
                            entered.countDown();
                            boolean interrupted = false;
                            try {
                                while (true) {
                                    try {
                                        release.await();
                                        break;
                                    } catch (InterruptedException ignored) {
                                        interrupted = true;
                                    }
                                }
                            } finally {
                                if (interrupted) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                            return late;
                        });
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(true)).isTrue();
        } finally {
            release.countDown();
        }
        verify(late, timeout(5000).times(1)).close();
    }

    @Test
    void cancellationBeforeSocketRegistrationPreventsALateConnection() throws Exception {
        try (StubBroker broker = new StubBroker(false, false, false)) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch attempted = new CountDownLatch(1);
            NsqBenchmarkConsumer consumer =
                    consumer(
                            new MutableLookup(Set.of(broker.address())),
                            (data, time) -> {},
                            addresses -> true,
                            new NsqEventLoopGroup(io),
                            5000,
                            50,
                            (address, config) -> {
                                entered.countDown();
                                while (true) {
                                    try {
                                        release.await();
                                        break;
                                    } catch (InterruptedException ignored) {
                                        // Model the client's uninterruptible connection constructor.
                                    }
                                }
                                try {
                                    return new Connection(address, config);
                                } finally {
                                    attempted.countDown();
                                }
                            });
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(creation.cancel(true)).isTrue();
            } finally {
                release.countDown();
            }
            assertThat(attempted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(broker.accepted.await(300, TimeUnit.MILLISECONDS)).isFalse();
            assertThat(io.isShuttingDown()).isFalse();
        }
    }

    @Test
    void failedIdentifyDoesNotLeakAConstructorOwnedSocket() throws Exception {
        try (StubBroker broker = new StubBroker(false, false, false, true)) {
            NsqBenchmarkConsumer consumer =
                    consumer(new MutableLookup(Set.of(broker.address())), (data, time) -> {});
            CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();

            assertThat(broker.identifyReceived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation).isNotDone();
            creation.cancel(true);
        }
    }

    @Test
    void rejectedSecondNodeClosesEarlierConnectionAndPreservesCleanupFailure() throws Exception {
        Connection good = mock(Connection.class);
        Connection bad = mock(Connection.class);
        ChannelFuture write = mock(ChannelFuture.class);
        when(good.isConnected()).thenReturn(true);
        when(good.isHeartbeatStatusOK()).thenReturn(true);
        ResponseFrame ok = new ResponseFrame();
        ok.setData("OK".getBytes(StandardCharsets.UTF_8));
        ErrorFrame rejected = new ErrorFrame();
        rejected.setData("E_BAD_TOPIC invalid".getBytes(StandardCharsets.UTF_8));
        when(good.commandAndWait(any(NSQCommand.class))).thenReturn(ok);
        when(bad.isConnected()).thenReturn(true);
        when(bad.commandAndWait(any(NSQCommand.class))).thenReturn(rejected);
        when(good.command(any(NSQCommand.class))).thenReturn(write);
        when(write.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(write.isSuccess()).thenReturn(true);
        RuntimeException cleanupFailure = new RuntimeException("cleanup failed");
        doThrow(cleanupFailure).when(bad).close();
        AtomicInteger created = new AtomicInteger();
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("one", 1), new ServerAddress("two", 2))),
                        (data, time) -> {},
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        5000,
                        50,
                        (address, config) -> created.getAndIncrement() == 0 ? good : bad);

        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        try {
            creation.get(5, TimeUnit.SECONDS);
            throw new AssertionError("Expected subscription failure");
        } catch (ExecutionException failure) {
            assertThat(failure.getCause()).isInstanceOf(NSQException.class);
            assertThat(failure.getCause().getMessage()).contains("E_BAD_TOPIC invalid");
            assertThat(failure.getCause().getSuppressed()).contains(cleanupFailure);
        }
        assertThatThrownBy(() -> quiescence(creation).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(cleanupFailure);
        assertThatThrownBy(consumer::close).isSameAs(cleanupFailure);
        consumers.remove(consumer);
        verify(good).close();
        verify(bad).close();
    }

    @Test
    void failedReadyWriteClosesThatConnectionBeforeRetrying() throws Exception {
        Connection failed = mock(Connection.class);
        Connection retry = mock(Connection.class);
        ChannelFuture failedWrite = mock(ChannelFuture.class);
        ChannelFuture goodWrite = mock(ChannelFuture.class);
        ResponseFrame ok = new ResponseFrame();
        ok.setData("OK".getBytes(StandardCharsets.UTF_8));
        ResponseFrame closeWait = new ResponseFrame();
        closeWait.setData("CLOSE_WAIT".getBytes(StandardCharsets.UTF_8));
        for (Connection connection : List.of(failed, retry)) {
            when(connection.isConnected()).thenReturn(true);
            when(connection.isHeartbeatStatusOK()).thenReturn(true);
            when(connection.commandAndWait(any(NSQCommand.class))).thenReturn(ok, closeWait);
        }
        when(failed.command(any(NSQCommand.class))).thenReturn(failedWrite);
        when(failedWrite.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(failedWrite.isSuccess()).thenReturn(false);
        when(failedWrite.cause()).thenReturn(new IOException("RDY write failed"));
        when(retry.command(any(NSQCommand.class))).thenReturn(goodWrite);
        when(goodWrite.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(goodWrite.isSuccess()).thenReturn(true);
        AtomicInteger created = new AtomicInteger();
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("one", 1))),
                        (data, time) -> {},
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        5000,
                        60_000,
                        (address, config) -> created.getAndIncrement() == 0 ? failed : retry);

        assertThat(consumer.startAsync().get(5, TimeUnit.SECONDS)).isSameAs(consumer);
        verify(failed).close();
        consumer.close();
        verify(retry).close();
    }

    @Test
    void closeReportsConnectionFailureWithoutSkippingOtherResources() throws Exception {
        Connection first = mock(Connection.class);
        Connection second = mock(Connection.class);
        ChannelFuture write = mock(ChannelFuture.class);
        ResponseFrame ok = new ResponseFrame();
        ok.setData("OK".getBytes(StandardCharsets.UTF_8));
        ResponseFrame closeWait = new ResponseFrame();
        closeWait.setData("CLOSE_WAIT".getBytes(StandardCharsets.UTF_8));
        for (Connection connection : List.of(first, second)) {
            when(connection.isConnected()).thenReturn(true);
            when(connection.isHeartbeatStatusOK()).thenReturn(true);
            when(connection.commandAndWait(any(NSQCommand.class))).thenReturn(ok, closeWait);
            when(connection.command(any(NSQCommand.class))).thenReturn(write);
        }
        when(write.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(write.isSuccess()).thenReturn(true);
        RuntimeException failure = new RuntimeException("connection close failed");
        doThrow(failure).when(first).close();
        AtomicInteger created = new AtomicInteger();
        NsqEventLoopGroup scope = new NsqEventLoopGroup(io);
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("one", 1), new ServerAddress("two", 2))),
                        (data, time) -> {},
                        addresses -> true,
                        scope,
                        5000,
                        60_000,
                        (address, config) -> created.getAndIncrement() == 0 ? first : second);
        consumer.startAsync().get(5, TimeUnit.SECONDS);

        assertThatThrownBy(consumer::close).isSameAs(failure);
        assertThatThrownBy(consumer::close).isSameAs(failure);
        verify(first).close();
        verify(second).close();
        assertThat(scope.isTerminated()).isTrue();
        consumers.remove(consumer);
    }

    @Test
    void closeDuringStartupReportsCleanupFailureAndQuiescenceWaitsForConnector() throws Exception {
        CountDownLatch subscribed = new CountDownLatch(1);
        CountDownLatch releaseSub = new CountDownLatch(1);
        Connection connection = mock(Connection.class);
        when(connection.isConnected()).thenReturn(true);
        doAnswer(
                        invocation -> {
                            subscribed.countDown();
                            awaitIgnoringInterrupts(releaseSub);
                            ResponseFrame ok = new ResponseFrame();
                            ok.setData("OK".getBytes(StandardCharsets.UTF_8));
                            return ok;
                        })
                .when(connection)
                .commandAndWait(any(NSQCommand.class));
        RuntimeException cleanupFailure = new RuntimeException("startup close failed");
        doThrow(cleanupFailure).when(connection).close();
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("one", 1))),
                        (data, time) -> {},
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        30_000,
                        50,
                        (address, config) -> connection);
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        try {
            assertThat(subscribed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(consumer::close).isSameAs(cleanupFailure);
            assertThat(quiescence(creation)).isNotDone();
        } finally {
            releaseSub.countDown();
        }
        assertThatThrownBy(() -> quiescence(creation).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(cleanupFailure);
        assertThatThrownBy(consumer::close).isSameAs(cleanupFailure);
        consumers.remove(consumer);
        verify(connection).close();
    }

    @Test
    void closeTimeoutIsReportedAndRemainingResourcesAreClosed() throws Exception {
        Connection connection = mock(Connection.class);
        ChannelFuture write = mock(ChannelFuture.class);
        ResponseFrame ok = new ResponseFrame();
        ok.setData("OK".getBytes(StandardCharsets.UTF_8));
        TimeoutException timeout = new TimeoutException("CLS timed out");
        when(connection.isConnected()).thenReturn(true);
        when(connection.isHeartbeatStatusOK()).thenReturn(true);
        when(connection.commandAndWait(any(NSQCommand.class))).thenReturn(ok).thenThrow(timeout);
        when(connection.command(any(NSQCommand.class))).thenReturn(write);
        when(write.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(write.isSuccess()).thenReturn(true);
        NsqEventLoopGroup scope = new NsqEventLoopGroup(io);
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("one", 1))),
                        (data, time) -> {},
                        addresses -> true,
                        scope,
                        5000,
                        60_000,
                        (address, config) -> connection);
        consumer.startAsync().get(5, TimeUnit.SECONDS);

        assertThatThrownBy(consumer::close).isSameAs(timeout);
        assertThat(scope.terminationFuture().await(5, TimeUnit.SECONDS)).isTrue();
        verify(connection).close();
        consumers.remove(consumer);
    }

    @Test
    void boundedCloseReportsTimeoutWhileCleanupAndQuiescenceContinue() throws Exception {
        CountDownLatch subscribed = new CountDownLatch(1);
        CountDownLatch releaseSub = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        Connection connection = mock(Connection.class);
        when(connection.isConnected()).thenReturn(true);
        doAnswer(
                        invocation -> {
                            subscribed.countDown();
                            awaitIgnoringInterrupts(releaseSub);
                            ResponseFrame ok = new ResponseFrame();
                            ok.setData("OK".getBytes(StandardCharsets.UTF_8));
                            return ok;
                        })
                .when(connection)
                .commandAndWait(any(NSQCommand.class));
        doAnswer(
                        invocation -> {
                            closing.countDown();
                            awaitIgnoringInterrupts(releaseClose);
                            return null;
                        })
                .when(connection)
                .close();
        NsqBenchmarkConsumer consumer =
                new NsqBenchmarkConsumer(
                        new MutableLookup(Set.of(new ServerAddress("one", 1))),
                        "topic",
                        "group",
                        adapt((data, time) -> {}),
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        new NsqBenchmarkConsumer.Settings(
                                30_000, 50, 25, (address, config) -> connection, 200));
        consumers.add(consumer);
        CompletableFuture<BenchmarkConsumer> creation = consumer.startAsync();
        try {
            assertThat(subscribed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(consumer::close)
                    .isInstanceOf(TimeoutException.class)
                    .hasMessageContaining("close did not finish");
            assertThat(closing.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(quiescence(creation)).isNotDone();
        } finally {
            releaseClose.countDown();
            releaseSub.countDown();
        }
        quiescence(creation).get(5, TimeUnit.SECONDS);
        consumer.close();
        verify(connection).close();
    }

    @Test
    void recursiveCloseDuringConnectionCleanupDoesNotWaitOnItself() throws Exception {
        Connection connection = mock(Connection.class);
        ChannelFuture write = mock(ChannelFuture.class);
        ResponseFrame ok = new ResponseFrame();
        ok.setData("OK".getBytes(StandardCharsets.UTF_8));
        ResponseFrame closeWait = new ResponseFrame();
        closeWait.setData("CLOSE_WAIT".getBytes(StandardCharsets.UTF_8));
        when(connection.isConnected()).thenReturn(true);
        when(connection.isHeartbeatStatusOK()).thenReturn(true);
        when(connection.commandAndWait(any(NSQCommand.class))).thenReturn(ok, closeWait);
        when(connection.command(any(NSQCommand.class))).thenReturn(write);
        when(write.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(write.isSuccess()).thenReturn(true);
        AtomicReference<NsqBenchmarkConsumer> handle = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            handle.get().close();
                            return null;
                        })
                .when(connection)
                .close();
        NsqBenchmarkConsumer consumer =
                consumer(
                        new MutableLookup(Set.of(new ServerAddress("one", 1))),
                        (data, time) -> {},
                        addresses -> true,
                        new NsqEventLoopGroup(io),
                        5000,
                        60_000,
                        (address, config) -> connection);
        handle.set(consumer);
        consumer.startAsync().get(5, TimeUnit.SECONDS);
        ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            Future<?> result =
                    closer.submit(
                            () -> {
                                consumer.close();
                                return null;
                            });
            result.get(5, TimeUnit.SECONDS);
            verify(connection).close();
        } finally {
            closer.shutdownNow();
        }
    }

    @Test
    void preservesHealthyConnectionOnEmptyLookupAndReconnectsToNewNode() throws Exception {
        try (StubBroker first = new StubBroker(false, false, false);
                StubBroker second = new StubBroker(false, false, false)) {
            MutableLookup lookup = new MutableLookup(Set.of(first.address()));
            CountDownLatch delivered = new CountDownLatch(1);
            NsqBenchmarkConsumer consumer =
                    consumer(
                            lookup,
                            (data, time) -> delivered.countDown(),
                            addresses -> true,
                            new NsqEventLoopGroup(io),
                            5000,
                            100,
                            Connection::new);
            consumer.startAsync().get(5, TimeUnit.SECONDS);
            assertThat(first.awaitCommand("RDY ")).isEqualTo("RDY 200");

            lookup.addresses.set(Set.of());
            assertThat(lookup.emptyObserved.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(first.disconnected.getCount()).isEqualTo(1);
            lookup.addresses.set(Set.of(second.address()));
            assertThat(second.subReceived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(second.awaitCommand("RDY ")).isEqualTo("RDY 200");
            assertThat(first.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
            second.sendMessage(new byte[] {1}, System.currentTimeMillis());
            assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(second.awaitCommand("FIN ")).isEqualTo("FIN 0123456789abcdef");
        }
    }

    @Test
    void closeWaitsForCallbackToFinishAndSendFin() throws Exception {
        try (StubBroker broker = new StubBroker(false, false, false)) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            NsqBenchmarkConsumer consumer =
                    consumer(
                            new MutableLookup(Set.of(broker.address())),
                            (data, time) -> {
                                entered.countDown();
                                try {
                                    release.await();
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                }
                            });
            consumer.startAsync().get(5, TimeUnit.SECONDS);
            broker.awaitCommand("RDY ");
            broker.sendMessage(new byte[] {1}, System.currentTimeMillis());
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            ExecutorService closer = Executors.newFixedThreadPool(2);
            try {
                Future<?> closing =
                        closer.submit(
                                () -> {
                                    consumer.close();
                                    return null;
                                });
                assertThat(broker.awaitCommand("CLS")).isEqualTo("CLS");
                assertThat(closing).isNotDone();
                CountDownLatch secondStarted = new CountDownLatch(1);
                Future<?> closingAgain =
                        closer.submit(
                                () -> {
                                    secondStarted.countDown();
                                    consumer.close();
                                    return null;
                                });
                assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(closingAgain).isNotDone();
                release.countDown();
                closing.get(5, TimeUnit.SECONDS);
                closingAgain.get(5, TimeUnit.SECONDS);
                assertThat(broker.awaitCommand("FIN ")).isEqualTo("FIN 0123456789abcdef");
                assertThat(broker.disconnected.await(5, TimeUnit.SECONDS)).isTrue();
            } finally {
                release.countDown();
                closer.shutdownNow();
            }
        }
    }

    @Test
    void reentrantCloseIsRejectedAndCanBeRetried() throws Exception {
        try (StubBroker broker = new StubBroker(false, false, false)) {
            AtomicReference<NsqBenchmarkConsumer> handle = new AtomicReference<>();
            AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            CountDownLatch delivered = new CountDownLatch(1);
            NsqBenchmarkConsumer consumer =
                    consumer(
                            new MutableLookup(Set.of(broker.address())),
                            (data, time) -> {
                                try {
                                    handle.get().close();
                                } catch (Exception failure) {
                                    closeFailure.set(failure);
                                }
                                delivered.countDown();
                            });
            consumer.startAsync().get(5, TimeUnit.SECONDS);
            handle.set(consumer);
            broker.awaitCommand("RDY ");
            broker.sendMessage(new byte[] {1}, System.currentTimeMillis());
            assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(closeFailure.get())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("retry from another thread");
            assertThat(broker.awaitCommand("FIN ")).isEqualTo("FIN 0123456789abcdef");
            consumer.close();
            assertThat(broker.awaitCommand("CLS")).isEqualTo("CLS");
        }
    }

    private static NsqBenchmarkDriver batchDriver(CompletableFuture<BenchmarkConsumer> creation) {
        return new NsqBenchmarkDriver() {
            @Override
            public CompletableFuture<BenchmarkConsumer> createConsumer(
                    String topic, String subscription, ConsumerCallback callback) {
                return creation;
            }
        };
    }

    private static CompletableFuture<List<BenchmarkConsumer>> createBatch(NsqBenchmarkDriver driver) {
        return driver.createConsumers(
                List.of(new BenchmarkDriver.ConsumerInfo(1, "topic", "group", adapt((data, time) -> {}))));
    }

    private static CompletableFuture<Void> quiescence(CompletableFuture<?> future) {
        assertThat(future).isInstanceOf(ResourceCreationCompletion.class);
        return ((ResourceCreationCompletion) future).quiescence();
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private NsqBenchmarkConsumer consumer(NSQLookup lookup, TestCallback callback) {
        return consumer(
                lookup,
                callback,
                addresses -> true,
                new NsqEventLoopGroup(io),
                5000,
                60_000,
                Connection::new);
    }

    private NsqBenchmarkConsumer consumer(
            NSQLookup lookup,
            TestCallback callback,
            Predicate<Set<ServerAddress>> publisherPresent,
            NsqEventLoopGroup scope,
            long timeout,
            long refresh,
            NsqBenchmarkConsumer.ConnectionFactory factory) {
        NsqBenchmarkConsumer consumer =
                new NsqBenchmarkConsumer(
                        lookup,
                        "topic",
                        "group",
                        adapt(callback),
                        publisherPresent,
                        scope,
                        new NsqBenchmarkConsumer.Settings(timeout, refresh, 25, factory));
        consumers.add(consumer);
        return consumer;
    }

    private static ConsumerCallback adapt(TestCallback callback) {
        return new ConsumerCallback() {
            @Override
            public void messageReceived(byte[] data, long time) {
                callback.received(data, time);
            }

            @Override
            public void messageReceived(ByteBuffer data, long time) {
                ByteBuffer copy = data.duplicate();
                byte[] bytes = new byte[copy.remaining()];
                copy.get(bytes);
                callback.received(bytes, time);
            }
        };
    }

    @FunctionalInterface
    private interface TestCallback {
        void received(byte[] data, long time);
    }

    private static final class MutableLookup implements NSQLookup {
        final AtomicReference<Set<ServerAddress>> addresses;
        final CountDownLatch emptyObserved = new CountDownLatch(1);

        MutableLookup(Set<ServerAddress> addresses) {
            this.addresses = new AtomicReference<>(addresses);
        }

        @Override
        public Set<ServerAddress> lookup(String topic) {
            Set<ServerAddress> result = addresses.get();
            if (result.isEmpty()) {
                emptyObserved.countDown();
            }
            return result;
        }

        @Override
        public void addLookupAddress(String address, int port) {}
    }

    private static final class StubBroker implements AutoCloseable {
        private final ServerSocket server;
        private final boolean rejectSub;
        private final boolean rejectIdentify;
        private final CopyOnWriteArrayList<Socket> sockets = new CopyOnWriteArrayList<>();
        private final CountDownLatch accepted = new CountDownLatch(1);
        private final CountDownLatch identifyReceived = new CountDownLatch(1);
        private final CountDownLatch releaseIdentify;
        private final CountDownLatch subReceived = new CountDownLatch(1);
        private final CountDownLatch releaseSub;
        private final CountDownLatch disconnected = new CountDownLatch(1);
        private final BlockingQueue<String> commands = new LinkedBlockingQueue<>();
        private final AtomicReference<DataOutputStream> output = new AtomicReference<>();
        private final Object outputLock = new Object();
        private final Thread listener;

        StubBroker(boolean delaySub, boolean rejectSub, boolean delayIdentify) throws IOException {
            this(delaySub, rejectSub, delayIdentify, false);
        }

        StubBroker(boolean delaySub, boolean rejectSub, boolean delayIdentify, boolean rejectIdentify)
                throws IOException {
            this.rejectSub = rejectSub;
            this.rejectIdentify = rejectIdentify;
            releaseSub = new CountDownLatch(delaySub ? 1 : 0);
            releaseIdentify = new CountDownLatch(delayIdentify ? 1 : 0);
            server = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
            listener = new Thread(this::accept, "nsq-test-broker");
            listener.setDaemon(true);
            listener.start();
        }

        ServerAddress address() {
            return new ServerAddress(server.getInetAddress().getHostAddress(), server.getLocalPort());
        }

        String awaitCommand(String prefix) throws InterruptedException, TimeoutException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (true) {
                long remaining = deadline - System.nanoTime();
                String command = remaining > 0 ? commands.poll(remaining, TimeUnit.NANOSECONDS) : null;
                if (command == null) {
                    throw new TimeoutException("Missing NSQ command " + prefix);
                }
                if (command.startsWith(prefix)) {
                    return command;
                }
            }
        }

        void sendMessage(byte[] body, long timestamp) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream message = new DataOutputStream(bytes)) {
                message.writeLong(TimeUnit.MILLISECONDS.toNanos(timestamp));
                message.writeShort(1);
                message.write("0123456789abcdef".getBytes(StandardCharsets.US_ASCII));
                message.write(body);
            }
            sendFrame(2, bytes.toByteArray());
        }

        private void accept() {
            try {
                while (!server.isClosed()) {
                    Socket socket = server.accept();
                    sockets.add(socket);
                    accepted.countDown();
                    Thread connection = new Thread(() -> serve(socket), "nsq-test-connection");
                    connection.setDaemon(true);
                    connection.start();
                }
            } catch (IOException ignored) {
                // Closing the server wakes accept.
            }
        }

        private void serve(Socket socket) {
            try (socket;
                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    DataOutputStream socketOutput = new DataOutputStream(socket.getOutputStream())) {
                output.set(socketOutput);
                if (!"  V2".equals(new String(input.readNBytes(4), StandardCharsets.US_ASCII))) {
                    return;
                }
                if (!"IDENTIFY".equals(readLine(input))) {
                    return;
                }
                int length = input.readInt();
                input.readNBytes(length);
                identifyReceived.countDown();
                releaseIdentify.await();
                if (rejectIdentify) {
                    sendFrame(1, "E_BAD_BODY identify rejected".getBytes(StandardCharsets.UTF_8));
                    String unexpected;
                    while ((unexpected = readLine(input)) != null) {
                        commands.add(unexpected);
                    }
                    return;
                }
                sendFrame(0, "{\"version\":\"1.3.0\"}".getBytes(StandardCharsets.UTF_8));
                String command;
                while ((command = readLine(input)) != null) {
                    commands.add(command);
                    if (command.startsWith("SUB ")) {
                        subReceived.countDown();
                        releaseSub.await();
                        sendFrame(
                                rejectSub ? 1 : 0,
                                (rejectSub ? "E_BAD_TOPIC rejected" : "OK").getBytes(StandardCharsets.UTF_8));
                    } else if (command.equals("CLS")) {
                        sendFrame(0, "CLOSE_WAIT".getBytes(StandardCharsets.UTF_8));
                    }
                }
            } catch (IOException ignored) {
                // Cancellation and cleanup close the socket while a response is pending.
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                disconnected.countDown();
            }
        }

        private void sendFrame(int type, byte[] payload) throws IOException {
            synchronized (outputLock) {
                DataOutputStream stream = output.get();
                if (stream == null) {
                    throw new IOException("No NSQ connection");
                }
                stream.writeInt(4 + payload.length);
                stream.writeInt(type);
                stream.write(payload);
                stream.flush();
            }
        }

        private static String readLine(DataInputStream input) throws IOException {
            ByteArrayOutputStream line = new ByteArrayOutputStream();
            try {
                while (true) {
                    int next = input.readUnsignedByte();
                    if (next == '\n') {
                        return line.toString(StandardCharsets.US_ASCII);
                    }
                    line.write(next);
                }
            } catch (EOFException end) {
                return null;
            }
        }

        @Override
        public void close() throws IOException {
            releaseIdentify.countDown();
            releaseSub.countDown();
            server.close();
            for (Socket socket : sockets) {
                socket.close();
            }
        }
    }
}
