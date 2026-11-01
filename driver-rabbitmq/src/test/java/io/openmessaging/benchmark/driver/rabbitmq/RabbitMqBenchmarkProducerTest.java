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
package io.openmessaging.benchmark.driver.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.ChannelN;
import com.rabbitmq.client.impl.ConnectionParams;
import com.rabbitmq.client.impl.ConsumerWorkService;
import com.rabbitmq.client.impl.Frame;
import com.rabbitmq.client.impl.FrameHandler;
import com.rabbitmq.client.impl.SocketFrameHandler;
import com.rabbitmq.client.impl.SocketFrameHandlerFactory;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import com.rabbitmq.client.impl.recovery.RecoveryAwareChannelN;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RabbitMqBenchmarkProducerTest {

    private FakeChannel channel;
    private RabbitMqBenchmarkProducer producer;

    @BeforeEach
    void setUp() {
        channel = new FakeChannel();
        producer = new RabbitMqBenchmarkProducer(channel.proxy, "exchange", false);
    }

    @AfterEach
    void tearDown() throws Exception {
        producer.close();
    }

    @Test
    void publishesExistingPropertiesAndRoutingKeys() throws Exception {
        byte[] payload = {1, 2, 3};
        long before = System.currentTimeMillis();
        CompletableFuture<Void> first = producer.sendAsync(Optional.of("routing-key"), payload);
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), payload);
        long after = System.currentTimeMillis();

        assertThat(channel.published).hasSize(2);
        Published firstPublish = channel.published.get(0);
        assertThat(firstPublish.exchange()).isEqualTo("exchange");
        assertThat(firstPublish.routingKey()).isEqualTo("routing-key");
        assertThat(firstPublish.payload()).isSameAs(payload);
        assertThat(firstPublish.properties().getTimestamp().getTime()).isBetween(before, after);
        assertThat(firstPublish.properties().getDeliveryMode()).isNull();
        assertThat(channel.published.get(1).routingKey()).isEmpty();

        FakeChannel persistentChannel = new FakeChannel();
        RabbitMqBenchmarkProducer persistent =
                new RabbitMqBenchmarkProducer(persistentChannel.proxy, "persistent-exchange", true);
        try {
            CompletableFuture<Void> persisted = persistent.sendAsync(Optional.empty(), payload);
            assertThat(persistentChannel.published.getFirst().properties().getDeliveryMode())
                    .isEqualTo(2);
            persistentChannel.ack(1, false);
            assertThat(persisted.join()).isNull();
        } finally {
            persistent.close();
        }

        channel.ack(2, true);
        assertThat(first.join()).isNull();
        assertThat(second.join()).isNull();
    }

    @Test
    void singleAckAndNackSettleOnlyTheirOwnSends() throws Exception {
        CompletableFuture<Void> first = send();
        CompletableFuture<Void> second = send();
        CompletableFuture<Void> third = send();
        AtomicInteger firstCompletions = observe(first);
        AtomicInteger secondCompletions = observe(second);

        channel.nack(2, false);
        assertNacked(second);
        assertThat(first.isDone()).isFalse();
        assertThat(third.isDone()).isFalse();

        channel.ack(1, false);
        assertThat(first.join()).isNull();
        channel.ack(2, false);
        channel.nack(1, false);
        channel.ack(100, false);
        assertThat(firstCompletions).hasValue(1);
        assertThat(secondCompletions).hasValue(1);
        assertThat(third.isDone()).isFalse();

        channel.ack(3, false);
        assertThat(third.join()).isNull();
    }

    @Test
    void rangeAckAndNackAreInclusiveAndLeaveLaterSendsPending() throws Exception {
        List<CompletableFuture<Void>> sends = sendMany(6);

        channel.ack(2, true);
        assertThat(sends.get(0).join()).isNull();
        assertThat(sends.get(1).join()).isNull();
        assertThat(sends.get(2).isDone()).isFalse();

        channel.nack(4, true);
        assertNacked(sends.get(2));
        assertNacked(sends.get(3));
        assertThat(sends.get(4).isDone()).isFalse();
        assertThat(sends.get(5).isDone()).isFalse();

        channel.ack(5, false);
        channel.nack(6, true);
        assertThat(sends.get(4).join()).isNull();
        assertNacked(sends.get(5));
    }

    @Test
    void rangeConfirmationsHandleUnsignedSequenceBoundariesWithoutOverflow() throws Exception {
        channel.nextSequence.set(Long.MAX_VALUE - 1);
        List<CompletableFuture<Void>> sends = sendMany(4);

        channel.ack(0, true);
        assertThat(sends).allMatch(future -> !future.isDone());
        channel.ack(Long.MAX_VALUE, true);
        assertThat(sends.get(0).join()).isNull();
        assertThat(sends.get(1).join()).isNull();
        assertThat(sends.get(2).isDone()).isFalse();
        assertThat(sends.get(3).isDone()).isFalse();

        channel.nack(Long.MIN_VALUE, true);
        assertNacked(sends.get(2));
        assertThat(sends.get(3).isDone()).isFalse();
        channel.ack(Long.MIN_VALUE + 1, true);
        assertThat(sends.get(3).join()).isNull();

        channel.nextSequence.set(-2);
        List<CompletableFuture<Void>> unsignedMaximum = sendMany(2);
        channel.ack(-1, true);
        assertThat(unsignedMaximum.get(0).join()).isNull();
        assertThat(unsignedMaximum.get(1).join()).isNull();
    }

    @Test
    void failedPublishIsRemovedBeforeTheSequenceIsReused() throws Exception {
        IOException error = new IOException("publish failed");
        AtomicInteger attempts = new AtomicInteger();
        channel.beforePublish =
                () -> {
                    if (attempts.getAndIncrement() == 0) {
                        throw error;
                    }
                };

        CompletableFuture<Void> failed = send();
        assertFailedWith(failed, error);
        assertThat(channel.nextSequence).hasValue(1);
        CompletableFuture<Void> next = send();
        assertThat(next.isDone()).isFalse();
        channel.ack(1, false);
        assertThat(next.join()).isNull();
        assertFailedWith(failed, error);
        assertThat(channel.publishAttempts).hasValue(2);
    }

    @Test
    void sequenceLookupFailureIsReturnedAsAnExceptionalFuture() throws Exception {
        IllegalStateException error = new IllegalStateException("sequence unavailable");
        channel.nextSequenceFailure.set(error);
        assertFailedWith(send(), error);
        assertThat(channel.publishAttempts).hasValue(0);

        CompletableFuture<Void> next = send();
        channel.ack(1, false);
        assertThat(next.join()).isNull();
    }

    @Test
    void duplicateSequenceCannotReplaceAnOutstandingSend() throws Exception {
        CompletableFuture<Void> first = send();
        channel.nextSequence.set(1);
        CompletableFuture<Void> duplicate = send();

        assertThatThrownBy(duplicate::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Duplicate RabbitMQ publish sequence: 1");
        assertThat(channel.publishAttempts).hasValue(1);
        assertThat(first.isDone()).isFalse();
        channel.ack(1, false);
        assertThat(first.join()).isNull();
    }

    @Test
    void confirmationSelectedBeforePublishExceptionWins() throws Exception {
        IOException error = new IOException("failure after confirmation");
        channel.afterPublish =
                published -> {
                    channel.ack(published.sequence(), false);
                    throw error;
                };

        CompletableFuture<Void> future = send();
        assertThat(future.join()).isNull();
        channel.nack(1, false);
        assertThat(future.join()).isNull();
    }

    @Test
    void nackSelectedBeforePublishExceptionWins() {
        IOException error = new IOException("failure after negative confirmation");
        channel.afterPublish =
                published -> {
                    channel.nack(published.sequence(), false);
                    throw error;
                };
        assertNacked(send());
    }

    @Test
    void cancellationRemovesThePendingSendAndLateConfirmCannotChangeIt() throws Exception {
        CompletableFuture<Void> canceled = send();
        AtomicInteger completions = observe(canceled);
        assertThat(canceled.cancel(true)).isTrue();
        assertThat(canceled.isCancelled()).isTrue();
        assertThatThrownBy(canceled::join).isInstanceOf(CancellationException.class);

        channel.nextSequence.set(1);
        CompletableFuture<Void> next = send();
        channel.ack(1, false);
        channel.nack(1, false);
        assertThat(next.join()).isNull();
        assertThat(canceled.isCancelled()).isTrue();
        assertThat(completions).hasValue(1);
    }

    @Test
    void blockingTransportWritesPayloadBeforeCancellationReturns() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                Socket writer = new Socket(server.getInetAddress(), server.getLocalPort());
                Socket reader = server.accept()) {
            reader.setSoTimeout(5000);
            BlockingConnectionFactory factory = new BlockingConnectionFactory();
            factory.setAutomaticRecoveryEnabled(true);
            SocketFrameHandler handler = factory.createBlockingHandler(writer);
            BlockingConnection connection = new BlockingConnection(factory.params(executor), handler);
            ConsumerWorkService work = new ConsumerWorkService(executor, Thread::new, 1000);
            RecoveryAwareChannelN delegate = new RecoveryAwareChannelN(connection, 1, work);
            Field sequence = ChannelN.class.getDeclaredField("nextPublishSeqNo");
            sequence.setAccessible(true);
            sequence.setLong(delegate, 1);
            RecoverableTransportChannel transportChannel = new RecoverableTransportChannel(delegate);
            RabbitMqBenchmarkProducer realProducer =
                    new RabbitMqBenchmarkProducer(transportChannel, "exchange", false);
            try {
                byte[] payload = new byte[4096];
                for (int i = 0; i < payload.length; i++) {
                    payload[i] = (byte) (i * 13);
                }
                byte[] expected = payload.clone();
                CompletableFuture<Void> future = realProducer.sendAsync(Optional.of("key"), payload);
                assertThat(future.isDone()).isFalse();
                assertThat(future.cancel(false)).isTrue();
                Arrays.fill(payload, (byte) 0x7f);

                DataInputStream input = new DataInputStream(reader.getInputStream());
                Frame method = Frame.readFrom(input, () -> Integer.MAX_VALUE);
                Frame header = Frame.readFrom(input, () -> Integer.MAX_VALUE);
                Frame body = Frame.readFrom(input, () -> Integer.MAX_VALUE);
                assertThat(method.getType()).isEqualTo(AMQP.FRAME_METHOD);
                assertThat(header.getType()).isEqualTo(AMQP.FRAME_HEADER);
                assertThat(body.getType()).isEqualTo(AMQP.FRAME_BODY);
                assertThat(body.getPayload()).isEqualTo(expected);
            } finally {
                realProducer.close();
                work.shutdown();
            }
        } finally {
            shutdownExecutor(executor);
        }
    }

    @Test
    void closeFailsPendingSendsAndRejectsFutureRegistrations() throws Exception {
        CompletableFuture<Void> first = send();
        CompletableFuture<Void> second = send();
        producer.close();

        assertClosed(first);
        assertClosed(second);
        int previousLookups = channel.sequenceLookups.get();
        assertClosed(send());
        assertThat(channel.sequenceLookups).hasValue(previousLookups);
        assertThat(channel.confirmRemoved).isTrue();
        assertThat(channel.shutdownRemoved).isTrue();
        assertThat(channel.closeCalls).hasValue(1);

        channel.ack(1, true);
        channel.nack(2, false);
        assertClosed(first);
        assertClosed(second);
        producer.close();
        assertThat(channel.closeCalls).hasValue(1);
    }

    @Test
    void failureCallbacksCannotRecursivelyCloseTheProducer() throws Exception {
        CompletableFuture<Void> first = send();
        CompletableFuture<Void> second = send();
        CompletableFuture<Void> callback =
                first.handle(
                        (ignored, error) -> {
                            assertThatThrownBy(producer::close).isInstanceOf(IllegalStateException.class);
                            return null;
                        });

        producer.close();
        callback.join();
        assertClosed(first);
        assertClosed(second);
        assertThat(channel.closeCalls).hasValue(1);
    }

    @Test
    void acknowledgedCallbackCloseIsRejectedWithoutClosingTheProducer() throws Exception {
        CompletableFuture<Void> first = send();
        CompletableFuture<Void> callback =
                first.thenRun(
                        () -> assertThatThrownBy(producer::close).isInstanceOf(IllegalStateException.class));

        channel.ack(1, false);

        callback.join();
        CompletableFuture<Void> second = send();
        assertThat(second.isDone()).isFalse();
        producer.close();
        assertClosed(second);
        assertThat(channel.closeCalls).hasValue(1);
    }

    @Test
    void closeCallbacksRunWithoutHoldingThePendingStateMonitor() throws Exception {
        CompletableFuture<Void> first = send();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<CompletableFuture<Void>> callback =
                    first.handle(
                            (ignored, error) -> {
                                try {
                                    return executor.submit(this::send).get(5, TimeUnit.SECONDS);
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            });
            producer.close();
            assertClosed(callback.get(5, TimeUnit.SECONDS));
        } finally {
            shutdownExecutor(executor);
        }
    }

    @Test
    void closeStillSettlesSendsWhenChannelCloseFails() throws Exception {
        CompletableFuture<Void> pending = send();
        IOException error = new IOException("close failed");
        channel.closeFailure = error;

        assertThatThrownBy(producer::close).isSameAs(error);
        assertClosed(pending);
        assertClosed(send());
        assertThat(channel.confirmRemoved).isTrue();
        assertThat(channel.shutdownRemoved).isTrue();
        assertThat(channel.closeCalls).hasValue(1);
    }

    @Test
    void closeAttemptsAllCleanupAndPreservesSuppressedFailures() throws Exception {
        CompletableFuture<Void> pending = send();
        IllegalStateException removeConfirm = new IllegalStateException("remove confirm");
        IllegalArgumentException removeShutdown = new IllegalArgumentException("remove shutdown");
        IOException close = new IOException("close");
        channel.removeConfirmFailure = removeConfirm;
        channel.removeShutdownFailure = removeShutdown;
        channel.closeFailure = close;

        assertThatThrownBy(producer::close).isSameAs(removeConfirm);
        assertThat(removeConfirm.getSuppressed()).containsExactly(removeShutdown, close);
        assertThat(channel.confirmRemoveAttempts).hasValue(1);
        assertThat(channel.shutdownRemoveAttempts).hasValue(1);
        assertThat(channel.closeCalls).hasValue(1);
        assertClosed(pending);
    }

    @Test
    void shutdownFailsPendingSendsAndPermanentlyRejectsNewOnes() throws Exception {
        CompletableFuture<Void> first = send();
        CompletableFuture<Void> second = send();
        ShutdownSignalException signal = shutdownSignal(channel);

        channel.shutdown(signal);
        assertFailedWith(first, signal);
        assertFailedWith(second, signal);
        assertFailedWith(send(), signal);
        assertThat(channel.sequenceLookups).hasValue(2);
        channel.ack(2, true);
        assertFailedWith(first, signal);
        assertFailedWith(second, signal);

        producer.close();
        assertThat(channel.confirmRemoved).isTrue();
        assertThat(channel.shutdownRemoved).isTrue();
        assertThat(channel.closeCalls).hasValue(1);
    }

    @Test
    void shutdownWithoutASignalStillClosesTheProducer() {
        CompletableFuture<Void> pending = send();
        channel.shutdown(null);
        assertThatThrownBy(pending::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("RabbitMQ channel shut down");
        assertThatThrownBy(send()::join)
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("RabbitMQ channel shut down");
    }

    @Test
    void channelAlreadyShutdownWhenListenerIsAddedIsRejected() throws Exception {
        FakeChannel closedChannel = new FakeChannel();
        ShutdownSignalException signal = shutdownSignal(closedChannel);
        closedChannel.initialShutdown = signal;
        closedChannel.closeFailure = new AlreadyClosedException(signal);
        RabbitMqBenchmarkProducer closedProducer =
                new RabbitMqBenchmarkProducer(closedChannel.proxy, "exchange", false);

        assertFailedWith(closedProducer.sendAsync(Optional.empty(), new byte[0]), signal);
        assertThat(closedChannel.sequenceLookups).hasValue(0);
        closedProducer.close();
        assertThat(closedChannel.confirmRemoved).isTrue();
        assertThat(closedChannel.shutdownRemoved).isTrue();
        assertThat(closedChannel.closeCalls).hasValue(1);
    }

    @Test
    void recoveredChannelCanPublishAndIgnoresCallbacksFromItsOldGeneration() throws Exception {
        FakeChannel recoveringChannel = new FakeChannel(true);
        RabbitMqBenchmarkProducer recoveringProducer =
                new RabbitMqBenchmarkProducer(recoveringChannel.proxy, "exchange", false);
        try {
            CompletableFuture<Void> oldSend = recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            ConfirmListener oldConfirm = recoveringChannel.confirmListener;
            ShutdownListener oldShutdown = recoveringChannel.shutdownListener;
            ShutdownSignalException signal = shutdownSignal(recoveringChannel);
            recoveringChannel.shutdown(signal);
            assertFailedWith(oldSend, signal);
            assertFailedWith(recoveringProducer.sendAsync(Optional.empty(), new byte[0]), signal);

            recoveringChannel.nextSequence.set(1);
            recoveringChannel.recover();
            CompletableFuture<Void> recovered =
                    recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            oldConfirm.handleAck(1, false);
            oldConfirm.handleNack(1, true);
            oldShutdown.shutdownCompleted(signal);
            assertThat(recovered.isDone()).isFalse();
            recoveringChannel.ack(1, false);
            assertThat(recovered.join()).isNull();
        } finally {
            recoveringProducer.close();
        }
        assertThat(recoveringChannel.recoveryRemoved).isTrue();
    }

    @Test
    void recoveryStartStopsSendsEvenWhenShutdownWasNotReported() throws Exception {
        FakeChannel recoveringChannel = new FakeChannel(true);
        RabbitMqBenchmarkProducer recoveringProducer =
                new RabbitMqBenchmarkProducer(recoveringChannel.proxy, "exchange", false);
        try {
            CompletableFuture<Void> pending = recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            recoveringChannel.recoveryListener.handleRecoveryStarted(
                    (Recoverable) recoveringChannel.proxy);
            assertThatThrownBy(pending::join)
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("RabbitMQ channel is recovering");
            assertThatThrownBy(() -> recoveringProducer.sendAsync(Optional.empty(), new byte[0]).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("RabbitMQ channel is recovering");
            assertThat(recoveringChannel.sequenceLookups).hasValue(1);

            recoveringChannel.recoveryListener.handleRecovery((Recoverable) recoveringChannel.proxy);
            CompletableFuture<Void> recovered =
                    recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            recoveringChannel.ack(2, false);
            assertThat(recovered.join()).isNull();
        } finally {
            recoveringProducer.close();
        }
    }

    @Test
    void recoveryCanSucceedAfterAnUnfinishedRecoveryAttempt() throws Exception {
        FakeChannel recoveringChannel = new FakeChannel(true);
        RabbitMqBenchmarkProducer recoveringProducer =
                new RabbitMqBenchmarkProducer(recoveringChannel.proxy, "exchange", false);
        try {
            Recoverable recoverable = (Recoverable) recoveringChannel.proxy;
            recoveringChannel.recoveryListener.handleRecoveryStarted(recoverable);
            assertThatThrownBy(() -> recoveringProducer.sendAsync(Optional.empty(), new byte[0]).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("RabbitMQ channel is recovering");
            assertThat(recoveringChannel.sequenceLookups).hasValue(0);

            recoveringChannel.recoveryListener.handleRecoveryStarted(recoverable);
            recoveringChannel.recoveryListener.handleRecovery(recoverable);
            CompletableFuture<Void> recovered =
                    recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            recoveringChannel.ack(1, false);
            assertThat(recovered.join()).isNull();
        } finally {
            recoveringProducer.close();
        }
    }

    @Test
    void recoveryStartedDuringListenerInstallationCannotReopenEarly() throws Exception {
        FakeChannel recoveringChannel = new FakeChannel(true);
        RabbitMqBenchmarkProducer recoveringProducer =
                new RabbitMqBenchmarkProducer(recoveringChannel.proxy, "exchange", false);
        try {
            AtomicBoolean startAnotherRecovery = new AtomicBoolean(true);
            recoveringChannel.beforeAddConfirm =
                    () -> {
                        if (startAnotherRecovery.getAndSet(false)) {
                            recoveringChannel.recoveryListener.handleRecoveryStarted(
                                    (Recoverable) recoveringChannel.proxy);
                        }
                    };
            recoveringChannel.recover();
            assertThatThrownBy(() -> recoveringProducer.sendAsync(Optional.empty(), new byte[0]).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("RabbitMQ channel is recovering");
            assertThat(recoveringChannel.sequenceLookups).hasValue(0);

            recoveringChannel.recoveryListener.handleRecovery((Recoverable) recoveringChannel.proxy);
            CompletableFuture<Void> recovered =
                    recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            recoveringChannel.ack(1, false);
            assertThat(recovered.join()).isNull();
        } finally {
            recoveringProducer.close();
        }
    }

    @Test
    void shutdownDuringRecoveryPreventsReopeningUntilTheNextRecovery() throws Exception {
        FakeChannel recoveringChannel = new FakeChannel(true);
        RabbitMqBenchmarkProducer recoveringProducer =
                new RabbitMqBenchmarkProducer(recoveringChannel.proxy, "exchange", false);
        try {
            ShutdownSignalException first = shutdownSignal(recoveringChannel);
            ShutdownSignalException second = shutdownSignal(recoveringChannel);
            recoveringChannel.shutdown(first);
            recoveringChannel.initialShutdown = second;
            recoveringChannel.recover();
            assertFailedWith(recoveringProducer.sendAsync(Optional.empty(), new byte[0]), second);

            recoveringChannel.initialShutdown = null;
            recoveringChannel.nextSequence.set(1);
            recoveringChannel.recover();
            CompletableFuture<Void> recovered =
                    recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            recoveringChannel.ack(1, false);
            assertThat(recovered.join()).isNull();
        } finally {
            recoveringProducer.close();
        }
    }

    @Test
    void recoveryWhilePublishIsBlockedKeepsLaterSequencesDistinct() throws Exception {
        FakeChannel recoveringChannel = new FakeChannel(true);
        RabbitMqBenchmarkProducer recoveringProducer =
                new RabbitMqBenchmarkProducer(recoveringChannel.proxy, "exchange", false);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch enteredPublish = new CountDownLatch(1);
        CountDownLatch releasePublish = new CountDownLatch(1);
        recoveringChannel.beforePublish =
                () -> {
                    enteredPublish.countDown();
                    await(releasePublish);
                };
        try {
            Future<CompletableFuture<Void>> old =
                    executor.submit(() -> recoveringProducer.sendAsync(Optional.empty(), new byte[0]));
            assertThat(enteredPublish.await(5, TimeUnit.SECONDS)).isTrue();
            ShutdownSignalException signal = shutdownSignal(recoveringChannel);
            recoveringChannel.shutdown(signal);
            recoveringChannel.nextSequence.set(1);
            recoveringChannel.recover();
            releasePublish.countDown();
            assertFailedWith(old.get(5, TimeUnit.SECONDS), signal);

            recoveringChannel.beforePublish = () -> {};
            CompletableFuture<Void> recovered =
                    recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            recoveringChannel.ack(1, false);
            assertThat(recovered.isDone()).isFalse();
            recoveringChannel.ack(2, false);
            assertThat(recovered.join()).isNull();
        } finally {
            releasePublish.countDown();
            shutdownExecutor(executor);
            recoveringProducer.close();
        }
    }

    @Test
    void recoveryCannotRegisterAnOldInFlightSequence() throws Exception {
        FakeChannel recoveringChannel = new FakeChannel(true);
        RabbitMqBenchmarkProducer recoveringProducer =
                new RabbitMqBenchmarkProducer(recoveringChannel.proxy, "exchange", false);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch enteredLookup = new CountDownLatch(1);
        CountDownLatch releaseLookup = new CountDownLatch(1);
        recoveringChannel.beforeSequence =
                () -> {
                    enteredLookup.countDown();
                    awaitUnchecked(releaseLookup);
                };
        try {
            Future<CompletableFuture<Void>> old =
                    executor.submit(() -> recoveringProducer.sendAsync(Optional.empty(), new byte[0]));
            assertThat(enteredLookup.await(5, TimeUnit.SECONDS)).isTrue();
            ShutdownSignalException signal = shutdownSignal(recoveringChannel);
            recoveringChannel.shutdown(signal);
            recoveringChannel.recover();
            releaseLookup.countDown();
            assertFailedWith(old.get(5, TimeUnit.SECONDS), signal);
            assertThat(recoveringChannel.publishAttempts).hasValue(0);

            recoveringChannel.beforeSequence = () -> {};
            CompletableFuture<Void> recovered =
                    recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            recoveringChannel.ack(1, false);
            assertThat(recovered.join()).isNull();
        } finally {
            releaseLookup.countDown();
            shutdownExecutor(executor);
            recoveringProducer.close();
        }
    }

    @Test
    void closeRacingRecoveryCannotReopenOrLeaveNewListenersRegistered() throws Exception {
        FakeChannel recoveringChannel = new FakeChannel(true);
        RabbitMqBenchmarkProducer recoveringProducer =
                new RabbitMqBenchmarkProducer(recoveringChannel.proxy, "exchange", false);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch installing = new CountDownLatch(1);
        CountDownLatch finishInstalling = new CountDownLatch(1);
        ShutdownSignalException signal = shutdownSignal(recoveringChannel);
        recoveringChannel.shutdown(signal);
        recoveringChannel.beforeAddConfirm =
                () -> {
                    installing.countDown();
                    awaitUnchecked(finishInstalling);
                };
        try {
            Future<?> recovery = executor.submit(recoveringChannel::recover);
            assertThat(installing.await(5, TimeUnit.SECONDS)).isTrue();
            recoveringProducer.close();
            finishInstalling.countDown();
            recovery.get(5, TimeUnit.SECONDS);
            assertFailedWith(recoveringProducer.sendAsync(Optional.empty(), new byte[0]), signal);
            assertThat(recoveringChannel.confirmRemoved).isTrue();
            assertThat(recoveringChannel.shutdownRemoved).isTrue();
            assertThat(recoveringChannel.recoveryRemoved).isTrue();
            assertThat(recoveringChannel.closeCalls).hasValue(1);
        } finally {
            finishInstalling.countDown();
            shutdownExecutor(executor);
            recoveringProducer.close();
        }
    }

    @Test
    void recoveryReportedDuringInitializationIsNotMissed() throws Exception {
        FakeChannel recoveringChannel = new FakeChannel(true);
        recoveringChannel.recoverOnRegistration = true;
        RabbitMqBenchmarkProducer recoveringProducer =
                new RabbitMqBenchmarkProducer(recoveringChannel.proxy, "exchange", false);
        try {
            CompletableFuture<Void> result = recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            recoveringChannel.ack(1, false);
            assertThat(result.join()).isNull();
            assertThat(recoveringChannel.confirmRegistrations).hasValue(2);
        } finally {
            recoveringProducer.close();
        }
    }

    @Test
    void concurrentSendsKeepSequenceLookupAndPublishTogether() throws Exception {
        int count = 64;
        ExecutorService executor = Executors.newFixedThreadPool(12);
        CountDownLatch start = new CountDownLatch(1);
        channel.beforePublish = Thread::yield;
        try {
            List<Future<CompletableFuture<Void>>> calls = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int index = i;
                calls.add(
                        executor.submit(
                                () -> {
                                    start.await();
                                    return producer.sendAsync(
                                            Optional.of(Integer.toString(index)), new byte[] {(byte) index});
                                }));
            }
            start.countDown();
            List<CompletableFuture<Void>> sends = new ArrayList<>();
            for (Future<CompletableFuture<Void>> call : calls) {
                sends.add(call.get(5, TimeUnit.SECONDS));
            }

            assertThat(channel.published).hasSize(count);
            assertThat(channel.sequenceLookups).hasValue(count);
            assertThat(channel.maxConcurrentPublishes).hasValue(1);
            assertThat(sends).allMatch(future -> !future.isDone());
            for (Published published : channel.published) {
                int index = Integer.parseInt(published.routingKey());
                assertThat(sends.get(index).isDone()).isFalse();
                channel.ack(published.sequence(), false);
                assertThat(sends.get(index).join()).isNull();
            }
        } finally {
            start.countDown();
            shutdownExecutor(executor);
        }
    }

    @Test
    void closeWakesWaitingSendsWithoutWaitingForBlockedPublish() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch enteredPublish = new CountDownLatch(1);
        CountDownLatch releasePublish = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        channel.beforePublish =
                () -> {
                    enteredPublish.countDown();
                    await(releasePublish);
                };
        try {
            Future<CompletableFuture<Void>> first = executor.submit(this::send);
            assertThat(enteredPublish.await(5, TimeUnit.SECONDS)).isTrue();
            Future<CompletableFuture<Void>> second =
                    executor.submit(
                            () -> {
                                secondStarted.countDown();
                                return send();
                            });
            assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();

            Future<?> close =
                    executor.submit(
                            () -> {
                                producer.close();
                                return null;
                            });
            close.get(5, TimeUnit.SECONDS);
            assertClosed(second.get(5, TimeUnit.SECONDS));
            assertThat(first.isDone()).isFalse();
            assertThat(channel.sequenceLookups).hasValue(1);

            releasePublish.countDown();
            assertClosed(first.get(5, TimeUnit.SECONDS));
            assertThat(channel.closeCalls).hasValue(1);
        } finally {
            releasePublish.countDown();
            shutdownExecutor(executor);
        }
    }

    @Test
    void closeDuringSequenceLookupPreventsRegistrationAndPublication() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch enteredLookup = new CountDownLatch(1);
        CountDownLatch releaseLookup = new CountDownLatch(1);
        channel.beforeSequence =
                () -> {
                    enteredLookup.countDown();
                    awaitUnchecked(releaseLookup);
                };
        try {
            Future<CompletableFuture<Void>> first = executor.submit(this::send);
            assertThat(enteredLookup.await(5, TimeUnit.SECONDS)).isTrue();
            producer.close();
            assertClosed(send());
            releaseLookup.countDown();
            assertClosed(first.get(5, TimeUnit.SECONDS));
            assertThat(channel.sequenceLookups).hasValue(1);
            assertThat(channel.publishAttempts).hasValue(0);
        } finally {
            releaseLookup.countDown();
            shutdownExecutor(executor);
        }
    }

    @Test
    void interruptedWaitingSendFailsAndRestoresInterruptStatus() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch enteredPublish = new CountDownLatch(1);
        CountDownLatch releasePublish = new CountDownLatch(1);
        channel.beforePublish =
                () -> {
                    enteredPublish.countDown();
                    await(releasePublish);
                };
        try {
            Future<CompletableFuture<Void>> first = executor.submit(this::send);
            assertThat(enteredPublish.await(5, TimeUnit.SECONDS)).isTrue();
            Future<InterruptedSend> interrupted =
                    executor.submit(
                            () -> {
                                Thread.currentThread().interrupt();
                                CompletableFuture<Void> result = send();
                                boolean interruptRestored = Thread.currentThread().isInterrupted();
                                Thread.interrupted();
                                return new InterruptedSend(result, interruptRestored);
                            });
            InterruptedSend result = interrupted.get(5, TimeUnit.SECONDS);
            assertThat(result.interruptRestored()).isTrue();
            assertThatThrownBy(result.future()::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(channel.sequenceLookups).hasValue(1);

            releasePublish.countDown();
            CompletableFuture<Void> firstSend = first.get(5, TimeUnit.SECONDS);
            channel.ack(1, false);
            assertThat(firstSend.join()).isNull();
        } finally {
            releasePublish.countDown();
            shutdownExecutor(executor);
        }
    }

    @Test
    void asynchronousConfirmIsNotDelayedByAnInFlightPublish() throws Exception {
        CompletableFuture<Void> first = send();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch enteredPublish = new CountDownLatch(1);
        CountDownLatch releasePublish = new CountDownLatch(1);
        channel.beforePublish =
                () -> {
                    enteredPublish.countDown();
                    await(releasePublish);
                };
        try {
            Future<CompletableFuture<Void>> second = executor.submit(this::send);
            assertThat(enteredPublish.await(5, TimeUnit.SECONDS)).isTrue();
            channel.ack(1, false);
            assertThat(first.isDone()).isTrue();
            assertThat(first.join()).isNull();

            releasePublish.countDown();
            CompletableFuture<Void> secondSend = second.get(5, TimeUnit.SECONDS);
            channel.ack(2, false);
            assertThat(secondSend.join()).isNull();
        } finally {
            releasePublish.countDown();
            shutdownExecutor(executor);
        }
    }

    @Test
    void synchronousConfirmCallbacksCanSendWithoutHoldingThePublishGate() throws Exception {
        CompletableFuture<Void> first = send();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<CompletableFuture<Void>> callback =
                    first.thenApply(
                            ignored -> {
                                try {
                                    return executor.submit(this::send).get(5, TimeUnit.SECONDS);
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            });
            channel.afterPublish =
                    published -> {
                        if (published.sequence() == 2) {
                            channel.ack(1, false);
                        }
                    };
            CompletableFuture<Void> second = send();
            CompletableFuture<Void> third = callback.get(5, TimeUnit.SECONDS);
            assertThat(second.isDone()).isFalse();
            assertThat(third.isDone()).isFalse();
            channel.ack(3, true);
            assertThat(second.join()).isNull();
            assertThat(third.join()).isNull();
        } finally {
            shutdownExecutor(executor);
        }
    }

    @Test
    void closeHelpsAConfirmationSelectedByABlockedPublish() throws Exception {
        CompletableFuture<Void> first = send();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch confirmed = new CountDownLatch(1);
        CountDownLatch releasePublish = new CountDownLatch(1);
        channel.afterPublish =
                published -> {
                    channel.ack(1, false);
                    confirmed.countDown();
                    await(releasePublish);
                };
        try {
            Future<CompletableFuture<Void>> second = executor.submit(this::send);
            assertThat(confirmed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(first.isDone()).isFalse();
            producer.close();
            assertThat(first.join()).isNull();
            releasePublish.countDown();
            assertClosed(second.get(5, TimeUnit.SECONDS));
        } finally {
            releasePublish.countDown();
            shutdownExecutor(executor);
        }
    }

    @Test
    void closeHelpsAnInFlightRangeConfirmationWithoutOverwritingIt() throws Exception {
        CompletableFuture<Void> first = send();
        CompletableFuture<Void> second = send();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        first.whenComplete(
                (ignored, error) -> {
                    callbackStarted.countDown();
                    awaitUnchecked(releaseCallback);
                });
        try {
            Future<?> ack =
                    executor.submit(
                            () -> {
                                channel.ack(2, true);
                                return null;
                            });
            assertThat(callbackStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(second.isDone()).isFalse();
            producer.close();
            assertThat(second.join()).isNull();
            releaseCallback.countDown();
            ack.get(5, TimeUnit.SECONDS);
            assertThat(first.join()).isNull();
        } finally {
            releaseCallback.countDown();
            shutdownExecutor(executor);
        }
    }

    @Test
    void shutdownCallbackCanReenterSendWithoutWaitingForThePublishingThread() throws Exception {
        CompletableFuture<Void> first = send();
        ShutdownSignalException signal = shutdownSignal(channel);
        AtomicBoolean insidePublish = new AtomicBoolean();
        AtomicBoolean callbackDuringPublish = new AtomicBoolean();
        CompletableFuture<CompletableFuture<Void>> nested =
                first.handle(
                        (ignored, error) -> {
                            callbackDuringPublish.set(insidePublish.get());
                            return producer.sendAsync(Optional.empty(), new byte[0]);
                        });
        channel.afterPublish =
                published -> {
                    insidePublish.set(true);
                    try {
                        channel.shutdown(signal);
                    } finally {
                        insidePublish.set(false);
                    }
                };

        CompletableFuture<Void> second = send();
        assertFailedWith(first, signal);
        assertFailedWith(second, signal);
        assertFailedWith(nested.join(), signal);
        assertThat(callbackDuringPublish).isFalse();
    }

    @Test
    void reentrantCloseIsRejectedAndCanBeRetriedAfterPublish() throws Exception {
        CompletableFuture<Void> first = send();
        AtomicBoolean insidePublish = new AtomicBoolean();
        AtomicBoolean callbackDuringPublish = new AtomicBoolean();
        CountDownLatch publishExited = new CountDownLatch(1);
        first.whenComplete(
                (ignored, error) -> {
                    callbackDuringPublish.set(insidePublish.get());
                    awaitUnchecked(publishExited);
                });
        channel.afterPublish =
                published -> {
                    insidePublish.set(true);
                    try {
                        assertThatThrownBy(producer::close)
                                .isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining("reentrantly");
                        assertThat(first.isDone()).isFalse();
                        assertThat(channel.closeCalls).hasValue(0);
                    } finally {
                        insidePublish.set(false);
                        publishExited.countDown();
                    }
                };

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<Void> second = executor.submit(this::send).get(5, TimeUnit.SECONDS);
            assertThat(first.isDone()).isFalse();
            assertThat(second.isDone()).isFalse();
            producer.close();
            assertClosed(first);
            assertClosed(second);
            assertThat(callbackDuringPublish).isFalse();
            assertThat(channel.closeCalls).hasValue(1);
        } finally {
            publishExited.countDown();
            shutdownExecutor(executor);
        }
    }

    @Test
    void synchronousRecoveryCompletesPendingSendsAfterReleasingThePublishGate() throws Exception {
        FakeChannel recoveringChannel = new FakeChannel(true);
        RabbitMqBenchmarkProducer recoveringProducer =
                new RabbitMqBenchmarkProducer(recoveringChannel.proxy, "exchange", false);
        try {
            CompletableFuture<Void> first = recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            AtomicBoolean insidePublish = new AtomicBoolean();
            AtomicBoolean callbackDuringPublish = new AtomicBoolean();
            CompletableFuture<CompletableFuture<Void>> nested =
                    first.handle(
                            (ignored, error) -> {
                                callbackDuringPublish.set(insidePublish.get());
                                return recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
                            });
            recoveringChannel.afterPublish =
                    published -> {
                        if (published.sequence() == 2) {
                            insidePublish.set(true);
                            try {
                                recoveringChannel.recover();
                            } finally {
                                insidePublish.set(false);
                            }
                        }
                    };

            CompletableFuture<Void> second = recoveringProducer.sendAsync(Optional.empty(), new byte[0]);
            assertThatThrownBy(first::join)
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("RabbitMQ channel is recovering");
            assertThatThrownBy(second::join)
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("RabbitMQ channel is recovering");
            assertThat(callbackDuringPublish).isFalse();
            CompletableFuture<Void> third = nested.join();
            recoveringChannel.ack(3, false);
            assertThat(third.join()).isNull();
        } finally {
            recoveringProducer.close();
        }
    }

    @Test
    void racingConfirmationsCancellationShutdownAndCloseSettleEveryFutureOnce() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            for (int iteration = 0; iteration < 25; iteration++) {
                FakeChannel concurrentChannel = new FakeChannel();
                RabbitMqBenchmarkProducer concurrentProducer =
                        new RabbitMqBenchmarkProducer(concurrentChannel.proxy, "race", false);
                List<CompletableFuture<Void>> sends = new ArrayList<>();
                AtomicInteger completions = new AtomicInteger();
                for (int i = 0; i < 32; i++) {
                    CompletableFuture<Void> future =
                            concurrentProducer.sendAsync(Optional.empty(), new byte[] {(byte) i});
                    future.whenComplete((ignored, error) -> completions.incrementAndGet());
                    sends.add(future);
                }
                CountDownLatch start = new CountDownLatch(1);
                ShutdownSignalException signal = shutdownSignal(concurrentChannel);
                List<Callable<Void>> actions =
                        List.of(
                                () -> {
                                    start.await();
                                    concurrentChannel.ack(16, true);
                                    return null;
                                },
                                () -> {
                                    start.await();
                                    concurrentChannel.nack(32, true);
                                    return null;
                                },
                                () -> {
                                    start.await();
                                    concurrentChannel.ack(1, false);
                                    return null;
                                },
                                () -> {
                                    start.await();
                                    for (int i = 0; i < sends.size(); i += 3) {
                                        sends.get(i).cancel(false);
                                    }
                                    return null;
                                },
                                () -> {
                                    start.await();
                                    concurrentChannel.shutdown(signal);
                                    return null;
                                },
                                () -> {
                                    start.await();
                                    concurrentProducer.close();
                                    return null;
                                });
                List<Future<Void>> calls = new ArrayList<>();
                for (Callable<Void> action : actions) {
                    calls.add(executor.submit(action));
                }
                start.countDown();
                for (Future<Void> call : calls) {
                    call.get(5, TimeUnit.SECONDS);
                }
                assertThat(sends).allMatch(CompletableFuture::isDone);
                assertThat(completions).hasValue(sends.size());
                assertThat(concurrentChannel.closeCalls).hasValue(1);
            }
        } finally {
            shutdownExecutor(executor);
        }
    }

    @Test
    void concurrentSendsConfirmationsAndCloseDoNotLoseFutures() throws Exception {
        ExecutorService senders = Executors.newFixedThreadPool(12);
        ExecutorService controls = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch published = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        channel.afterPublish = ignored -> published.countDown();
        channel.beforePublish = Thread::yield;
        try {
            List<Future<CompletableFuture<Void>>> calls = new ArrayList<>();
            for (int i = 0; i < 80; i++) {
                calls.add(
                        senders.submit(
                                () -> {
                                    start.await();
                                    return send();
                                }));
            }
            Future<?> acknowledger =
                    controls.submit(
                            () -> {
                                start.await();
                                while (closed.getCount() != 0) {
                                    for (Published message : channel.published) {
                                        channel.ack(message.sequence(), false);
                                    }
                                    Thread.yield();
                                }
                                for (Published message : channel.published) {
                                    channel.ack(message.sequence(), false);
                                }
                                return null;
                            });
            Future<?> closer =
                    controls.submit(
                            () -> {
                                start.await();
                                published.await(5, TimeUnit.SECONDS);
                                try {
                                    producer.close();
                                } finally {
                                    closed.countDown();
                                }
                                return null;
                            });
            start.countDown();
            closer.get(5, TimeUnit.SECONDS);
            acknowledger.get(5, TimeUnit.SECONDS);
            List<CompletableFuture<Void>> sends = new ArrayList<>();
            for (Future<CompletableFuture<Void>> call : calls) {
                sends.add(call.get(5, TimeUnit.SECONDS));
            }
            assertThat(sends).allMatch(CompletableFuture::isDone);
            assertThat(channel.maxConcurrentPublishes).hasValue(1);
            assertThat(channel.closeCalls).hasValue(1);
        } finally {
            start.countDown();
            closed.countDown();
            shutdownExecutor(senders);
            shutdownExecutor(controls);
        }
    }

    private CompletableFuture<Void> send() {
        return producer.sendAsync(Optional.empty(), new byte[] {1});
    }

    private List<CompletableFuture<Void>> sendMany(int count) {
        List<CompletableFuture<Void>> sends = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            sends.add(send());
        }
        return sends;
    }

    private static AtomicInteger observe(CompletableFuture<Void> future) {
        AtomicInteger completions = new AtomicInteger();
        future.whenComplete((ignored, error) -> completions.incrementAndGet());
        return completions;
    }

    private static void assertNacked(CompletableFuture<Void> future) {
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Message was negatively acknowledged");
    }

    private static void assertClosed(CompletableFuture<Void> future) {
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("RabbitMQ producer is closed");
    }

    private static void assertFailedWith(CompletableFuture<Void> future, Throwable cause) {
        assertThatThrownBy(future::join).isInstanceOf(CompletionException.class).hasCause(cause);
    }

    private static ShutdownSignalException shutdownSignal(FakeChannel channel) {
        return new ShutdownSignalException(false, false, null, channel.proxy);
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for test latch", e);
        }
    }

    private static void awaitUnchecked(CountDownLatch latch) {
        try {
            await(latch);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void shutdownExecutor(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    private record Published(
            long sequence,
            String exchange,
            String routingKey,
            BasicProperties properties,
            byte[] payload) {}

    private record InterruptedSend(CompletableFuture<Void> future, boolean interruptRestored) {}

    @FunctionalInterface
    private interface PublishAction {
        void run() throws IOException;
    }

    @FunctionalInterface
    private interface PublishedAction {
        void accept(Published published) throws IOException;
    }

    private static final class BlockingConnectionFactory extends ConnectionFactory {
        private SocketFrameHandler createBlockingHandler(Socket socket) throws IOException {
            var factory = createFrameHandlerFactory();
            assertThat(factory).isInstanceOf(SocketFrameHandlerFactory.class);
            return (SocketFrameHandler) ((SocketFrameHandlerFactory) factory).create(socket);
        }
    }

    private static final class BlockingConnection extends AMQConnection {
        private final FrameHandler handler;

        private BlockingConnection(ConnectionParams params, FrameHandler handler) {
            super(params, handler);
            this.handler = handler;
        }

        @Override
        public void writeFrame(Frame frame) throws IOException {
            handler.writeFrame(frame);
        }
    }

    private static final class RecoverableTransportChannel extends AutorecoveringChannel {
        private RecoverableTransportChannel(RecoveryAwareChannelN delegate) {
            super(null, delegate);
        }

        @Override
        public void close() {
            // The loopback peer only captures frames; it is not a broker.
        }
    }

    private static final class FakeChannel implements InvocationHandler {
        private final Channel proxy;
        private final AtomicLong nextSequence = new AtomicLong(1);
        private final AtomicInteger sequenceLookups = new AtomicInteger();
        private final AtomicInteger publishAttempts = new AtomicInteger();
        private final AtomicInteger concurrentPublishes = new AtomicInteger();
        private final AtomicInteger maxConcurrentPublishes = new AtomicInteger();
        private final AtomicInteger confirmRegistrations = new AtomicInteger();
        private final AtomicInteger confirmRemoveAttempts = new AtomicInteger();
        private final AtomicInteger shutdownRemoveAttempts = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();
        private final AtomicReference<RuntimeException> nextSequenceFailure = new AtomicReference<>();
        private final List<Published> published = new CopyOnWriteArrayList<>();
        private volatile ConfirmListener confirmListener;
        private volatile ShutdownListener shutdownListener;
        private volatile RecoveryListener recoveryListener;
        private volatile ShutdownSignalException initialShutdown;
        private volatile boolean confirmRemoved;
        private volatile boolean shutdownRemoved;
        private volatile boolean recoveryRemoved;
        private volatile boolean recoverOnRegistration;
        private volatile Throwable removeConfirmFailure;
        private volatile Throwable removeShutdownFailure;
        private volatile Throwable closeFailure;
        private volatile Runnable beforeAddConfirm = () -> {};
        private volatile Runnable beforeSequence = () -> {};
        private volatile PublishAction beforePublish = () -> {};
        private volatile PublishedAction afterPublish = ignored -> {};

        private FakeChannel() {
            this(false);
        }

        private FakeChannel(boolean recoverable) {
            Class<?>[] interfaces =
                    recoverable
                            ? new Class<?>[] {Channel.class, Recoverable.class}
                            : new Class<?>[] {Channel.class};
            proxy = (Channel) Proxy.newProxyInstance(Channel.class.getClassLoader(), interfaces, this);
        }

        @Override
        public Object invoke(Object ignored, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "addConfirmListener":
                    beforeAddConfirm.run();
                    confirmRegistrations.incrementAndGet();
                    confirmListener = (ConfirmListener) args[0];
                    return null;
                case "removeConfirmListener":
                    confirmRemoveAttempts.incrementAndGet();
                    if (removeConfirmFailure != null) {
                        throw removeConfirmFailure;
                    }
                    confirmRemoved = args[0] == confirmListener;
                    return confirmRemoved;
                case "addShutdownListener":
                    shutdownListener = (ShutdownListener) args[0];
                    if (initialShutdown != null) {
                        shutdownListener.shutdownCompleted(initialShutdown);
                    }
                    return null;
                case "removeShutdownListener":
                    shutdownRemoveAttempts.incrementAndGet();
                    if (removeShutdownFailure != null) {
                        throw removeShutdownFailure;
                    }
                    shutdownRemoved = args[0] == shutdownListener;
                    return null;
                case "addRecoveryListener":
                    recoveryListener = (RecoveryListener) args[0];
                    if (recoverOnRegistration) {
                        recover();
                    }
                    return null;
                case "removeRecoveryListener":
                    recoveryRemoved = args[0] == recoveryListener;
                    return null;
                case "getNextPublishSeqNo":
                    sequenceLookups.incrementAndGet();
                    beforeSequence.run();
                    RuntimeException failure = nextSequenceFailure.getAndSet(null);
                    if (failure != null) {
                        throw failure;
                    }
                    return nextSequence.get();
                case "basicPublish":
                    publishAttempts.incrementAndGet();
                    int active = concurrentPublishes.incrementAndGet();
                    maxConcurrentPublishes.accumulateAndGet(active, Math::max);
                    try {
                        beforePublish.run();
                        Published message =
                                new Published(
                                        nextSequence.getAndIncrement(),
                                        (String) args[0],
                                        (String) args[1],
                                        (BasicProperties) args[2],
                                        (byte[]) args[3]);
                        published.add(message);
                        afterPublish.accept(message);
                    } finally {
                        concurrentPublishes.decrementAndGet();
                    }
                    return null;
                case "close":
                    closeCalls.incrementAndGet();
                    if (closeFailure != null) {
                        throw closeFailure;
                    }
                    return null;
                case "toString":
                    return "FakeChannel";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                default:
                    throw new UnsupportedOperationException(method.toString());
            }
        }

        private void ack(long sequence, boolean multiple) throws IOException {
            confirmListener.handleAck(sequence, multiple);
        }

        private void nack(long sequence, boolean multiple) throws IOException {
            confirmListener.handleNack(sequence, multiple);
        }

        private void shutdown(ShutdownSignalException signal) {
            shutdownListener.shutdownCompleted(signal);
        }

        private void recover() {
            recoveryListener.handleRecoveryStarted((Recoverable) proxy);
            recoveryListener.handleRecovery((Recoverable) proxy);
        }
    }
}
