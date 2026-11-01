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
package io.openmessaging.benchmark.driver.mqtt5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.exceptions.MqttClientStateException;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient.Mqtt5SubscribeAndCallbackBuilder;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConfig;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectRestrictionsBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscription;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver.ConsumerInfo;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreationCompletion;
import io.openmessaging.benchmark.driver.mqtt5.client.MqttConfig;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class MqttBenchmarkDriverTest {
    @Test
    @SuppressWarnings("unchecked")
    void realClientBuilderPathSubscribesAfterConnectWithManualAcknowledgements() throws Exception {
        Mqtt5AsyncClient client = mock(Mqtt5AsyncClient.class);
        CompletableFuture<Mqtt5ConnAck> connection = new CompletableFuture<>();
        CompletableFuture<Mqtt5SubAck> subscription = new CompletableFuture<>();
        Mqtt5ConnectBuilder.Send<CompletableFuture<Mqtt5ConnAck>> connectBuilder =
                mock(Mqtt5ConnectBuilder.Send.class, RETURNS_SELF);
        Mqtt5ConnectRestrictionsBuilder.Nested<
                        Mqtt5ConnectBuilder.Send<CompletableFuture<Mqtt5ConnAck>>>
                restrictions = mock(Mqtt5ConnectRestrictionsBuilder.Nested.class, RETURNS_SELF);
        when(client.connectWith()).thenReturn(connectBuilder);
        doReturn(restrictions).when(connectBuilder).restrictions();
        doReturn(connectBuilder).when(restrictions).applyRestrictions();
        doReturn(connection).when(connectBuilder).send();

        Mqtt5SubscribeAndCallbackBuilder.Start start =
                mock(Mqtt5SubscribeAndCallbackBuilder.Start.class);
        Mqtt5SubscribeAndCallbackBuilder.Complete complete =
                mock(Mqtt5SubscribeAndCallbackBuilder.Complete.class);
        Mqtt5SubscribeAndCallbackBuilder.Call.Ex callback =
                mock(Mqtt5SubscribeAndCallbackBuilder.Call.Ex.class, RETURNS_SELF);
        when(client.subscribeWith()).thenReturn(start);
        when(start.addSubscriptions(anyList())).thenReturn(complete);
        when(complete.callback(any())).thenReturn(callback);
        when(callback.send()).thenReturn(subscription);
        when(client.getConfig()).thenReturn(mock(Mqtt5ClientConfig.class));
        when(client.disconnect()).thenReturn(CompletableFuture.completedFuture(null));
        clearInvocations(client);
        MqttBenchmarkDriver driver =
                new MqttBenchmarkDriver(new MqttConfig()) {
                    @Override
                    Mqtt5AsyncClient createClient(String clientId, Supplier<Boolean> closed) {
                        return client;
                    }
                };
        CompletableFuture<BenchmarkConsumer> future =
                driver.createConsumer("topic", "group", mock(ConsumerCallback.class));
        verify(client, never()).subscribeWith();
        connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        assertThat(future.isDone()).isFalse();
        verify(client).subscribeWith();
        verify(callback).manualAcknowledgement(true);
        verify(connectBuilder).cleanStart(true);
        verify(connectBuilder).sessionExpiryInterval(0);
        verify(restrictions).receiveMaximum(256);
        subscription.complete(subAck(Mqtt5SubAckReasonCode.GRANTED_QOS_1));
        future.get(2, TimeUnit.SECONDS).close();
    }

    @Test
    void consumerCompletesOnlyAfterEverySubscriptionIsAcknowledged() throws Exception {
        TestDriver driver = new TestDriver();
        CompletableFuture<BenchmarkConsumer> future =
                driver.createConsumer("first, second", "group", mock(ConsumerCallback.class));
        assertThat(future.isDone()).isFalse();
        assertThat(driver.subscriptions).isNull();

        driver.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        assertThat(future.isDone()).isFalse();
        assertThat(driver.subscriptions)
                .extracting(subscription -> subscription.getTopicFilter().toString())
                .containsExactly("$share/group/first", "$share/group/second");
        driver.subscription.complete(
                subAck(Mqtt5SubAckReasonCode.GRANTED_QOS_1, Mqtt5SubAckReasonCode.GRANTED_QOS_0));

        BenchmarkConsumer consumer = future.get(2, TimeUnit.SECONDS);
        verify(driver.client, never()).disconnect();
        consumer.close();
        consumer.close();
        verify(driver.client, times(1)).disconnect();
        assertThat(driver.closedAtDisconnect).isTrue();
    }

    @Test
    void connectionFailureAndRejectionClosePartialClients() {
        RuntimeException failure = new RuntimeException("connection failed");
        TestDriver failed = new TestDriver();
        CompletableFuture<BenchmarkConsumer> failedFuture =
                failed.createConsumer("topic", "group", mock(ConsumerCallback.class));
        failed.connection.completeExceptionally(failure);
        assertThatThrownBy(failedFuture::join).hasCause(failure);
        assertThat(failed.subscriptions).isNull();
        verify(failed.client).disconnect();
        assertThat(failed.closedAtDisconnect).isTrue();

        TestDriver rejected = new TestDriver();
        CompletableFuture<BenchmarkConsumer> rejectedFuture =
                rejected.createConsumer("topic", "group", mock(ConsumerCallback.class));
        rejected.connection.complete(connAck(Mqtt5ConnAckReasonCode.NOT_AUTHORIZED));
        assertThatThrownBy(rejectedFuture::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("NOT_AUTHORIZED");
        verify(rejected.client).disconnect();
    }

    @Test
    void subscriptionFailuresAndRejectedOrMalformedSubacksClosePartialClients() {
        RuntimeException failure = new RuntimeException("subscribe failed");
        TestDriver failed = new TestDriver();
        CompletableFuture<BenchmarkConsumer> failedFuture =
                failed.createConsumer("topic", "group", mock(ConsumerCallback.class));
        failed.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        failed.subscription.completeExceptionally(failure);
        assertThatThrownBy(failedFuture::join).hasCause(failure);
        verify(failed.client).disconnect();

        TestDriver rejected = new TestDriver();
        CompletableFuture<BenchmarkConsumer> rejectedFuture =
                rejected.createConsumer("topic,other", "group", mock(ConsumerCallback.class));
        rejected.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        rejected.subscription.complete(
                subAck(Mqtt5SubAckReasonCode.GRANTED_QOS_1, Mqtt5SubAckReasonCode.NOT_AUTHORIZED));
        assertThatThrownBy(rejectedFuture::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("NOT_AUTHORIZED");
        verify(rejected.client).disconnect();

        TestDriver malformed = new TestDriver();
        CompletableFuture<BenchmarkConsumer> malformedFuture =
                malformed.createConsumer("one,two", "group", mock(ConsumerCallback.class));
        malformed.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        malformed.subscription.complete(subAck(Mqtt5SubAckReasonCode.GRANTED_QOS_1));
        assertThatThrownBy(malformedFuture::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SUBACK");
        verify(malformed.client).disconnect();
    }

    @Test
    void synchronousSubscriptionFailureIsNotLost() {
        TestDriver driver = new TestDriver();
        RuntimeException failure = new RuntimeException("subscribe invocation failed");
        driver.subscribeStartFailure = failure;
        CompletableFuture<BenchmarkConsumer> future =
                driver.createConsumer("topic", "group", mock(ConsumerCallback.class));
        driver.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        assertThatThrownBy(future::join).hasCause(failure);
        verify(driver.client).disconnect();
    }

    @Test
    void subscriptionCallbackRegistrationFailureDoesNotLeakQuiescence() throws Exception {
        TestDriver driver = new TestDriver();
        RuntimeException failure = new RuntimeException("callback registration failed");
        driver.subscription =
                new CompletableFuture<>() {
                    @Override
                    public CompletableFuture<Mqtt5SubAck> whenComplete(
                            BiConsumer<? super Mqtt5SubAck, ? super Throwable> action) {
                        throw failure;
                    }
                };
        CompletableFuture<BenchmarkConsumer> future =
                driver.createConsumer("topic", "group", mock(ConsumerCallback.class));

        driver.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));

        assertThatThrownBy(future::join).hasCause(failure);
        awaitCleanup(future);
        verify(driver.client).disconnect();
    }

    @Test
    void cancelingBeforeOrAfterConnectionClosesTheClient() throws Exception {
        TestDriver connecting = new TestDriver();
        CompletableFuture<BenchmarkConsumer> beforeConnection =
                connecting.createConsumer("topic", "group", mock(ConsumerCallback.class));
        assertThat(beforeConnection.cancel(false)).isTrue();
        connecting.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        assertThat(connecting.subscriptions).isNull();
        awaitCleanup(beforeConnection);
        verify(connecting.client, times(1)).disconnect();

        TestDriver subscribing = new TestDriver();
        CompletableFuture<BenchmarkConsumer> afterConnection =
                subscribing.createConsumer("topic", "group", mock(ConsumerCallback.class));
        subscribing.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        assertThat(afterConnection.cancel(false)).isTrue();
        subscribing.subscription.complete(subAck(Mqtt5SubAckReasonCode.GRANTED_QOS_1));
        assertThat(afterConnection.isCancelled()).isTrue();
        awaitCleanup(afterConnection);
        verify(subscribing.client, times(1)).disconnect();
    }

    @Test
    void failedProducerConnectionAlsoClosesThePartialClient() {
        TestDriver driver = new TestDriver();
        when(driver.client.connect()).thenReturn(driver.connection);
        CompletableFuture<BenchmarkProducer> future = driver.createProducer("topic");
        RuntimeException failure = new RuntimeException("connect failed");
        driver.connection.completeExceptionally(failure);
        assertThatThrownBy(future::join).hasCause(failure);
        verify(driver.client).disconnect();
        assertThat(driver.closedAtDisconnect).isTrue();
    }

    @Test
    void rejectedRealConnectionStopsReconnectAndQuiescesCleanly() throws Exception {
        try (ServerSocket broker = new ServerSocket(0, 5, InetAddress.getByName("127.0.0.1"))) {
            broker.setSoTimeout(1500);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            CompletableFuture<BenchmarkProducer> future = null;
            try {
                Future<Integer> connections =
                        executor.submit(
                                () -> {
                                    try (Socket first = broker.accept()) {
                                        acknowledgeConnection(first, 0x87);
                                    }
                                    try (Socket second = broker.accept()) {
                                        return 2;
                                    } catch (SocketTimeoutException expected) {
                                        return 1;
                                    }
                                });
                MqttConfig config = new MqttConfig();
                config.client.serverUri = "tcp://127.0.0.1:" + broker.getLocalPort();
                future = new MqttBenchmarkDriver(config).createProducer("topic");

                CompletableFuture<BenchmarkProducer> creation = future;
                Throwable failure = catchThrowable(() -> creation.get(5, TimeUnit.SECONDS));
                assertThat(failure).hasCauseInstanceOf(Mqtt5ConnAckException.class);
                assertThat(failure.getCause())
                        .isInstanceOfSatisfying(
                                Mqtt5ConnAckException.class,
                                rejection ->
                                        assertThat(rejection.getMqttMessage().getReasonCode())
                                                .isEqualTo(Mqtt5ConnAckReasonCode.NOT_AUTHORIZED));
                awaitCleanup(future);
                assertThat(connections.get(3, TimeUnit.SECONDS)).isEqualTo(1);
            } finally {
                if (future != null) {
                    future.cancel(false);
                }
                executor.shutdownNow();
            }
        }
    }

    @Test
    void canceledRealConnectionClosesAfterLateConnackWithoutReconnecting() throws Exception {
        try (ServerSocket broker = new ServerSocket(0, 5, InetAddress.getByName("127.0.0.1"))) {
            broker.setSoTimeout(1500);
            CountDownLatch accepted = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            CompletableFuture<BenchmarkProducer> future = null;
            try {
                Future<Integer> connections =
                        executor.submit(
                                () -> {
                                    try (Socket first = broker.accept()) {
                                        accepted.countDown();
                                        if (!release.await(3, TimeUnit.SECONDS)) {
                                            throw new IOException("Timed out waiting to acknowledge CONNECT");
                                        }
                                        acknowledgeConnection(first, 0);
                                        assertThat(first.getInputStream().read()).isEqualTo(0xe0);
                                    }
                                    try (Socket second = broker.accept()) {
                                        return 2;
                                    } catch (SocketTimeoutException expected) {
                                        return 1;
                                    }
                                });
                MqttConfig config = new MqttConfig();
                config.client.serverUri = "tcp://127.0.0.1:" + broker.getLocalPort();
                future = new MqttBenchmarkDriver(config).createProducer("topic");
                assertThat(accepted.await(3, TimeUnit.SECONDS)).isTrue();

                assertThat(future.cancel(false)).isTrue();
                release.countDown();
                awaitCleanup(future);
                assertThat(connections.get(3, TimeUnit.SECONDS)).isEqualTo(1);
            } finally {
                if (future != null) {
                    future.cancel(false);
                }
                release.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void createdRealClientStillReconnectsAfterConnectionLoss() throws Exception {
        try (ServerSocket broker = new ServerSocket(0, 5, InetAddress.getByName("127.0.0.1"))) {
            broker.setSoTimeout(5000);
            CountDownLatch dropConnection = new CountDownLatch(1);
            CountDownLatch reconnected = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<?> connections =
                        executor.submit(
                                () -> {
                                    try (Socket first = broker.accept()) {
                                        acknowledgeConnection(first, 0);
                                        if (!dropConnection.await(3, TimeUnit.SECONDS)) {
                                            throw new IOException("Timed out waiting to drop connection");
                                        }
                                    }
                                    try (Socket second = broker.accept()) {
                                        acknowledgeConnection(second, 0);
                                        reconnected.countDown();
                                        assertThat(second.getInputStream().read()).isEqualTo(0xe0);
                                    }
                                    return null;
                                });
                MqttConfig config = new MqttConfig();
                config.client.serverUri = "tcp://127.0.0.1:" + broker.getLocalPort();
                AtomicReference<Mqtt5AsyncClient> client = new AtomicReference<>();
                MqttBenchmarkDriver driver =
                        new MqttBenchmarkDriver(config) {
                            @Override
                            Mqtt5AsyncClient createClient(String clientId, Supplier<Boolean> closed) {
                                Mqtt5AsyncClient result = super.createClient(clientId, closed);
                                client.set(result);
                                return result;
                            }
                        };
                CompletableFuture<BenchmarkProducer> creation = driver.createProducer("topic");
                try {
                    try (BenchmarkProducer producer = creation.get(5, TimeUnit.SECONDS)) {
                        dropConnection.countDown();
                        assertThat(reconnected.await(5, TimeUnit.SECONDS)).isTrue();
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                        while (client.get().getState() != MqttClientState.CONNECTED
                                && System.nanoTime() < deadline) {
                            Thread.sleep(10);
                        }
                        assertThat(client.get().getState()).isEqualTo(MqttClientState.CONNECTED);
                    }
                } finally {
                    creation.cancel(false);
                }
                connections.get(3, TimeUnit.SECONDS);
            } finally {
                dropConnection.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void producerConfigurationFailureIsAnExceptionalFuture() {
        MqttBenchmarkDriver driver = new MqttBenchmarkDriver(null);
        assertThatThrownBy(() -> driver.createProducer("topic").join())
                .hasCauseInstanceOf(NullPointerException.class);
    }

    @Test
    void canceledProducerClosesAConnectionThatCompletesLate() throws Exception {
        TestDriver driver = new TestDriver();
        when(driver.client.connect()).thenReturn(driver.connection);
        CompletableFuture<BenchmarkProducer> future = driver.createProducer("topic");
        assertThat(future.cancel(false)).isTrue();
        driver.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        awaitCleanup(future);
        verify(driver.client, times(1)).disconnect();
        assertThat(driver.closedAtDisconnect).isTrue();
    }

    @Test
    void cancellationWaitsForOutstandingSubscriptionAndDisconnect() throws Exception {
        TestDriver driver = new TestDriver();
        CompletableFuture<Void> disconnect = new CompletableFuture<>();
        CountDownLatch disconnectStarted = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            disconnectStarted.countDown();
                            return disconnect;
                        })
                .when(driver.client)
                .disconnect();
        CompletableFuture<BenchmarkConsumer> future =
                driver.createConsumer("topic", "group", mock(ConsumerCallback.class));
        driver.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));

        assertThat(future.cancel(false)).isTrue();
        assertThat(disconnectStarted.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(((ResourceCreationCompletion) future).quiescence()).isNotDone();
        driver.subscription.complete(subAck(Mqtt5SubAckReasonCode.GRANTED_QOS_1));
        assertThat(((ResourceCreationCompletion) future).quiescence()).isNotDone();
        disconnect.complete(null);

        awaitCleanup(future);
        verify(driver.client, times(1)).disconnect();
    }

    @Test
    void canceledConsumerBatchAbandonsAndWaitsForLateConnection() throws Exception {
        TestDriver driver = new TestDriver();
        CompletableFuture<List<BenchmarkConsumer>> batch =
                driver.createConsumers(
                        List.of(new ConsumerInfo(0, "topic", "group", mock(ConsumerCallback.class))));

        assertThat(batch.cancel(false)).isTrue();
        assertThat(((ResourceCreationCompletion) batch).quiescence()).isNotDone();
        driver.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        awaitCleanup(batch);

        assertThat(driver.subscriptions).isNull();
        verify(driver.client, times(1)).disconnect();
    }

    @Test
    void canceledConsumerBatchReportsAsynchronousCleanupFailure() throws Exception {
        TestDriver driver = new TestDriver();
        IOException cleanupFailure = new IOException("disconnect failed");
        doReturn(CompletableFuture.failedFuture(cleanupFailure)).when(driver.client).disconnect();
        CompletableFuture<List<BenchmarkConsumer>> batch =
                driver.createConsumers(
                        List.of(new ConsumerInfo(0, "topic", "group", mock(ConsumerCallback.class))));
        driver.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));

        assertThat(batch.cancel(false)).isTrue();
        driver.subscription.complete(subAck(Mqtt5SubAckReasonCode.GRANTED_QOS_1));
        assertThatThrownBy(() -> awaitCleanup(batch)).hasCause(cleanupFailure);
        verify(driver.client, times(1)).disconnect();
    }

    @Test
    void canceledCreationCleansUpAfterConnectionFailure() throws Exception {
        TestDriver driver = new TestDriver();
        CompletableFuture<BenchmarkConsumer> future =
                driver.createConsumer("topic", "group", mock(ConsumerCallback.class));

        assertThat(future.cancel(false)).isTrue();
        driver.connection.completeExceptionally(new IOException("connection failed"));
        awaitCleanup(future);

        verify(driver.client).disconnect();
    }

    @Test
    void failedCreationWaitsForDisconnectWithoutBlockingTheClientCallback() throws Exception {
        TestDriver driver = new TestDriver();
        CompletableFuture<Void> disconnect = new CompletableFuture<>();
        doReturn(disconnect).when(driver.client).disconnect();
        CompletableFuture<BenchmarkConsumer> result =
                driver.createConsumer("topic", "group", mock(ConsumerCallback.class));
        RuntimeException failure = new RuntimeException("connection failed");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> clientCallback =
                    executor.submit(() -> driver.connection.completeExceptionally(failure));
            clientCallback.get(2, TimeUnit.SECONDS);
            assertThat(result.isDone()).isFalse();
            disconnect.complete(null);
            assertThatThrownBy(result::join).hasCause(failure);
            awaitCleanup(result);
        } finally {
            disconnect.complete(null);
            executor.shutdownNow();
        }
    }

    @Test
    void failedDisconnectIsReportedAndRetainedOnCreationFailure() throws Exception {
        TestDriver driver = new TestDriver();
        IOException cleanupFailure = new IOException("disconnect failed");
        doReturn(CompletableFuture.failedFuture(cleanupFailure)).when(driver.client).disconnect();
        CompletableFuture<BenchmarkConsumer> result =
                driver.createConsumer("topic", "group", mock(ConsumerCallback.class));
        RuntimeException failure = new RuntimeException("connection failed");
        driver.connection.completeExceptionally(failure);

        assertThatThrownBy(result::join).hasCause(failure);
        assertThat(failure.getSuppressed()).contains(cleanupFailure);
        assertThatThrownBy(() -> awaitCleanup(result)).hasCause(cleanupFailure);
    }

    @Test
    void consumerCloseWaitsForDisconnectAndReportsItsFailure() throws Exception {
        Mqtt5AsyncClient client = mock();
        when(client.getConfig()).thenReturn(mock(Mqtt5ClientConfig.class));
        CompletableFuture<Void> disconnect = new CompletableFuture<>();
        CountDownLatch disconnectStarted = new CountDownLatch(1);
        when(client.disconnect())
                .thenAnswer(
                        invocation -> {
                            disconnectStarted.countDown();
                            return disconnect;
                        });
        MqttBenchmarkConsumer consumer = new MqttBenchmarkConsumer();
        consumer.setClient(client);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IOException failure = new IOException("disconnect failed");
        try {
            Future<?> closing =
                    executor.submit(
                            () -> {
                                consumer.close();
                                return null;
                            });
            assertThat(disconnectStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> closing.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            disconnect.completeExceptionally(failure);
            assertThatThrownBy(() -> closing.get(2, TimeUnit.SECONDS)).hasCause(failure);
            assertThatThrownBy(consumer::close).isSameAs(failure);
            verify(client, times(1)).disconnect();
        } finally {
            disconnect.complete(null);
            executor.shutdownNow();
        }
    }

    @Test
    void alreadyDisconnectedIsSafeButPendingReconnectIsNot() throws Exception {
        Mqtt5AsyncClient client = mock();
        when(client.getConfig()).thenReturn(mock(Mqtt5ClientConfig.class));
        MqttClientStateException failure = new MqttClientStateException("not connected");
        when(client.disconnect()).thenReturn(CompletableFuture.failedFuture(failure));
        when(client.getState())
                .thenReturn(MqttClientState.DISCONNECTED, MqttClientState.DISCONNECTED_RECONNECT);

        MqttBenchmarkDriver.closeClient(client);
        assertThatThrownBy(() -> MqttBenchmarkDriver.closeClient(client)).isSameAs(failure);
    }

    @Test
    void consumerCallbackCannotCloseItsOwnClient() throws Exception {
        TestDriver driver = new TestDriver();
        ConsumerCallback callback = mock();
        AtomicReference<BenchmarkConsumer> handle = new AtomicReference<>();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            try {
                                handle.get().close();
                            } catch (Exception failure) {
                                closeFailure.set(failure);
                            }
                            return null;
                        })
                .when(callback)
                .messageReceived(any(byte[].class), anyLong());
        CompletableFuture<BenchmarkConsumer> future = driver.createConsumer("topic", "group", callback);
        driver.connection.complete(connAck(Mqtt5ConnAckReasonCode.SUCCESS));
        driver.subscription.complete(subAck(Mqtt5SubAckReasonCode.GRANTED_QOS_1));
        BenchmarkConsumer consumer = future.get(2, TimeUnit.SECONDS);
        handle.set(consumer);

        driver.callback.messageReceived(new byte[] {1}, 1L);

        assertThat(closeFailure.get()).isInstanceOf(IllegalStateException.class);
        verify(driver.client, never()).disconnect();
        consumer.close();
        verify(driver.client).disconnect();
    }

    private static void awaitCleanup(CompletableFuture<?> creation) throws Exception {
        ((ResourceCreationCompletion) creation).quiescence().get(2, TimeUnit.SECONDS);
    }

    private static void acknowledgeConnection(Socket socket, int reason) throws IOException {
        socket.setSoTimeout(5000);
        var input = socket.getInputStream();
        if (input.read() != 0x10) {
            throw new IOException("Expected an MQTT CONNECT packet");
        }
        int remaining = 0;
        int multiplier = 1;
        int digit;
        do {
            digit = input.read();
            if (digit < 0 || multiplier > 128 * 128 * 128) {
                throw new IOException("Invalid MQTT CONNECT length");
            }
            remaining += (digit & 0x7f) * multiplier;
            multiplier *= 128;
        } while ((digit & 0x80) != 0);
        if (remaining > 65536 || input.readNBytes(remaining).length != remaining) {
            throw new IOException("Incomplete MQTT CONNECT packet");
        }
        socket.getOutputStream().write(new byte[] {0x20, 0x03, 0x00, (byte) reason, 0x00});
        socket.getOutputStream().flush();
    }

    private static Mqtt5ConnAck connAck(Mqtt5ConnAckReasonCode code) {
        Mqtt5ConnAck ack = mock(Mqtt5ConnAck.class);
        when(ack.getReasonCode()).thenReturn(code);
        return ack;
    }

    private static Mqtt5SubAck subAck(Mqtt5SubAckReasonCode... reasons) {
        Mqtt5SubAck ack = mock(Mqtt5SubAck.class);
        when(ack.getReasonCodes()).thenReturn(List.of(reasons));
        return ack;
    }

    private static class TestDriver extends MqttBenchmarkDriver {
        final Mqtt5AsyncClient client = mock(Mqtt5AsyncClient.class);
        final CompletableFuture<Mqtt5ConnAck> connection = new CompletableFuture<>();
        CompletableFuture<Mqtt5SubAck> subscription = new CompletableFuture<>();
        List<Mqtt5Subscription> subscriptions;
        ConsumerCallback callback;
        Supplier<Boolean> closed;
        boolean closedAtDisconnect;
        RuntimeException subscribeStartFailure;

        TestDriver() {
            super(new MqttConfig());
            Mqtt5ClientConfig config = mock(Mqtt5ClientConfig.class);
            when(client.getConfig()).thenReturn(config);
            when(client.disconnect())
                    .thenAnswer(
                            invocation -> {
                                closedAtDisconnect = closed.get();
                                return CompletableFuture.completedFuture(null);
                            });
        }

        @Override
        Mqtt5AsyncClient createClient(String clientId, Supplier<Boolean> closed) {
            this.closed = closed;
            return client;
        }

        @Override
        CompletableFuture<Mqtt5ConnAck> connectConsumer(Mqtt5AsyncClient client) {
            return connection;
        }

        @Override
        CompletableFuture<Mqtt5SubAck> subscribeConsumer(
                Mqtt5AsyncClient client,
                List<Mqtt5Subscription> subscriptions,
                ConsumerCallback consumerCallback) {
            this.subscriptions = subscriptions;
            this.callback = consumerCallback;
            if (subscribeStartFailure != null) {
                throw subscribeStartFailure;
            }
            return subscription;
        }
    }
}
