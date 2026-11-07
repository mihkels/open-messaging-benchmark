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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.exceptions.MqttClientStateException;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient.Mqtt5SubscribeAndCallbackBuilder;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConfig;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectRestrictionsBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscription;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.mqtt5.client.MqttConfig;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MqttBenchmarkDriverOptionsTest {
    @Test
    void realClientUsesParsedServerUriAndSimpleAuth() {
        MqttConfig config = new MqttConfig();
        config.client.serverUri = "ssl://broker.example:8883";
        config.client.username = "user";
        config.client.password = "secret";
        Mqtt5ClientConfig built =
                new MqttBenchmarkDriver(config).createClient("id", () -> true).getConfig();
        InetSocketAddress address = built.getServerAddress();
        assertThat(address.getHostString()).isEqualTo("broker.example");
        assertThat(address.getPort()).isEqualTo(8883);
        assertThat(built.getSimpleAuth()).isPresent();
        assertThat(built.getSimpleAuth().get().getPassword())
                .get()
                .extracting(buffer -> StandardCharsets.UTF_8.decode(buffer).toString())
                .isEqualTo("secret");
        assertThat(new MqttBenchmarkDriver(config).getTopicNamePrefix()).isEqualTo("benchmark/test");
        assertThat(new MqttBenchmarkDriver(config).createTopic("t", 1)).isCompleted();
    }

    @Test
    void realClientWithoutPortOrPasswordUsesDefaultsAndNoAuth() {
        MqttConfig config = new MqttConfig();
        config.client.serverUri = "broker.example";
        config.client.username = "user";
        Mqtt5ClientConfig built =
                new MqttBenchmarkDriver(config).createClient("id", () -> false).getConfig();
        assertThat(built.getServerAddress().getPort()).isEqualTo(80);
        assertThat(built.getSimpleAuth()).isEmpty();

        config.client.username = null;
        assertThat(
                        new MqttBenchmarkDriver(config)
                                .createClient("id", () -> false)
                                .getConfig()
                                .getSimpleAuth())
                .isEmpty();
    }

    @Test
    void invalidServerUriFailsProducerCreation() {
        MqttConfig config = new MqttConfig();
        config.client.serverUri = ":";
        assertThatThrownBy(() -> new MqttBenchmarkDriver(config).createProducer("topic").join())
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid serverUri");
    }

    @Test
    void longServerUriSchemeIsParsedWithoutBacktracking() {
        MqttConfig config = new MqttConfig();
        config.client.serverUri = "s".repeat(20_000) + "://broker.example:1883";
        Mqtt5ClientConfig built =
                new MqttBenchmarkDriver(config).createClient("id", () -> true).getConfig();
        assertThat(built.getServerAddress().getHostString()).isEqualTo("broker.example");
        assertThat(built.getServerAddress().getPort()).isEqualTo(1883);
    }

    @Test
    @SuppressWarnings("unchecked")
    void persistentSessionKeepsConfiguredExpiry() {
        MqttConfig config = new MqttConfig();
        config.consumer.cleanSession = false;
        config.consumer.sessionExpiryInterval = 60;
        Mqtt5AsyncClient client = mock(Mqtt5AsyncClient.class);
        Mqtt5ConnectBuilder.Send<CompletableFuture<Mqtt5ConnAck>> connect =
                mock(Mqtt5ConnectBuilder.Send.class, RETURNS_SELF);
        Mqtt5ConnectRestrictionsBuilder.Nested<
                        Mqtt5ConnectBuilder.Send<CompletableFuture<Mqtt5ConnAck>>>
                restrictions = mock(Mqtt5ConnectRestrictionsBuilder.Nested.class, RETURNS_SELF);
        when(client.connectWith()).thenReturn(connect);
        doReturn(restrictions).when(connect).restrictions();
        doReturn(connect).when(restrictions).applyRestrictions();
        CompletableFuture<Mqtt5ConnAck> sent = new CompletableFuture<>();
        doReturn(sent).when(connect).send();

        assertThat(new MqttBenchmarkDriver(config).connectConsumer(client)).isSameAs(sent);
        verify(connect).cleanStart(false);
        verify(connect).sessionExpiryInterval(60);
    }

    @Test
    @SuppressWarnings("unchecked")
    void subscriptionCallbackReadsPublishTimestampAndAcknowledges() throws Exception {
        Mqtt5AsyncClient client = mock(Mqtt5AsyncClient.class);
        Mqtt5SubscribeAndCallbackBuilder.Start start =
                mock(Mqtt5SubscribeAndCallbackBuilder.Start.class);
        Mqtt5SubscribeAndCallbackBuilder.Complete complete =
                mock(Mqtt5SubscribeAndCallbackBuilder.Complete.class);
        Mqtt5SubscribeAndCallbackBuilder.Call.Ex call =
                mock(Mqtt5SubscribeAndCallbackBuilder.Call.Ex.class, RETURNS_SELF);
        when(client.subscribeWith()).thenReturn(start);
        when(start.addSubscriptions(anyList())).thenReturn(complete);
        when(complete.callback(any())).thenReturn(call);
        ConsumerCallback callback = mock(ConsumerCallback.class);

        new MqttBenchmarkDriver(new MqttConfig()).subscribeConsumer(client, List.of(), callback);
        ArgumentCaptor<Consumer<Mqtt5Publish>> handler = ArgumentCaptor.forClass(Consumer.class);
        verify(complete).callback(handler.capture());

        Mqtt5Publish stamped =
                publish(
                        new byte[] {1},
                        Mqtt5UserProperty.of("other", "1"),
                        Mqtt5UserProperty.of(
                                MqttBenchmarkDriver.USER_PROPERTY_KEY_PUBLISH_TIMESTAMP.toString(), "42"));
        handler.getValue().accept(stamped);
        verify(callback).messageReceived(new byte[] {1}, 42L);
        verify(stamped).acknowledge();

        long before = System.currentTimeMillis();
        Mqtt5Publish garbled =
                publish(
                        new byte[] {2},
                        Mqtt5UserProperty.of(
                                MqttBenchmarkDriver.USER_PROPERTY_KEY_PUBLISH_TIMESTAMP.toString(), "x"));
        handler.getValue().accept(garbled);
        Mqtt5Publish unstamped = publish(new byte[] {3});
        handler.getValue().accept(unstamped);
        ArgumentCaptor<Long> time = ArgumentCaptor.forClass(Long.class);
        verify(callback)
                .messageReceived(org.mockito.ArgumentMatchers.eq(new byte[] {2}), time.capture());
        verify(callback)
                .messageReceived(org.mockito.ArgumentMatchers.eq(new byte[] {3}), time.capture());
        assertThat(time.getAllValues()).allMatch(t -> t >= before);
        verify(unstamped).acknowledge();
    }

    @Test
    void missingConnAckAndMissingOrEmptySubAckFailCreation() {
        StubDriver missingConnAck = new StubDriver();
        CompletableFuture<BenchmarkConsumer> first =
                missingConnAck.createConsumer("topic", "g", mock(ConsumerCallback.class));
        missingConnAck.connection.complete(null);
        assertThatThrownBy(first::join).hasMessageContaining("ConnAck-ReasonCode: missing");

        StubDriver missingSubAck = new StubDriver();
        CompletableFuture<BenchmarkConsumer> second =
                missingSubAck.createConsumer(" , $share/own/topic ,", "g", mock(ConsumerCallback.class));
        missingSubAck.connection.complete(success());
        assertThat(missingSubAck.subscriptions)
                .extracting(s -> s.getTopicFilter().toString())
                .containsExactly("$share/own/topic");
        missingSubAck.subscription.complete(null);
        assertThatThrownBy(second::join).hasMessageContaining("Invalid SUBACK");

        StubDriver nullCodes = new StubDriver();
        CompletableFuture<BenchmarkConsumer> third =
                nullCodes.createConsumer("topic", "g", mock(ConsumerCallback.class));
        nullCodes.connection.complete(success());
        nullCodes.subscription.complete(mock(Mqtt5SubAck.class));
        assertThatThrownBy(third::join).hasMessageContaining("Invalid SUBACK");
    }

    @Test
    void topicWithoutFiltersFailsBeforeConnecting() {
        StubDriver noTopics =
                new StubDriver() {
                    @Override
                    Mqtt5AsyncClient createClient(String clientId, BooleanSupplier closed) {
                        throw new AssertionError("no client should be created without topic filters");
                    }
                };
        CompletableFuture<BenchmarkConsumer> creation =
                noTopics.createConsumer(" , ", "g", mock(ConsumerCallback.class));

        assertThatThrownBy(creation::join)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No MQTT topic filters in topic ' , '");
        assertThat(noTopics.subscriptions).isNull();
    }

    @Test
    void unknownQosFallsBackToAtLeastOnce() {
        assertThat(MqttBenchmarkDriver.intToQoS(7)).isEqualTo(MqttQos.AT_LEAST_ONCE);
        assertThat(MqttBenchmarkDriver.intToQoS(0)).isEqualTo(MqttQos.AT_MOST_ONCE);
    }

    @Test
    void closeClientUnwrapsDisconnectFailures() throws Exception {
        MqttBenchmarkDriver.closeClient(null);

        RuntimeException runtime = new RuntimeException("boom");
        assertThatThrownBy(() ->
                        MqttBenchmarkDriver.closeClient(failingClient(runtime, MqttClientState.CONNECTED)))
                .isSameAs(runtime);

        MqttClientStateException state = mock(MqttClientStateException.class);
        assertThatThrownBy(
                        () -> MqttBenchmarkDriver.closeClient(failingClient(state, MqttClientState.CONNECTED)))
                .isSameAs(state);

        AssertionError error = new AssertionError("fatal");
        assertThatThrownBy(
                        () -> MqttBenchmarkDriver.closeClient(failingClient(error, MqttClientState.CONNECTED)))
                .isSameAs(error);

        Throwable odd = new Throwable("odd");
        assertThatThrownBy(
                        () -> MqttBenchmarkDriver.closeClient(failingClient(odd, MqttClientState.CONNECTED)))
                .isInstanceOf(ExecutionException.class)
                .hasCause(odd);
    }

    @Test
    void closeOfTheDriverIsANoOp() {
        assertThatCode(() -> new MqttBenchmarkDriver().close()).doesNotThrowAnyException();
    }

    private static Mqtt5AsyncClient failingClient(Throwable failure, MqttClientState state) {
        Mqtt5AsyncClient client = mock(Mqtt5AsyncClient.class);
        when(client.getConfig()).thenReturn(mock(Mqtt5ClientConfig.class));
        when(client.getState()).thenReturn(state);
        when(client.disconnect()).thenReturn(CompletableFuture.failedFuture(failure));
        return client;
    }

    private static Mqtt5Publish publish(byte[] payload, Mqtt5UserProperty... properties) {
        Mqtt5Publish publish = mock(Mqtt5Publish.class);
        when(publish.getPayloadAsBytes()).thenReturn(payload);
        when(publish.getUserProperties()).thenReturn(Mqtt5UserProperties.of(properties));
        return publish;
    }

    private static Mqtt5ConnAck success() {
        Mqtt5ConnAck ack = mock(Mqtt5ConnAck.class);
        when(ack.getReasonCode()).thenReturn(Mqtt5ConnAckReasonCode.SUCCESS);
        return ack;
    }

    private static class StubDriver extends MqttBenchmarkDriver {
        final Mqtt5AsyncClient client = mock(Mqtt5AsyncClient.class);
        final CompletableFuture<Mqtt5ConnAck> connection = new CompletableFuture<>();
        final CompletableFuture<Mqtt5SubAck> subscription = new CompletableFuture<>();
        List<Mqtt5Subscription> subscriptions;

        StubDriver() {
            super(new MqttConfig());
            when(client.getConfig()).thenReturn(mock(Mqtt5ClientConfig.class));
            when(client.disconnect()).thenReturn(CompletableFuture.completedFuture(null));
        }

        @Override
        Mqtt5AsyncClient createClient(String clientId, BooleanSupplier closed) {
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
            return subscription;
        }
    }
}
