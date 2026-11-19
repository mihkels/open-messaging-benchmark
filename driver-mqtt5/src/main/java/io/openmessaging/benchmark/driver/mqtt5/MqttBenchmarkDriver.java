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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;
import com.hivemq.client.mqtt.MqttClientConfig;
import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.MqttClientTransportConfig;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.exceptions.MqttClientStateException;
import com.hivemq.client.mqtt.lifecycle.MqttClientAutoReconnect;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscription;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.DriverConfigs;
import io.openmessaging.benchmark.driver.ResourceNames;
import io.openmessaging.benchmark.driver.mqtt5.client.MqttConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttBenchmarkDriver implements BenchmarkDriver {

    private static final Logger log = LoggerFactory.getLogger(MqttBenchmarkDriver.class);
    public static final MqttUtf8String USER_PROPERTY_KEY_PUBLISH_TIMESTAMP =
            MqttUtf8String.of("benchmark-publish-timestamp");

    private MqttConfig config;

    public MqttBenchmarkDriver() {}

    MqttBenchmarkDriver(MqttConfig config) {
        this.config = config;
    }

    @Override
    public void initialize(Path configurationFile, MeterRegistry statsLogger)
            throws IOException, InterruptedException {
        this.config = readConfig(configurationFile);
        intToQoS(config.client.qos);
    }

    @Override
    public String getTopicNamePrefix() {
        return config.client.topicPrefix + "/test";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        // MQTT topics are created on the fly when messages are published or subscribed to.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        AtomicBoolean closed = new AtomicBoolean();
        AtomicReference<MqttCreationFuture<?>> creation = new AtomicReference<>();
        MqttCreationFuture<BenchmarkProducer> future;
        Mqtt5AsyncClient client;
        try {
            client = createClient(clientId("pub"), stopReconnect(closed, creation));
            future =
                    new MqttCreationFuture<>(
                            new MqttBenchmarkProducer(topic, config.client.qos, client, closed));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        creation.set(future);
        try {
            connect(future, client.connect(), future::succeed);
        } catch (Exception failure) {
            future.fail(failure);
            future.finished();
        }

        return future;
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        AtomicBoolean closed = new AtomicBoolean();
        AtomicReference<MqttCreationFuture<?>> creation = new AtomicReference<>();
        List<Mqtt5Subscription> subscriptions = new ArrayList<>();
        Mqtt5AsyncClient client;
        MqttBenchmarkConsumer consumer;
        MqttCreationFuture<BenchmarkConsumer> future;
        try {
            MqttQos qos = intToQoS(config.client.qos);
            Splitter.on(",")
                    .split(topic)
                    .forEach(topicFilter -> {
                        topicFilter = topicFilter.trim();
                        if (!topicFilter.isEmpty()) {
                            subscriptions.add(
                                    Mqtt5Subscription.builder()
                                            .topicFilter(toSharedSubscription(topicFilter, subscriptionName))
                                            .qos(qos)
                                            .build());
                        }
                    });
            if (subscriptions.isEmpty()) {
                throw new IllegalArgumentException("No MQTT topic filters in topic '" + topic + "'");
            }
            client = createClient(clientId("sub"), stopReconnect(closed, creation));
            consumer = new MqttBenchmarkConsumer(client, closed);
            future = new MqttCreationFuture<>(consumer);
        } catch (Exception failure) {
            return CompletableFuture.failedFuture(failure);
        }
        creation.set(future);
        try {
            connect(
                    future,
                    connectConsumer(client),
                    () -> {
                        if (future.isCancelled()) {
                            return;
                        }
                        ConsumerCallback guardedCallback = consumer.guardedCallback(consumerCallback);
                        CompletableFuture<Mqtt5SubAck> subscription =
                                Objects.requireNonNull(
                                        subscribeConsumer(client, subscriptions, guardedCallback),
                                        "MQTT subscription future");
                        awaitSubscription(future, subscriptions, subscription);
                    });
        } catch (Exception failure) {
            future.fail(failure);
            future.finished();
        }

        return future;
    }

    // Stops a client's reconnects once its producer or consumer is closed, and on a disconnect while
    // its creation is still in progress. The creation future is set before the client connects.
    private static BooleanSupplier stopReconnect(
            AtomicBoolean closed, AtomicReference<MqttCreationFuture<?>> creation) {
        return () -> closed.get() || creation.get().stopReconnectOnDisconnect();
    }

    // Marks the future connected and runs onConnected once the CONNACK succeeds, otherwise fails
    // it; the connection attempt is finished either way.
    private static void connect(
            MqttCreationFuture<?> future,
            CompletableFuture<Mqtt5ConnAck> connection,
            Runnable onConnected) {
        Objects.requireNonNull(connection, "MQTT connection future")
                .whenComplete((connAck, ex) -> {
                    try {
                        Throwable failure = connectionFailure(connAck, ex);
                        if (failure != null) {
                            future.fail(failure);
                            return;
                        }
                        future.connected();
                        onConnected.run();
                    } catch (Throwable failureDuringConnection) {
                        future.fail(failureDuringConnection);
                    } finally {
                        future.finished();
                    }
                });
    }

    private static void awaitSubscription(
            MqttCreationFuture<BenchmarkConsumer> future,
            List<Mqtt5Subscription> subscriptions,
            CompletableFuture<Mqtt5SubAck> subscription) {
        future.retain();
        try {
            subscription.whenComplete(
                    (subAck, error) -> completeSubscription(future, subscriptions, subAck, error));
        } catch (Throwable registrationFailure) {
            future.finished();
            throw registrationFailure;
        }
    }

    private static void completeSubscription(
            MqttCreationFuture<BenchmarkConsumer> future,
            List<Mqtt5Subscription> subscriptions,
            @Nullable Mqtt5SubAck subAck,
            @Nullable Throwable subscribeFailure) {
        try {
            if (future.isCancelled()) {
                return;
            }
            Throwable rejection = subscriptionFailure(subAck, subscribeFailure, subscriptions);
            if (rejection != null) {
                future.fail(rejection);
            } else {
                future.succeed();
            }
        } catch (Throwable failureDuringSubscription) {
            future.fail(failureDuringSubscription);
        } finally {
            future.finished();
        }
    }

    Mqtt5AsyncClient createClient(String clientId, BooleanSupplier closed) {
        return getClientBuilder(clientId, closed).buildAsync();
    }

    CompletableFuture<Mqtt5ConnAck> connectConsumer(Mqtt5AsyncClient client) {
        return client
                .connectWith()
                .cleanStart(config.consumer.cleanSession)
                .sessionExpiryInterval(
                        Boolean.TRUE.equals(config.consumer.cleanSession)
                                ? 0
                                : config.consumer.sessionExpiryInterval)
                .restrictions()
                .receiveMaximum(config.consumer.receiveMaximum)
                .applyRestrictions()
                .send();
    }

    CompletableFuture<Mqtt5SubAck> subscribeConsumer(
            Mqtt5AsyncClient client,
            List<Mqtt5Subscription> subscriptions,
            ConsumerCallback consumerCallback) {
        return client
                .subscribeWith()
                .addSubscriptions(subscriptions)
                .callback(message -> {
                    long publishTime = extractPublishTimestamp(message.getUserProperties());
                    consumerCallback.messageReceived(message.getPayloadAsBytes(), publishTime);
                    message.acknowledge();
                })
                .manualAcknowledgement(true)
                .send();
    }

    private static @Nullable Throwable connectionFailure(
            @Nullable Mqtt5ConnAck connAck, @Nullable Throwable ex) {
        if (ex != null) {
            return ex;
        }
        if (connAck == null || connAck.getReasonCode() != Mqtt5ConnAckReasonCode.SUCCESS) {
            return new IllegalStateException(
                    "ConnAck-ReasonCode: " + (connAck == null ? "missing" : connAck.getReasonCode()));
        }
        return null;
    }

    private static @Nullable Throwable subscriptionFailure(
            @Nullable Mqtt5SubAck subAck, @Nullable Throwable ex, List<Mqtt5Subscription> subscriptions) {
        if (ex != null) {
            return ex;
        }
        if (subAck == null || subAck.getReasonCodes().size() != subscriptions.size()) {
            return new IllegalStateException("Invalid SUBACK for subscriptions: " + subscriptions);
        }
        for (int i = 0; i < subscriptions.size(); i++) {
            Mqtt5SubAckReasonCode reason = subAck.getReasonCodes().get(i);
            if (reason != Mqtt5SubAckReasonCode.GRANTED_QOS_0
                    && reason != Mqtt5SubAckReasonCode.GRANTED_QOS_1
                    && reason != Mqtt5SubAckReasonCode.GRANTED_QOS_2) {
                return new IllegalStateException(
                        "Subscription rejected for " + subscriptions.get(i).getTopicFilter() + ": " + reason);
            }
        }
        return null;
    }

    private Mqtt5ClientBuilder getClientBuilder(String clientId, BooleanSupplier closed) {
        Mqtt5ClientBuilder clientBuilder =
                Mqtt5Client.builder()
                        .identifier(clientId)
                        .transportConfig(transportConfig(this.config.client.serverUri));

        // Simple Auth with username and password
        if (isNotEmpty(this.config.client.username) && isNotEmpty(this.config.client.password)) {
            clientBuilder =
                    clientBuilder
                            .simpleAuth()
                            .username(this.config.client.username)
                            .password(this.config.client.password.getBytes(StandardCharsets.UTF_8))
                            .applySimpleAuth();
        }

        // Auto reconnect with initial delay 100 mills, max delay 10 seconds
        clientBuilder =
                clientBuilder.automaticReconnect(
                        MqttClientAutoReconnect.builder()
                                .initialDelay(100, TimeUnit.MILLISECONDS)
                                .maxDelay(10, TimeUnit.SECONDS)
                                .build());

        // Listen to connection events
        clientBuilder =
                clientBuilder
                        .addConnectedListener(context ->
                                log.info(
                                        "Client[{}] connected to MQTT broker {}",
                                        extractClientId(context.getClientConfig()),
                                        context.getClientConfig().getServerAddress()))
                        .addDisconnectedListener(context -> {
                            String clientId1 = extractClientId(context.getClientConfig());
                            log.warn(
                                    "Client[{}] lost connection to MQTT broker {}, by {}",
                                    clientId1,
                                    context.getClientConfig().getServerAddress(),
                                    context.getSource().name(),
                                    context.getCause());
                            if (closed.getAsBoolean()) {
                                log.warn(
                                        "Client[{}] stops reconnecting to MQTT broker since the client"
                                                + " wasn't created successfully or has been stopped already",
                                        clientId1);
                                context.getReconnector().reconnect(false);
                            }
                        });

        return clientBuilder;
    }

    @Override
    public void close() throws Exception {
        // Nothing to close
    }

    private static MqttConfig readConfig(Path configurationFile) throws IOException {
        return DriverConfigs.read(configurationFile, MqttConfig.class);
    }

    /**
     * Builds the transport for a server URI. {@code ssl://}, {@code tls://} and {@code mqtts://}
     * connect over TLS with the JVM's default trust store and host name verification, so the username
     * and password never cross the network in clear text.
     *
     * @param serverUri the server URI from the driver config
     * @return the transport configuration
     */
    static MqttClientTransportConfig transportConfig(String serverUri) {
        HostAndPort hostAndPort = parseServerUri(serverUri);
        var transport =
                MqttClientTransportConfig.builder()
                        .serverHost(hostAndPort.getHost())
                        .serverPort(hostAndPort.getPort())
                        .socketConnectTimeout(3, TimeUnit.SECONDS)
                        .mqttConnectTimeout(3, TimeUnit.SECONDS);
        if (isTls(serverUri)) {
            transport = transport.sslWithDefaultConfig();
        }
        return transport.build();
    }

    private static boolean isTls(String serverUri) {
        int separator = serverUri.indexOf("://");
        String scheme = separator >= 0 ? serverUri.substring(0, separator) : "";
        return Set.of("ssl", "tls", "mqtts").contains(scheme.toLowerCase(Locale.ROOT));
    }

    static HostAndPort parseServerUri(String serverUri) {
        int separator = serverUri.indexOf("://");
        boolean tls = isTls(serverUri);
        HostAndPort address;
        try {
            address =
                    HostAndPort.fromString(serverUri.substring(separator >= 0 ? separator + 3 : 0))
                            .withDefaultPort(tls ? 8883 : 1883);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid serverUri: " + serverUri, e);
        }
        if (address.getHost().isEmpty()) {
            throw new IllegalArgumentException("Invalid serverUri: " + serverUri);
        }
        return address;
    }

    private static String clientId(String role) {
        return Joiner.on("_")
                .join("benchmark", role, ResourceNames.randomSuffix(), System.currentTimeMillis());
    }

    private static String extractClientId(MqttClientConfig clientConfig) {
        return clientConfig.getClientIdentifier().map(Object::toString).orElse("");
    }

    private static String toSharedSubscription(String topicFilter, String sharedGroup) {
        if (topicFilter == null || topicFilter.isEmpty()) {
            return topicFilter;
        }

        if (topicFilter.startsWith("$share")) {
            return topicFilter;
        }

        return Joiner.on('/').join("$share", sharedGroup, topicFilter);
    }

    private static boolean isNotEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    // A message without a readable publish timestamp is counted but gives no latency sample.
    private static long extractPublishTimestamp(Mqtt5UserProperties userProperties) {
        List<? extends Mqtt5UserProperty> propertyList = userProperties.asList();
        for (Mqtt5UserProperty property : propertyList) {
            if (USER_PROPERTY_KEY_PUBLISH_TIMESTAMP.equals(property.getName())) {
                try {
                    return Long.parseLong(property.getValue().toString());
                } catch (NumberFormatException _) {
                    return ConsumerCallback.NO_PUBLISH_TIMESTAMP;
                }
            }
        }
        return ConsumerCallback.NO_PUBLISH_TIMESTAMP;
    }

    public static MqttQos intToQoS(int val) {
        MqttQos qos = MqttQos.fromCode(val);
        if (qos == null) {
            throw new IllegalArgumentException("Invalid MQTT qos " + val + "; use 0, 1 or 2");
        }
        return qos;
    }

    @SuppressWarnings("java:S112") // Unwraps and rethrows the disconnect future's own failure
    public static void closeClient(Mqtt5AsyncClient client) throws Exception {
        if (client != null) {
            String clientId = String.valueOf(client.getConfig().getClientIdentifier());
            log.info("Client[{}] disconnecting...", clientId);
            try {
                Objects.requireNonNull(client.disconnect(), "MQTT disconnect future")
                        .get(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof MqttClientStateException
                        && client.getState() == MqttClientState.DISCONNECTED) {
                    log.debug("Client[{}] is already disconnected", clientId);
                    return;
                }
                if (e.getCause() instanceof Exception failure) {
                    throw failure;
                }
                if (e.getCause() instanceof Error failure) {
                    throw failure;
                }
                throw e;
            }
        }
    }
}
