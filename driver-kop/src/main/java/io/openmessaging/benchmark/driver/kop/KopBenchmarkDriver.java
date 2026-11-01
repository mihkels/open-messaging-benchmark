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
package io.openmessaging.benchmark.driver.kop;

import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreator;
import io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkConsumer;
import io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkProducer;
import io.openmessaging.benchmark.driver.kop.config.ClientType;
import io.openmessaging.benchmark.driver.kop.config.Config;
import io.openmessaging.benchmark.driver.kop.config.PulsarConfig;
import io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkConsumer;
import io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkProducer;
import io.streamnative.pulsar.handlers.kop.KafkaPayloadProcessor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.yaml.YAMLFactory;

public class KopBenchmarkDriver implements BenchmarkDriver {

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private static final ObjectReader reader =
            mapper
                    .readerFor(Config.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .with(DeserializationFeature.USE_LONG_FOR_INTS);

    private Config config;
    private AdminClient admin;
    private Properties producerProperties;
    private Properties consumerProperties;
    private PulsarClient client = null;
    private ProducerBuilder<byte[]> producerBuilder = null;
    private ConsumerBuilder<ByteBuffer> consumerBuilder = null;

    public static Config loadConfig(Path file) throws IOException {
        return reader.readValue(Files.newInputStream(file));
    }

    @Override
    public void initialize(Path configurationFile, MeterRegistry statsLogger) throws IOException {
        config = loadConfig(configurationFile);
        final Properties commonProperties = config.getKafkaProperties();
        admin = AdminClient.create(commonProperties);

        producerProperties = new Properties();
        producerProperties.putAll(commonProperties);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        consumerProperties = new Properties();
        consumerProperties.putAll(commonProperties);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        final PulsarConfig pulsarConfig = config.pulsarConfig();
        if (config.producerType().equals(ClientType.PULSAR)) {
            producerBuilder =
                    getPulsarClient(pulsarConfig.serviceUrl())
                            .newProducer()
                            .enableBatching(pulsarConfig.batchingEnabled())
                            .blockIfQueueFull(pulsarConfig.blockIfQueueFull())
                            .batchingMaxPublishDelay(
                                    pulsarConfig.batchingMaxPublishDelayMs(), TimeUnit.MILLISECONDS)
                            .batchingMaxBytes(pulsarConfig.batchingMaxBytes())
                            .maxPendingMessages(pulsarConfig.pendingQueueSize())
                            .maxPendingMessagesAcrossPartitions(
                                    pulsarConfig.maxPendingMessagesAcrossPartitions());
        }
        if (config.consumerType().equals(ClientType.PULSAR)) {
            consumerBuilder =
                    getPulsarClient(pulsarConfig.serviceUrl())
                            .newConsumer(Schema.BYTEBUFFER)
                            .subscriptionType(SubscriptionType.Failover)
                            .receiverQueueSize(pulsarConfig.receiverQueueSize())
                            .maxTotalReceiverQueueSizeAcrossPartitions(
                                    pulsarConfig.maxTotalReceiverQueueSizeAcrossPartitions());
        }
    }

    @Override
    public String getTopicNamePrefix() {
        return "test-topic";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        // replicationFactor is meaningless in KoP
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) 1L);
        final CompletableFuture<Void> future = new CompletableFuture<>();
        admin
                .createTopics(Collections.singletonList(newTopic))
                .all()
                .whenComplete(
                        (result, throwable) -> {
                            if (throwable == null) {
                                future.complete(result);
                            } else {
                                future.completeExceptionally(throwable);
                            }
                        });
        return future;
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        if (config.producerType().equals(ClientType.KAFKA)) {
            KafkaProducer<String, byte[]> producer = null;
            try {
                producer = new KafkaProducer<>(producerProperties);
                return CompletableFuture.completedFuture(new KafkaBenchmarkProducer(producer, topic));
            } catch (RuntimeException error) {
                closeAfterFailure(producer, error);
                return CompletableFuture.failedFuture(error);
            }
        } else if (config.producerType().equals(ClientType.PULSAR)) {
            try {
                return ResourceCreator.compose(
                        CompletableFuture.completedFuture(List.of(topic)),
                        name -> producerBuilder.clone().topic(name).createAsync(),
                        producers -> new PulsarBenchmarkProducer(producers.get(0)));
            } catch (RuntimeException error) {
                return CompletableFuture.failedFuture(error);
            }
        } else {
            throw new IllegalArgumentException("producerType " + config.producerType() + " is invalid");
        }
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        if (config.consumerType().equals(ClientType.KAFKA)) {
            final Properties properties = new Properties();
            properties.putAll(consumerProperties);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, subscriptionName);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaConsumer<String, byte[]> kafkaConsumer = null;
            try {
                kafkaConsumer = new KafkaConsumer<>(properties);
                return KafkaBenchmarkConsumer.create(
                        kafkaConsumer, topic, properties, consumerCallback, config.pollTimeoutMs());
            } catch (RuntimeException error) {
                closeAfterFailure(kafkaConsumer, error);
                return CompletableFuture.failedFuture(error);
            }
        } else if (config.consumerType().equals(ClientType.PULSAR)) {
            try {
                return ResourceCreator.compose(
                        client.getPartitionsForTopic(topic, true),
                        partition ->
                                createInternalPulsarConsumer(partition, subscriptionName, consumerCallback),
                        PulsarBenchmarkConsumer::new);
            } catch (RuntimeException error) {
                return CompletableFuture.failedFuture(error);
            }
        } else {
            throw new IllegalArgumentException("consumerType " + config.consumerType() + " is invalid");
        }
    }

    @Override
    public synchronized void close() throws Exception {
        AdminClient ownedAdmin = admin;
        PulsarClient ownedClient = client;
        admin = null;
        client = null;

        Exception failure = closeResource(ownedAdmin, null);
        failure = closeResource(ownedClient, failure);
        if (failure != null) {
            throw failure;
        }
    }

    private static void closeAfterFailure(AutoCloseable resource, Exception failure) {
        closeResource(resource, failure);
    }

    private static Exception closeResource(AutoCloseable resource, Exception failure) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception closeFailure) {
                if (closeFailure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                if (failure == null) {
                    return closeFailure;
                }
                if (failure != closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        return failure;
    }

    private PulsarClient getPulsarClient(String serviceUrl) throws PulsarClientException {
        if (client == null) {
            client = PulsarClient.builder().serviceUrl(serviceUrl).build();
        }
        return client;
    }

    private CompletableFuture<Consumer<ByteBuffer>> createInternalPulsarConsumer(
            String topic, String subscriptionName, ConsumerCallback callback) {
        return consumerBuilder
                .clone()
                .topic(topic)
                .subscriptionName(subscriptionName)
                .messagePayloadProcessor(
                        new KafkaPayloadProcessor()) // support consuming Kafka format messages
                .poolMessages(true)
                .messageListener(
                        (c, msg) -> {
                            try {
                                callback.messageReceived(msg.getValue(), msg.getPublishTime());
                                c.acknowledgeAsync(msg);
                            } finally {
                                msg.release();
                            }
                        })
                .subscribeAsync();
    }
}
