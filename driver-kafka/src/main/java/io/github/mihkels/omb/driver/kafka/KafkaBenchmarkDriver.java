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
package io.github.mihkels.omb.driver.kafka;

import io.github.mihkels.omb.driver.BenchmarkConsumer;
import io.github.mihkels.omb.driver.BenchmarkDriver;
import io.github.mihkels.omb.driver.BenchmarkProducer;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.DriverConfigs;
import io.github.mihkels.omb.driver.Failures;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;

public class KafkaBenchmarkDriver implements BenchmarkDriver {
    private static final String ZONE_ID_CONFIG = "zone.id";
    private static final String ZONE_ID_TEMPLATE = "{zone.id}";
    private static final String KAFKA_CLIENT_ID = "client.id";
    private Config config;

    private Properties topicProperties;
    private Properties producerProperties;
    private Properties consumerProperties;

    private @Nullable AdminClient admin;

    @Override
    public void initialize(Path configurationFile, MeterRegistry statsLogger) throws IOException {
        config = DriverConfigs.read(configurationFile, Config.class);

        Properties commonProperties = new Properties();
        commonProperties.load(new StringReader(config.commonConfig()));

        if (commonProperties.containsKey(KAFKA_CLIENT_ID)) {
            commonProperties.put(
                    KAFKA_CLIENT_ID,
                    applyZoneId(
                            commonProperties.getProperty(KAFKA_CLIENT_ID), System.getProperty(ZONE_ID_CONFIG)));
        }

        producerProperties = new Properties();
        producerProperties.putAll(commonProperties);
        producerProperties.load(new StringReader(config.producerConfig()));
        producerProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        consumerProperties = new Properties();
        consumerProperties.putAll(commonProperties);
        consumerProperties.load(new StringReader(config.consumerConfig()));
        consumerProperties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        topicProperties = new Properties();
        topicProperties.load(new StringReader(config.topicConfig()));

        admin = AdminClient.create(commonProperties);
    }

    @Override
    public boolean supportsHeaders() {
        return true;
    }

    @Override
    public boolean supportsPartitionTelemetry() {
        return true;
    }

    @Override
    public String getTopicNamePrefix() {
        return "test-topic";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        return createTopics(List.of(new TopicInfo(topic, partitions)));
    }

    @Override
    public CompletableFuture<Void> createTopics(List<TopicInfo> topicInfos) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Map<String, String> topicConfigs = new HashMap<>((Map) topicProperties);
        AdminClient admin = this.admin;
        if (admin == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Kafka benchmark driver is not initialized or already closed"));
        }
        KafkaTopicCreator topicCreator =
                new KafkaTopicCreator(admin, topicConfigs, config.replicationFactor());
        return topicCreator.create(topicInfos);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        KafkaProducer<String, byte[]> kafkaProducer = null;
        try {
            kafkaProducer = new KafkaProducer<>(producerProperties);
            return CompletableFuture.completedFuture(new KafkaBenchmarkProducer(kafkaProducer, topic));
        } catch (Throwable t) {
            Failures.closeAfterFailure(t, kafkaProducer);
            return CompletableFuture.failedFuture(t);
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        Properties properties = new Properties();
        properties.putAll(consumerProperties);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, subscriptionName);
        KafkaConsumer<String, byte[]> consumer = null;
        try {
            consumer = new KafkaConsumer<>(properties);
            return KafkaBenchmarkConsumer.create(
                    consumer, topic, consumerProperties, consumerCallback, 100L);
        } catch (Throwable t) {
            Failures.closeAfterFailure(t, consumer);
            return CompletableFuture.failedFuture(t);
        }
    }

    @Override
    public synchronized void close() {
        if (admin != null) {
            AdminClient toClose = admin;
            admin = null;
            toClose.close();
        }
    }

    private static String applyZoneId(String clientId, String zoneId) {
        if (zoneId == null) {
            return clientId;
        }
        return clientId.replace(ZONE_ID_TEMPLATE, zoneId);
    }
}
