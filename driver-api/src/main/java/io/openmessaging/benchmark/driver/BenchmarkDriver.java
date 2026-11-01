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
package io.openmessaging.benchmark.driver;

import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Driver SPI for creating topics, producers, and consumers.
 *
 * <p>Creation methods return non-null futures. A successful producer or consumer creation completes
 * with a non-null, usable handle; an operational failure completes exceptionally with its cause.
 *
 * <p>A successful single-resource creation hands its handle to its caller. A batch owns the handles
 * returned by its individual creations until the batch future completes successfully and hands them
 * to its caller, normally the worker. The worker closes handed-off producer and consumer handles;
 * the driver closes its shared clients, connections, and other shared resources. On creation
 * failure or cancellation, the operation must close every successfully created handle that was not
 * handed off exactly once, including late completions, and cancel pending work where possible. One
 * failed close must not prevent the remaining handles from being closed.
 */
public interface BenchmarkDriver extends AutoCloseable {
    /**
     * Driver implementation can use this method to initialize the client libraries, with the provided
     * configuration file.
     *
     * <p>The format of the configuration file is specific to the driver implementation.
     *
     * @param configurationFile the configuration file
     * @param statsLogger host-owned registry for the driver's metrics
     * @throws IOException if configuration file cannot be read
     * @throws InterruptedException if initialization is interrupted
     */
    void initialize(Path configurationFile, MeterRegistry statsLogger)
            throws IOException, InterruptedException;

    /**
     * Get a driver specific prefix to be used in creating multiple topic names.
     *
     * @return the topic name prefix
     */
    String getTopicNamePrefix();

    /**
     * Create a new topic with a given number of partitions.
     *
     * @param topic the topic name
     * @param partitions the number of partitions
     * @return a non-null future that completes when the topic is created, or exceptionally if
     *     creation fails
     */
    CompletableFuture<Void> createTopic(String topic, int partitions);

    /**
     * Create a list of new topics with the given number of partitions.
     *
     * @param topicInfos the list of topics to create
     * @return a non-null future that completes when all topics are created, or exceptionally if
     *     creation fails
     */
    default CompletableFuture<Void> createTopics(List<TopicInfo> topicInfos) {
        return CompletableFuture.allOf(
                topicInfos.stream()
                        .map(topicInfo -> createTopic(topicInfo.topic(), topicInfo.partitions()))
                        .toArray(CompletableFuture[]::new));
    }

    /**
     * Create a producer for a given topic.
     *
     * @param topic the topic name
     * @return a non-null future that completes with a non-null, usable producer, or exceptionally if
     *     creation fails
     */
    CompletableFuture<BenchmarkProducer> createProducer(String topic);

    /**
     * Create producers for given topics.
     *
     * <p>On failure or cancellation, the default implementation closes every untransferred handle,
     * including handles whose individual creation completes later.
     *
     * @param producers the list of producers to create
     * @return a non-null future that completes with a non-null list of non-null producer handles, or
     *     exceptionally if creation fails
     */
    default CompletableFuture<List<BenchmarkProducer>> createProducers(List<ProducerInfo> producers) {
        return ResourceBatch.create(producers, producer -> createProducer(producer.topic()));
    }

    /**
     * Create a benchmark consumer relative to one particular topic and subscription.
     *
     * <p>The driver invokes {@code consumerCallback} for each message received. The future must not
     * complete successfully until the subscription is ready to receive messages published after
     * completion.
     *
     * @param topic the topic name
     * @param subscriptionName the subscription name
     * @param consumerCallback the callback to invoke when messages are received
     * @return a non-null future that completes with a non-null, ready consumer, or exceptionally if
     *     creation or subscription fails
     */
    CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback);

    /**
     * Create consumers for given topics.
     *
     * <p>On failure or cancellation, the default implementation closes every untransferred handle,
     * including handles whose individual creation completes later.
     *
     * @param consumers the list of consumers to create
     * @return a non-null future that completes with a non-null list of non-null, ready consumer
     *     handles, or exceptionally if creation fails
     */
    default CompletableFuture<List<BenchmarkConsumer>> createConsumers(List<ConsumerInfo> consumers) {
        return ResourceBatch.create(
                consumers,
                consumer ->
                        createConsumer(
                                consumer.topic(), consumer.subscriptionName(), consumer.consumerCallback()));
    }

    /**
     * Topic information record containing topic name and partition count.
     *
     * @param topic the topic name
     * @param partitions the number of partitions
     */
    record TopicInfo(String topic, int partitions) {}

    /**
     * Producer information record containing producer ID and topic.
     *
     * @param id the producer ID
     * @param topic the topic name
     */
    record ProducerInfo(int id, String topic) {}

    /**
     * Consumer information record containing consumer details.
     *
     * @param id the consumer ID
     * @param topic the topic name
     * @param subscriptionName the subscription name
     * @param consumerCallback the callback to invoke when messages are received
     */
    record ConsumerInfo(
            int id, String topic, String subscriptionName, ConsumerCallback consumerCallback) {}
}
