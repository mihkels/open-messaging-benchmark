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
package io.openmessaging.benchmark.tool.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.openmessaging.benchmark.utils.distributor.KeyDistribution;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import java.util.Collections;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * A template that defines a set of workload definitions. This is much like the {@link
 * io.openmessaging.benchmark.Workload} entity, except that for many values that typically change in
 * a benchmark, one can specify a sequence of values.
 *
 * @param nameFormat the format string for generating workload names
 * @param topics list of topic counts to test
 * @param partitionsPerTopic list of partition counts per topic to test
 * @param messageSize list of message sizes in bytes to test
 * @param subscriptionsPerTopic list of subscription counts per topic to test
 * @param producersPerTopic list of producer counts per topic to test
 * @param consumerPerSubscription list of consumer counts per subscription to test
 * @param producerRate list of producer rates to test
 * @param keyDistributor the key distribution strategy to use
 * @param keyDistribution optional Zipfian parameters
 * @param payloadFile optional file path for custom payload data
 * @param useRandomizedPayloads whether to use randomized payloads
 * @param randomBytesRatio ratio of random bytes in the payload
 * @param randomizedPayloadPoolSize size of the randomized payload pool
 * @param consumerBacklogSizeGB consumer backlog size in gigabytes
 * @param testDurationMinutes duration of each test in minutes
 * @param warmupDurationMinutes warmup duration in minutes before each test
 */
public record WorkloadSetTemplate(
        String nameFormat,
        List<Integer> topics,
        List<Integer> partitionsPerTopic,
        List<Integer> messageSize,
        List<Integer> subscriptionsPerTopic,
        List<Integer> producersPerTopic,
        List<Integer> consumerPerSubscription,
        List<Integer> producerRate,
        KeyDistributorType keyDistributor,
        @Nullable KeyDistribution keyDistribution,
        @Nullable String payloadFile,
        boolean useRandomizedPayloads,
        double randomBytesRatio,
        int randomizedPayloadPoolSize,
        long consumerBacklogSizeGB,
        int testDurationMinutes,
        int warmupDurationMinutes) {

    public static final String DEFAULT_NAME_TEMPLATE =
            "${topics}-topics-${partitionsPerTopic}-partitions-${messageSize}b"
                    + "-${producersPerTopic}p-${consumerPerSubscription}c-${producerRate}";
    public static final int DEFAULT_TEST_DURATION_MINUTES = 5;
    public static final int DEFAULT_WARMUP_DURATION_MINUTES = 1;

    // Defensive copy constructor
    @SuppressWarnings("checkstyle:ParameterNumber")
    public WorkloadSetTemplate(
            String nameFormat,
            List<Integer> topics,
            List<Integer> partitionsPerTopic,
            List<Integer> messageSize,
            List<Integer> subscriptionsPerTopic,
            List<Integer> producersPerTopic,
            List<Integer> consumerPerSubscription,
            List<Integer> producerRate,
            KeyDistributorType keyDistributor,
            @Nullable KeyDistribution keyDistribution,
            @Nullable String payloadFile,
            boolean useRandomizedPayloads,
            double randomBytesRatio,
            int randomizedPayloadPoolSize,
            long consumerBacklogSizeGB,
            int testDurationMinutes,
            int warmupDurationMinutes) {
        this.nameFormat = nameFormat;
        this.topics = topics != null ? List.copyOf(topics) : List.of();
        this.partitionsPerTopic =
                partitionsPerTopic != null ? List.copyOf(partitionsPerTopic) : List.of();
        this.messageSize = messageSize != null ? List.copyOf(messageSize) : List.of();
        this.subscriptionsPerTopic =
                subscriptionsPerTopic != null ? List.copyOf(subscriptionsPerTopic) : List.of();
        this.producersPerTopic = producersPerTopic != null ? List.copyOf(producersPerTopic) : List.of();
        this.consumerPerSubscription =
                consumerPerSubscription != null ? List.copyOf(consumerPerSubscription) : List.of();
        this.producerRate = producerRate != null ? List.copyOf(producerRate) : List.of();
        this.keyDistributor = keyDistributor;
        this.keyDistribution = keyDistribution;
        this.payloadFile = payloadFile;
        this.useRandomizedPayloads = useRandomizedPayloads;
        this.randomBytesRatio = randomBytesRatio;
        this.randomizedPayloadPoolSize = randomizedPayloadPoolSize;
        this.consumerBacklogSizeGB = consumerBacklogSizeGB;
        this.testDurationMinutes = testDurationMinutes;
        this.warmupDurationMinutes = warmupDurationMinutes;
    }

    // Jackson creator: boxed scalars let fields missing from the template fall back to the defaults
    @JsonCreator
    @SuppressWarnings("checkstyle:ParameterNumber")
    static WorkloadSetTemplate fromJson(
            @JsonProperty("nameFormat") String nameFormat,
            @JsonProperty("topics") List<Integer> topics,
            @JsonProperty("partitionsPerTopic") List<Integer> partitionsPerTopic,
            @JsonProperty("messageSize") List<Integer> messageSize,
            @JsonProperty("subscriptionsPerTopic") List<Integer> subscriptionsPerTopic,
            @JsonProperty("producersPerTopic") List<Integer> producersPerTopic,
            @JsonProperty("consumerPerSubscription") List<Integer> consumerPerSubscription,
            @JsonProperty("producerRate") List<Integer> producerRate,
            @JsonProperty("keyDistributor") KeyDistributorType keyDistributor,
            @JsonProperty("keyDistribution") KeyDistribution keyDistribution,
            @JsonProperty("payloadFile") String payloadFile,
            @JsonProperty("useRandomizedPayloads") Boolean useRandomizedPayloads,
            @JsonProperty("randomBytesRatio") Double randomBytesRatio,
            @JsonProperty("randomizedPayloadPoolSize") Integer randomizedPayloadPoolSize,
            @JsonProperty("consumerBacklogSizeGB") Long consumerBacklogSizeGB,
            @JsonProperty("testDurationMinutes") Integer testDurationMinutes,
            @JsonProperty("warmupDurationMinutes") Integer warmupDurationMinutes) {
        return new WorkloadSetTemplate(
                nameFormat != null ? nameFormat : DEFAULT_NAME_TEMPLATE,
                topics,
                partitionsPerTopic,
                messageSize,
                subscriptionsPerTopic,
                producersPerTopic,
                consumerPerSubscription,
                producerRate,
                keyDistributor != null ? keyDistributor : KeyDistributorType.NO_KEY,
                keyDistribution,
                payloadFile,
                useRandomizedPayloads != null && useRandomizedPayloads,
                randomBytesRatio != null ? randomBytesRatio : 0.0,
                randomizedPayloadPoolSize != null ? randomizedPayloadPoolSize : 0,
                consumerBacklogSizeGB != null ? consumerBacklogSizeGB : 0,
                testDurationMinutes != null ? testDurationMinutes : DEFAULT_TEST_DURATION_MINUTES,
                warmupDurationMinutes != null ? warmupDurationMinutes : DEFAULT_WARMUP_DURATION_MINUTES);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public WorkloadSetTemplate() {
        this(
                DEFAULT_NAME_TEMPLATE,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                KeyDistributorType.NO_KEY,
                null,
                null,
                false,
                0.0,
                0,
                0,
                DEFAULT_TEST_DURATION_MINUTES,
                DEFAULT_WARMUP_DURATION_MINUTES);
    }

    // Defensive copy getters for mutable List fields
    @Override
    public List<Integer> topics() {
        return List.copyOf(topics);
    }

    @Override
    public List<Integer> partitionsPerTopic() {
        return List.copyOf(partitionsPerTopic);
    }

    @Override
    public List<Integer> messageSize() {
        return List.copyOf(messageSize);
    }

    @Override
    public List<Integer> subscriptionsPerTopic() {
        return List.copyOf(subscriptionsPerTopic);
    }

    @Override
    public List<Integer> producersPerTopic() {
        return List.copyOf(producersPerTopic);
    }

    @Override
    public List<Integer> consumerPerSubscription() {
        return List.copyOf(consumerPerSubscription);
    }

    @Override
    public List<Integer> producerRate() {
        return List.copyOf(producerRate);
    }
}
