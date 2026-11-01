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

import static org.assertj.core.api.Assertions.assertThat;

import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class WorkloadGeneratorTest {

    @Test
    void allWorkloadFieldsAreExplicitlyAccountedFor() {
        Set<String> templateFields =
                Arrays.stream(WorkloadSetTemplate.class.getRecordComponents())
                        .map(RecordComponent::getName)
                        .filter(name -> !name.equals("nameFormat"))
                        .collect(Collectors.toSet());
        Set<String> workloadFields =
                Arrays.stream(Workload.class.getFields())
                        .filter(field -> !Modifier.isStatic(field.getModifiers()))
                        .map(Field::getName)
                        .collect(Collectors.toSet());
        Set<String> supportedFields = SupportedWorkloadFields.nameVariables(new Workload()).keySet();

        assertThat(supportedFields).containsExactlyInAnyOrderElementsOf(templateFields);
        assertThat(workloadFields)
                .containsExactlyInAnyOrderElementsOf(
                        Stream.concat(
                                        supportedFields.stream(),
                                        Stream.of("name", "messageSizeDistribution", "backlogDrainRatio"))
                                .toList());
    }

    @Test
    void emptySweepsUseExistingDefaults() throws Exception {
        WorkloadSetTemplate template = new WorkloadSetTemplate();

        assertThat(new WorkloadGenerator(template).generate())
                .singleElement()
                .satisfies(
                        workload -> {
                            assertThat(workload.name).isEqualTo("1-topics-1-partitions-1kb-1p-1c-10k");
                            assertThat(workload.topics).isEqualTo(1);
                            assertThat(workload.partitionsPerTopic).isEqualTo(1);
                            assertThat(workload.messageSize).isEqualTo(1024);
                            assertThat(workload.subscriptionsPerTopic).isEqualTo(1);
                            assertThat(workload.producersPerTopic).isEqualTo(1);
                            assertThat(workload.consumerPerSubscription).isEqualTo(1);
                            assertThat(workload.producerRate).isEqualTo(10000);
                            assertThat(workload.keyDistributor).isEqualTo(KeyDistributorType.NO_KEY);
                            assertThat(workload.payloadFile).isNull();
                            assertThat(workload.useRandomizedPayloads).isFalse();
                            assertThat(workload.randomBytesRatio).isEqualTo(0.0);
                            assertThat(workload.randomizedPayloadPoolSize).isZero();
                            assertThat(workload.consumerBacklogSizeGB).isZero();
                            assertThat(workload.testDurationMinutes).isEqualTo(5);
                            assertThat(workload.warmupDurationMinutes).isEqualTo(1);
                            assertThat(workload.messageSizeDistribution).isNull();
                            assertThat(workload.backlogDrainRatio).isEqualTo(1.0);
                        });
    }

    @Test
    void nullListsUseSweepDefaultsButNullSettingsAreStillCopied() throws Exception {
        WorkloadSetTemplate template =
                new WorkloadSetTemplate(
                        "${keyDistributor}|${payloadFile}",
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        false,
                        0.0,
                        0,
                        0,
                        0,
                        0);

        assertThat(new WorkloadGenerator(template).generate())
                .singleElement()
                .satisfies(
                        workload -> {
                            assertThat(workload.messageSize).isEqualTo(1024);
                            assertThat(workload.producerRate).isEqualTo(10000);
                            assertThat(workload.keyDistributor).isNull();
                            assertThat(workload.payloadFile).isNull();
                            assertThat(workload.testDurationMinutes).isZero();
                            assertThat(workload.warmupDurationMinutes).isZero();
                            assertThat(workload.name).isEqualTo("${keyDistributor}|${payloadFile}");
                        });
    }

    @Test
    void generatesEveryCombinationInExistingOrderWithAllFieldsInNames() throws Exception {
        String format =
                "${topics}|${partitionsPerTopic}|${messageSize}|${subscriptionsPerTopic}"
                        + "|${producersPerTopic}|${consumerPerSubscription}|${producerRate}"
                        + "|${keyDistributor}|${payloadFile}|${useRandomizedPayloads}"
                        + "|${randomBytesRatio}|${randomizedPayloadPoolSize}|${consumerBacklogSizeGB}"
                        + "|${testDurationMinutes}|${warmupDurationMinutes}";
        WorkloadSetTemplate template =
                new WorkloadSetTemplate(
                        format,
                        List.of(2, 3),
                        List.of(5, 6),
                        List.of(120, 250),
                        List.of(7, 8),
                        List.of(9, 10),
                        List.of(11, 12),
                        List.of(13000, 14000),
                        KeyDistributorType.KEY_ROUND_ROBIN,
                        "payload/custom.data",
                        true,
                        0.37,
                        1234,
                        2345,
                        67,
                        89);

        List<Workload> workloads = new WorkloadGenerator(template).generate();

        assertThat(workloads).hasSize(128);
        for (int i = 0; i < workloads.size(); i++) {
            Workload workload = workloads.get(i);
            int topics = i / 64 % 2 == 0 ? 2 : 3;
            int partitions = i / 32 % 2 == 0 ? 5 : 6;
            int messageSize = i / 16 % 2 == 0 ? 120 : 250;
            int subscriptions = i / 8 % 2 == 0 ? 7 : 8;
            int producers = i / 4 % 2 == 0 ? 9 : 10;
            int consumers = i / 2 % 2 == 0 ? 11 : 12;
            int rate = i % 2 == 0 ? 13000 : 14000;

            assertThat(workload.topics).isEqualTo(topics);
            assertThat(workload.partitionsPerTopic).isEqualTo(partitions);
            assertThat(workload.messageSize).isEqualTo(messageSize);
            assertThat(workload.subscriptionsPerTopic).isEqualTo(subscriptions);
            assertThat(workload.producersPerTopic).isEqualTo(producers);
            assertThat(workload.consumerPerSubscription).isEqualTo(consumers);
            assertThat(workload.producerRate).isEqualTo(rate);
            assertThat(workload.keyDistributor).isEqualTo(KeyDistributorType.KEY_ROUND_ROBIN);
            assertThat(workload.payloadFile).isEqualTo("payload/custom.data");
            assertThat(workload.useRandomizedPayloads).isTrue();
            assertThat(workload.randomBytesRatio).isEqualTo(0.37);
            assertThat(workload.randomizedPayloadPoolSize).isEqualTo(1234);
            assertThat(workload.consumerBacklogSizeGB).isEqualTo(2345);
            assertThat(workload.testDurationMinutes).isEqualTo(67);
            assertThat(workload.warmupDurationMinutes).isEqualTo(89);
            assertThat(workload.messageSizeDistribution).isNull();
            assertThat(workload.backlogDrainRatio).isEqualTo(1.0);
            assertThat(workload.name)
                    .isEqualTo(
                            String.format(
                                    Locale.ROOT,
                                    "%d|%d|%d|%d|%d|%d|%dk|KEY_ROUND_ROBIN"
                                            + "|payload/custom.data|true|0.37|1k|2k|67|89",
                                    topics,
                                    partitions,
                                    messageSize,
                                    subscriptions,
                                    producers,
                                    consumers,
                                    rate / 1000));
        }
    }
}
