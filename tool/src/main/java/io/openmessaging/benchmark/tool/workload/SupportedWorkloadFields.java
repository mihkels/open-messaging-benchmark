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

import io.openmessaging.benchmark.Workload;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;

/** The workload fields supported by templates, including their name-format representations. */
final class SupportedWorkloadFields {
    private static final long MAX_PRODUCER_RATE = 10_000_000;

    // Order defines the Cartesian-product traversal; the last field varies fastest.
    static final List<SweepField> SWEEP_FIELDS =
            List.of(
                    new SweepField(
                            "topics",
                            WorkloadSetTemplate::topics,
                            1,
                            (workload, value) -> workload.topics = value,
                            workload -> countToDisplaySize(workload.topics)),
                    new SweepField(
                            "partitionsPerTopic",
                            WorkloadSetTemplate::partitionsPerTopic,
                            1,
                            (workload, value) -> workload.partitionsPerTopic = value,
                            workload -> countToDisplaySize(workload.partitionsPerTopic)),
                    new SweepField(
                            "messageSize",
                            WorkloadSetTemplate::messageSize,
                            1024,
                            (workload, value) -> workload.messageSize = value,
                            workload -> countToDisplaySize(workload.messageSize)),
                    new SweepField(
                            "subscriptionsPerTopic",
                            WorkloadSetTemplate::subscriptionsPerTopic,
                            1,
                            (workload, value) -> workload.subscriptionsPerTopic = value,
                            workload -> countToDisplaySize(workload.subscriptionsPerTopic)),
                    new SweepField(
                            "producersPerTopic",
                            WorkloadSetTemplate::producersPerTopic,
                            1,
                            (workload, value) -> workload.producersPerTopic = value,
                            workload -> countToDisplaySize(workload.producersPerTopic)),
                    new SweepField(
                            "consumerPerSubscription",
                            WorkloadSetTemplate::consumerPerSubscription,
                            1,
                            (workload, value) -> workload.consumerPerSubscription = value,
                            workload -> countToDisplaySize(workload.consumerPerSubscription)),
                    new SweepField(
                            "producerRate",
                            WorkloadSetTemplate::producerRate,
                            10000,
                            (workload, value) -> workload.producerRate = value,
                            workload ->
                                    workload.producerRate >= MAX_PRODUCER_RATE
                                            ? "max-rate"
                                            : countToDisplaySize(workload.producerRate)));

    private static final List<FixedField> FIXED_FIELDS =
            List.of(
                    new FixedField(
                            "keyDistributor",
                            (template, workload) -> workload.keyDistributor = template.keyDistributor(),
                            workload -> workload.keyDistributor),
                    new FixedField(
                            "payloadFile",
                            (template, workload) -> workload.payloadFile = template.payloadFile(),
                            workload -> workload.payloadFile),
                    new FixedField(
                            "useRandomizedPayloads",
                            (template, workload) ->
                                    workload.useRandomizedPayloads = template.useRandomizedPayloads(),
                            workload -> workload.useRandomizedPayloads),
                    new FixedField(
                            "randomBytesRatio",
                            (template, workload) -> workload.randomBytesRatio = template.randomBytesRatio(),
                            workload -> workload.randomBytesRatio),
                    new FixedField(
                            "randomizedPayloadPoolSize",
                            (template, workload) ->
                                    workload.randomizedPayloadPoolSize = template.randomizedPayloadPoolSize(),
                            workload -> countToDisplaySize(workload.randomizedPayloadPoolSize)),
                    new FixedField(
                            "consumerBacklogSizeGB",
                            (template, workload) ->
                                    workload.consumerBacklogSizeGB = template.consumerBacklogSizeGB(),
                            workload -> countToDisplaySize(workload.consumerBacklogSizeGB)),
                    new FixedField(
                            "testDurationMinutes",
                            (template, workload) -> workload.testDurationMinutes = template.testDurationMinutes(),
                            workload -> workload.testDurationMinutes),
                    new FixedField(
                            "warmupDurationMinutes",
                            (template, workload) ->
                                    workload.warmupDurationMinutes = template.warmupDurationMinutes(),
                            workload -> workload.warmupDurationMinutes));

    private SupportedWorkloadFields() {}

    static void applyFixedFields(WorkloadSetTemplate template, Workload workload) {
        for (FixedField field : FIXED_FIELDS) {
            field.assign().accept(template, workload);
        }
    }

    static Map<String, Object> nameVariables(Workload workload) {
        Map<String, Object> variables = new HashMap<>();
        for (SweepField field : SWEEP_FIELDS) {
            variables.put(field.name(), field.nameValue().apply(workload));
        }
        for (FixedField field : FIXED_FIELDS) {
            variables.put(field.name(), field.nameValue().apply(workload));
        }
        return variables;
    }

    record SweepField(
            String name,
            Function<WorkloadSetTemplate, List<Integer>> templateValues,
            int defaultValue,
            ObjIntConsumer<Workload> assign,
            Function<Workload, Object> nameValue) {
        List<Integer> valuesOrDefault(WorkloadSetTemplate template) {
            List<Integer> values = templateValues.apply(template);
            return values != null && !values.isEmpty() ? values : List.of(defaultValue);
        }
    }

    private record FixedField(
            String name,
            BiConsumer<WorkloadSetTemplate, Workload> assign,
            Function<Workload, Object> nameValue) {}

    private static String countToDisplaySize(long size) {
        if (size / 1_000_000_000L > 0L) {
            return size / 1_000_000_000L + "g";
        } else if (size / 1_000_000L > 0L) {
            return size / 1_000_000L + "m";
        } else if (size / 1_000L > 0L) {
            return size / 1_000 + "k";
        } else {
            return size + "";
        }
    }
}
