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
package io.openmessaging.benchmark;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;

public record Workload(
        WorkloadSetTemplate template,
        String name,
        int topics,
        int partitionsPerTopic,
        int messageSize,
        int subscriptionsPerTopic,
        int producersPerTopic,
        int consumerPerSubscription,
        int producerRate,
        double backlogDrainRatio) {

    private static final long MAX_PRODUCER_RATE = 10_000_000;

    public Workload {
        if (template == null) {
            throw new IllegalArgumentException("template cannot be null");
        }

        if (name == null || name.isBlank()) {
            name =
                    loadWorkloadName(
                            name,
                            template,
                            topics,
                            partitionsPerTopic,
                            messageSize,
                            subscriptionsPerTopic,
                            producersPerTopic,
                            consumerPerSubscription,
                            producerRate);
        }

        if (backlogDrainRatio < 0.0 || backlogDrainRatio > 1.0) {
            throw new IllegalArgumentException("backlogDrainRatio must be between 0.0 and 1.0");
        }
    }

    public Workload() {
        this(new WorkloadSetTemplate(), null, 0, 0, 0, 0, 0, 0, 0, 1.0);
    }

    /**
     * Generates a name for the given workload. If the workload has a non-null name, that is simply
     * returned. Otherwise, the format string is used to generate a name.
     *
     * @return the name of the workload
     */
    private static String loadWorkloadName(
            String name,
            WorkloadSetTemplate template,
            int topics,
            int partitionsPerTopic,
            int messageSize,
            int subscriptionsPerTopic,
            int producersPerTopic,
            int consumerPerSubscription,
            int producerRate) {
        if (name != null) {
            return name;
        }

        Map<String, Object> params = new HashMap<>();
        params.put("topics", countToDisplaySize(topics));
        params.put("partitionsPerTopic", countToDisplaySize(partitionsPerTopic));
        params.put("messageSize", countToDisplaySize(messageSize));
        params.put("subscriptionsPerTopic", countToDisplaySize(subscriptionsPerTopic));
        params.put("producersPerTopic", countToDisplaySize(producersPerTopic));
        params.put("consumerPerSubscription", countToDisplaySize(consumerPerSubscription));
        params.put(
                "producerRate",
                (producerRate >= MAX_PRODUCER_RATE) ? "max-rate" : countToDisplaySize(producerRate));
        params.put("keyDistributor", template.keyDistributor());
        params.put("payloadFile", template.payloadFile());
        params.put("useRandomizedPayloads", template.useRandomizedPayloads());
        params.put("randomBytesRatio", template.randomBytesRatio());
        params.put(
                "randomizedPayloadPoolSize", countToDisplaySize(template.randomizedPayloadPoolSize()));
        params.put("consumerBacklogSizeGB", countToDisplaySize(template.consumerBacklogSizeGB()));
        params.put("testDurationMinutes", template.testDurationMinutes());
        params.put("warmupDurationMinutes", template.warmupDurationMinutes());
        return StringSubstitutor.replace(template.nameFormat(), params, "${", "}");
    }

    private static String countToDisplaySize(long size) {
        String displaySize;
        if (size / 1_000_000_000L > 0L) {
            displaySize = size / 1_000_000_000L + "g";
        } else if (size / 1_000_000L > 0L) {
            displaySize = size / 1_000_000L + "m";
        } else if (size / 1_000L > 0L) {
            displaySize = size / 1_000 + "k";
        } else {
            displaySize = size + "";
        }
        return displaySize;
    }
}
