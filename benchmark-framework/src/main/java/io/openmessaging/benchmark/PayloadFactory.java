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

import io.openmessaging.benchmark.utils.payload.FilePayloadReader;
import io.openmessaging.benchmark.utils.payload.MessageSizeDistribution;
import io.openmessaging.benchmark.utils.payload.PayloadTemplates;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Prepares the payloads a workload's producers send. */
final class PayloadFactory {
    // Logged under the generator's name, which log configurations and users already know
    @SuppressWarnings("java:S3416")
    private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);

    /**
     * The payloads to send.
     *
     * <p>{@code equals} and {@code hashCode} compare array contents; {@code toString} prints the
     * template sizes and weights rather than the payload bytes.
     *
     * @param data the payload templates
     * @param weights the weight of each template, or {@code null} to pick them uniformly
     */
    record Payloads(List<byte[]> data, int @Nullable [] weights) {

        @Override
        public boolean equals(Object other) {
            return other instanceof Payloads(List<byte[]> otherData, int[] otherWeights)
                    && PayloadTemplates.equals(data, otherData)
                    && Arrays.equals(weights, otherWeights);
        }

        @Override
        public int hashCode() {
            return 31 * PayloadTemplates.hashCode(data) + Arrays.hashCode(weights);
        }

        @Override
        public String toString() {
            return "Payloads[data="
                    + PayloadTemplates.describe(data)
                    + ", weights="
                    + Arrays.toString(weights)
                    + "]";
        }
    }

    private PayloadFactory() {}

    static Payloads create(Workload workload) {
        Random random = ThreadLocalRandom.current();
        Map<String, Integer> sizes = workload.messageSizeDistribution;
        if (sizes != null && !sizes.isEmpty()) {
            MessageSizeDistribution distribution = new MessageSizeDistribution(sizes);
            List<byte[]> payloads = new ArrayList<>();
            for (int size : distribution.getBucketSizes()) {
                payloads.add(
                        workload.useRandomizedPayloads
                                ? randomized(size, workload.randomBytesRatio, random)
                                : new byte[size]);
            }
            log.info(
                    "Prepared {} payload templates for message-size distribution (bucket sizes: {}, "
                            + "weighted average: {} bytes)",
                    payloads.size(),
                    distribution.getBucketSizes(),
                    distribution.getAvgSize());
            return new Payloads(payloads, distribution.getWeights());
        }
        if (workload.useRandomizedPayloads) {
            List<byte[]> payloads = new ArrayList<>();
            for (int i = 0; i < workload.randomizedPayloadPoolSize; i++) {
                payloads.add(randomized(workload.messageSize, workload.randomBytesRatio, random));
            }
            return new Payloads(payloads, null);
        }
        String payloadFile = workload.payloadFile;
        if (payloadFile == null) {
            throw new IllegalArgumentException(
                    "Workload "
                            + workload.name
                            + " must set payloadFile, useRandomizedPayloads or"
                            + " messageSizeDistribution");
        }
        return new Payloads(
                List.of(new FilePayloadReader(workload.messageSize).load(payloadFile)), null);
    }

    // A payload whose first randomBytesRatio share is random and whose remainder is zero
    private static byte[] randomized(int size, double randomBytesRatio, Random random) {
        byte[] payload = new byte[size];
        byte[] randomBytes = new byte[(int) (size * randomBytesRatio)];
        random.nextBytes(randomBytes);
        System.arraycopy(randomBytes, 0, payload, 0, randomBytes.length);
        return payload;
    }
}
