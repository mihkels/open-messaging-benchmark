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

import io.openmessaging.benchmark.utils.payload.CompressiblePayloads;
import io.openmessaging.benchmark.utils.payload.FilePayloadReader;
import io.openmessaging.benchmark.utils.payload.MessageSizeDistribution;
import io.openmessaging.benchmark.utils.payload.PayloadGenerator;
import io.openmessaging.benchmark.utils.payload.PayloadTemplates;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Prepares the payloads a workload's producers send. */
final class PayloadFactory {
    private static final Logger log = LoggerFactory.getLogger(PayloadFactory.class);

    private PayloadFactory() {}

    static PayloadTemplates create(Workload workload) {
        Random random = ThreadLocalRandom.current();
        PayloadGenerator generator = workload.payloadGenerator;
        if (generator != null) {
            return generated(workload, generator, random);
        }
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
            return PayloadTemplates.of(payloads, distribution.getWeights());
        }
        if (workload.useRandomizedPayloads) {
            List<byte[]> payloads = new ArrayList<>();
            for (int i = 0; i < workload.randomizedPayloadPoolSize; i++) {
                payloads.add(randomized(workload.messageSize, workload.randomBytesRatio, random));
            }
            return PayloadTemplates.of(payloads, null);
        }
        String payloadFile = workload.payloadFile;
        if (payloadFile == null) {
            throw new IllegalArgumentException(
                    "Workload "
                            + workload.name
                            + " must set payloadFile, payloadGenerator,"
                            + " useRandomizedPayloads or messageSizeDistribution");
        }
        return PayloadTemplates.of(
                List.of(new FilePayloadReader(workload.messageSize).load(payloadFile)), null);
    }

    // poolSize payloads per size; with a distribution each payload carries its bucket's weight
    private static PayloadTemplates generated(
            Workload workload, PayloadGenerator generator, Random random) {
        double target = generator.targetCompressionRatio();
        int poolSize = generator.poolSize();
        Map<String, Integer> sizes = workload.messageSizeDistribution;
        if (sizes == null || sizes.isEmpty()) {
            return PayloadTemplates.of(
                    CompressiblePayloads.pool(workload.messageSize, target, poolSize, random), null);
        }
        MessageSizeDistribution distribution = new MessageSizeDistribution(sizes);
        List<Integer> bucketSizes = distribution.getBucketSizes();
        int[] bucketWeights = distribution.getWeights();
        List<byte[]> payloads = new ArrayList<>();
        int[] weights = new int[bucketSizes.size() * poolSize];
        for (int bucket = 0; bucket < bucketSizes.size(); bucket++) {
            payloads.addAll(CompressiblePayloads.pool(bucketSizes.get(bucket), target, poolSize, random));
            Arrays.fill(weights, bucket * poolSize, (bucket + 1) * poolSize, bucketWeights[bucket]);
        }
        return PayloadTemplates.of(payloads, weights);
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
