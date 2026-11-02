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
package io.openmessaging.benchmark.worker;

import io.openmessaging.benchmark.utils.payload.MessageSizeDistribution;
import java.util.List;
import java.util.random.RandomGenerator;

/** Picks the payload of each message, uniformly or according to the payload weights. */
final class PayloadSampler {
    private final List<byte[]> payloads;
    private final int[] cumulativeWeights;
    private final int totalWeight;

    private PayloadSampler(List<byte[]> payloads, int[] cumulativeWeights, int totalWeight) {
        this.payloads = payloads;
        this.cumulativeWeights = cumulativeWeights;
        this.totalWeight = totalWeight;
    }

    /**
     * Validates the payloads and their optional weights.
     *
     * @param payloads the payloads, at least one
     * @param weights one non-negative weight per payload, or {@code null} for a uniform choice
     * @return the sampler
     * @throws IllegalArgumentException if there are no payloads or the weights are invalid
     */
    static PayloadSampler of(List<byte[]> payloads, int[] weights) {
        if (payloads.isEmpty()) {
            throw new IllegalArgumentException("At least one payload is required");
        }
        if (weights == null) {
            return new PayloadSampler(List.copyOf(payloads), null, payloads.size());
        }
        int[] cumulativeWeights = new int[weights.length];
        int totalWeight = 0;
        for (int i = 0; i < weights.length; i++) {
            if (weights[i] < 0) {
                throw new IllegalArgumentException("Payload weights cannot be negative");
            }
            try {
                totalWeight = Math.addExact(totalWeight, weights[i]);
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException("Payload weights exceed the supported total", e);
            }
            cumulativeWeights[i] = totalWeight;
        }
        if (totalWeight == 0) {
            throw new IllegalArgumentException("Payload weights must sum to a positive value");
        }
        return new PayloadSampler(List.copyOf(payloads), cumulativeWeights, totalWeight);
    }

    byte[] next(RandomGenerator random) {
        int index =
                cumulativeWeights == null
                        ? random.nextInt(payloads.size())
                        : MessageSizeDistribution.bucketIndexForTicket(
                                random.nextInt(totalWeight), cumulativeWeights);
        return payloads.get(index);
    }
}
