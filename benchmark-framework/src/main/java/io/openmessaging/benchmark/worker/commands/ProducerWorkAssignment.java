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
package io.openmessaging.benchmark.worker.commands;

import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import io.openmessaging.benchmark.utils.payload.PayloadTemplates;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A producer load command.
 *
 * <p>{@code equals} and {@code hashCode} compare array contents; {@code toString} prints the
 * template sizes and weights rather than the payload bytes.
 *
 * @param payloadData the payload templates
 * @param publishRate the assigned publish rate
 * @param keyDistributorType the message key distribution strategy
 * @param payloadWeights the weight of each template, or {@code null} to pick them uniformly
 */
public record ProducerWorkAssignment(
        List<byte[]> payloadData,
        double publishRate,
        KeyDistributorType keyDistributorType,
        int @Nullable [] payloadWeights) {

    public ProducerWorkAssignment(
            List<byte[]> payloadData, double publishRate, KeyDistributorType keyDistributorType) {
        this(payloadData, publishRate, keyDistributorType, null);
    }

    public ProducerWorkAssignment(
            @Nullable List<byte[]> payloadData,
            double publishRate,
            KeyDistributorType keyDistributorType,
            int @Nullable [] payloadWeights) {
        this.payloadData =
                payloadData != null
                        ? new ArrayList<>(payloadData.stream().map(ProducerWorkAssignment::getBytes).toList())
                        : new ArrayList<>();
        if (payloadWeights != null && payloadWeights.length != this.payloadData.size()) {
            throw new IllegalArgumentException("Payload weights must match the number of payloads");
        }
        this.payloadWeights = payloadWeights == null ? null : payloadWeights.clone();
        this.publishRate = publishRate;
        this.keyDistributorType = keyDistributorType;
    }

    private static byte @Nullable [] getBytes(byte @Nullable [] bytes) {
        return bytes != null ? bytes.clone() : null;
    }

    // Return defensive copy in getter
    @Override
    public List<byte[]> payloadData() {
        return new ArrayList<>(payloadData.stream().map(ProducerWorkAssignment::getBytes).toList());
    }

    /** Returns the number of payload templates without copying them. */
    public int payloadCount() {
        return payloadData.size();
    }

    @Override
    public int @Nullable [] payloadWeights() {
        return payloadWeights == null ? null : payloadWeights.clone();
    }

    public ProducerWorkAssignment withPublishRate(double publishRate) {
        return new ProducerWorkAssignment(
                this.payloadData, publishRate, this.keyDistributorType, this.payloadWeights);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProducerWorkAssignment that
                && Double.compare(publishRate, that.publishRate) == 0
                && keyDistributorType == that.keyDistributorType
                && PayloadTemplates.equals(payloadData, that.payloadData)
                && Arrays.equals(payloadWeights, that.payloadWeights);
    }

    @Override
    public int hashCode() {
        int hash = PayloadTemplates.hashCode(payloadData);
        hash = 31 * hash + Double.hashCode(publishRate);
        hash = 31 * hash + Objects.hashCode(keyDistributorType);
        return 31 * hash + Arrays.hashCode(payloadWeights);
    }

    @Override
    public String toString() {
        return "ProducerWorkAssignment[payloadData="
                + PayloadTemplates.describe(payloadData)
                + ", publishRate="
                + publishRate
                + ", keyDistributorType="
                + keyDistributorType
                + ", payloadWeights="
                + Arrays.toString(payloadWeights)
                + "]";
    }
}
