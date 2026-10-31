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
import java.util.ArrayList;
import java.util.List;

public record ProducerWorkAssignment(
        List<byte[]> payloadData,
        double publishRate,
        KeyDistributorType keyDistributorType,
        int[] payloadWeights) {

    public ProducerWorkAssignment(
            List<byte[]> payloadData, double publishRate, KeyDistributorType keyDistributorType) {
        this(payloadData, publishRate, keyDistributorType, null);
    }

    public ProducerWorkAssignment(
            List<byte[]> payloadData,
            double publishRate,
            KeyDistributorType keyDistributorType,
            int[] payloadWeights) {
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

    private static byte[] getBytes(byte[] bytes) {
        return bytes != null ? bytes.clone() : null;
    }

    // Return defensive copy in getter
    public List<byte[]> payloadData() {
        return new ArrayList<>(payloadData.stream().map(ProducerWorkAssignment::getBytes).toList());
    }

    public int[] payloadWeights() {
        return payloadWeights == null ? null : payloadWeights.clone();
    }

    public ProducerWorkAssignment withPublishRate(double publishRate) {
        return new ProducerWorkAssignment(
                this.payloadData, publishRate, this.keyDistributorType, this.payloadWeights);
    }
}
