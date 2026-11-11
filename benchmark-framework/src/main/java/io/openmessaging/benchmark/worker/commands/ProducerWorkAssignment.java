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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import io.openmessaging.benchmark.utils.distributor.KeyDistribution;
import io.openmessaging.benchmark.utils.payload.PayloadTemplates;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * A producer load command.
 *
 * <p>On the wire the payloads are flattened into {@code payloadData} and {@code payloadWeights}.
 *
 * @param payloads the payload templates and their optional weights
 * @param publishRate the assigned publish rate
 * @param keyDistributorType the message key distribution strategy
 * @param keyDistribution optional Zipfian distribution parameters
 */
@JsonPropertyOrder({"payloadData", "publishRate", "keyDistributorType", "payloadWeights", "keyDistribution"})
public record ProducerWorkAssignment(
        @JsonIgnore PayloadTemplates payloads,
        double publishRate,
        KeyDistributorType keyDistributorType,
        @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable KeyDistribution keyDistribution) {

    public ProducerWorkAssignment(
            PayloadTemplates payloads, double publishRate, KeyDistributorType keyDistributorType) {
        this(payloads, publishRate, keyDistributorType, null);
    }

    public ProducerWorkAssignment(
            List<byte[]> payloadData, double publishRate, KeyDistributorType keyDistributorType) {
        this(payloadData, publishRate, keyDistributorType, null);
    }

    public ProducerWorkAssignment(
            @Nullable List<byte[]> payloadData,
            double publishRate,
            KeyDistributorType keyDistributorType,
            int @Nullable [] payloadWeights) {
        this(PayloadTemplates.of(payloadData, payloadWeights), publishRate, keyDistributorType, null);
    }

    @JsonCreator
    public ProducerWorkAssignment(
            @JsonProperty("payloadData") @Nullable List<byte[]> payloadData,
            @JsonProperty("publishRate") double publishRate,
            @JsonProperty("keyDistributorType") KeyDistributorType keyDistributorType,
            @JsonProperty("payloadWeights") int @Nullable [] payloadWeights,
            @JsonProperty("keyDistribution") @Nullable KeyDistribution keyDistribution) {
        this(PayloadTemplates.of(payloadData, payloadWeights), publishRate, keyDistributorType, keyDistribution);
    }

    /**
     * Returns the payload templates without copying them.
     *
     * @return the stored templates, whose arrays must not be modified
     */
    @JsonProperty("payloadData")
    public List<byte[]> payloadData() {
        return payloads.templates();
    }

    /**
     * Returns a copy of the payload weights.
     *
     * @return the weight of each template, or {@code null} to pick them uniformly
     */
    @JsonProperty("payloadWeights")
    public int @Nullable [] payloadWeights() {
        return payloads.weights();
    }

    /**
     * Returns this assignment with another publish rate, sharing the payloads.
     *
     * @param publishRate the new publish rate
     * @return the assignment
     */
    public ProducerWorkAssignment withPublishRate(double publishRate) {
        return new ProducerWorkAssignment(payloads, publishRate, keyDistributorType, keyDistribution);
    }
}
