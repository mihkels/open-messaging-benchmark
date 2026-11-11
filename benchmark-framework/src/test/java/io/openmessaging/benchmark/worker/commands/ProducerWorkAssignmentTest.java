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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.openmessaging.benchmark.utils.distributor.KeyDistribution;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import io.openmessaging.benchmark.worker.jackson.ObjectMappers;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class ProducerWorkAssignmentTest {

    @Test
    void copiesPayloadWeightsAndPreservesThemWhenChangingPublishRate() {
        int[] weights = {1, 3};
        ProducerWorkAssignment assignment =
                new ProducerWorkAssignment(
                        List.of(new byte[] {1}, new byte[] {2}), 10, KeyDistributorType.NO_KEY, weights);
        weights[0] = 9;

        assertThat(assignment.payloadWeights()).containsExactly(1, 3);
        assertThat(assignment.payloads().size()).isEqualTo(2);

        int[] returnedWeights = assignment.payloadWeights();
        returnedWeights[0] = 8;
        assertThat(assignment.withPublishRate(20).payloadWeights()).containsExactly(1, 3);
    }

    @Test
    void requiresOneWeightPerPayload() {
        List<byte[]> payloads = List.of(new byte[] {1});
        int[] weights = {1, 2};
        assertThatThrownBy(
                        () -> new ProducerWorkAssignment(payloads, 10, KeyDistributorType.NO_KEY, weights))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void equalityAndHashCodeUsePayloadAndWeightContents() {
        ProducerWorkAssignment assignment =
                new ProducerWorkAssignment(
                        List.of(new byte[] {1}, new byte[] {2}),
                        10,
                        KeyDistributorType.NO_KEY,
                        new int[] {1, 3});
        ProducerWorkAssignment same =
                new ProducerWorkAssignment(
                        List.of(new byte[] {1}, new byte[] {2}),
                        10,
                        KeyDistributorType.NO_KEY,
                        new int[] {1, 3});

        assertThat(assignment)
                .isEqualTo(same)
                .hasSameHashCodeAs(same)
                .isNotEqualTo("other")
                .isNotEqualTo(
                        new ProducerWorkAssignment(
                                List.of(new byte[] {1}, new byte[] {9}),
                                10,
                                KeyDistributorType.NO_KEY,
                                new int[] {1, 3}))
                .isNotEqualTo(
                        new ProducerWorkAssignment(
                                List.of(new byte[] {1}, new byte[] {2}),
                                10,
                                KeyDistributorType.NO_KEY,
                                new int[] {3, 1}))
                .isNotEqualTo(assignment.withPublishRate(20))
                .isNotEqualTo(
                        new ProducerWorkAssignment(
                                List.of(new byte[] {1}, new byte[] {2}),
                                10,
                                KeyDistributorType.RANDOM_NANO,
                                new int[] {1, 3}));
    }

    @Test
    void withPublishRateSharesThePayloadsWithoutCopying() {
        ProducerWorkAssignment assignment =
                new ProducerWorkAssignment(List.of(new byte[] {1}), 10, KeyDistributorType.NO_KEY);

        ProducerWorkAssignment changed = assignment.withPublishRate(20);

        assertThat(changed.publishRate()).isEqualTo(20);
        assertThat(changed.payloads()).isSameAs(assignment.payloads());
        assertThat(changed.payloadData().getFirst()).isSameAs(assignment.payloadData().getFirst());
    }

    @Test
    void toStringDescribesPayloadSizesInsteadOfArrayIdentities() {
        ProducerWorkAssignment assignment =
                new ProducerWorkAssignment(
                        List.of(new byte[] {1, 2, 3}), 10, KeyDistributorType.NO_KEY, new int[] {7});

        assertThat(assignment)
                .hasToString(
                        "ProducerWorkAssignment[payloads=1 templates, sizes [3], weights [7], publishRate=10.0, "
                                + "keyDistributorType=NO_KEY, keyDistribution=null]");
    }

    @Test
    void keepsTheWireFormat() {
        ProducerWorkAssignment weighted =
                new ProducerWorkAssignment(
                        List.of(new byte[] {0, 1, -1}, new byte[] {2, 3}),
                        123.5,
                        KeyDistributorType.NO_KEY,
                        new int[] {3, 1});
        ProducerWorkAssignment uniform =
                new ProducerWorkAssignment(List.of(new byte[] {7}), 10, KeyDistributorType.RANDOM_NANO);
        ObjectMapper mapper = ObjectMappers.DEFAULT.mapper();

        String weightedJson =
                "{\"payloadData\":[\"AAH/\",\"AgM=\"],\"publishRate\":123.5,"
                        + "\"keyDistributorType\":\"NO_KEY\",\"payloadWeights\":[3,1]}";
        String uniformJson =
                "{\"payloadData\":[\"Bw==\"],\"publishRate\":10.0,"
                        + "\"keyDistributorType\":\"RANDOM_NANO\",\"payloadWeights\":null}";
        assertThat(mapper.writeValueAsString(weighted)).isEqualTo(weightedJson);
        assertThat(mapper.writeValueAsString(uniform)).isEqualTo(uniformJson);
        assertThat(mapper.readValue(weightedJson, ProducerWorkAssignment.class)).isEqualTo(weighted);
        assertThat(mapper.readValue(uniformJson, ProducerWorkAssignment.class)).isEqualTo(uniform);
    }

    @Test
    void preservesZipfianParametersOnWireAndRateChange() {
        var config = new KeyDistribution(37, 1.2);
        var assignment =
                new ProducerWorkAssignment(
                        io.openmessaging.benchmark.utils.payload.PayloadTemplates.of(
                                List.of(new byte[] {1}), null),
                        10,
                        KeyDistributorType.ZIPFIAN,
                        config);
        ObjectMapper mapper = ObjectMappers.DEFAULT.mapper();
        String json = mapper.writeValueAsString(assignment);
        assertThat(json).contains("\"keyDistribution\":{\"keys\":37,\"exponent\":1.2}");
        assertThat(mapper.readValue(json, ProducerWorkAssignment.class)).isEqualTo(assignment);
        assertThat(assignment.withPublishRate(20).keyDistribution()).isEqualTo(config);
    }
}
