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

import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProducerWorkAssignmentTest {

    @Test
    void copiesPayloadWeightsAndPreservesThemWhenChangingPublishRate() {
        int[] weights = {1, 3};
        ProducerWorkAssignment assignment =
                new ProducerWorkAssignment(
                        List.of(new byte[] {1}, new byte[] {2}), 10, KeyDistributorType.NO_KEY, weights);
        weights[0] = 9;

        assertThat(assignment.payloadWeights()).containsExactly(1, 3);
        assertThat(assignment.payloadCount()).isEqualTo(2);

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
    void toStringDescribesPayloadSizesInsteadOfArrayIdentities() {
        ProducerWorkAssignment assignment =
                new ProducerWorkAssignment(
                        List.of(new byte[] {1, 2, 3}), 10, KeyDistributorType.NO_KEY, new int[] {7});

        assertThat(assignment)
                .hasToString(
                        "ProducerWorkAssignment[payloadData=1 templates, sizes [3], publishRate=10.0, "
                                + "keyDistributorType=NO_KEY, payloadWeights=[7]]");
    }
}
