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
package io.openmessaging.benchmark.utils.payload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class PayloadTemplatesTest {

    @Test
    void copiesTemplatesAndWeightsOnceAndSharesThemAfterwards() {
        byte[] template = {1, 2};
        List<byte[]> source = new ArrayList<>(List.of(template));
        int[] weights = {4};
        PayloadTemplates templates = PayloadTemplates.of(source, weights);
        template[0] = 9;
        source.add(new byte[] {3});
        weights[0] = 7;

        assertThat(templates.size()).isEqualTo(1);
        assertThat(templates.template(0))
                .containsExactly(1, 2)
                .isSameAs(templates.templates().getFirst());
        assertThat(templates.weighted()).isTrue();
        assertThat(templates.weights()).containsExactly(4);
        templates.weights()[0] = 8;
        assertThat(templates.weights()).containsExactly(4);
        List<byte[]> view = templates.templates();
        byte[] extra = new byte[1];
        assertThatThrownBy(() -> view.add(extra)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void treatsMissingTemplatesAsEmptyAndMissingWeightsAsUniform() {
        PayloadTemplates templates = PayloadTemplates.of(null, null);

        assertThat(templates.size()).isZero();
        assertThat(templates.templates()).isEmpty();
        assertThat(templates.weighted()).isFalse();
        assertThat(templates.weights()).isNull();
    }

    @Test
    void rejectsMismatchedWeightsAndNullTemplates() {
        List<byte[]> one = List.of(new byte[] {1});
        int[] two = {1, 2};
        assertThatThrownBy(() -> PayloadTemplates.of(one, two))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Payload weights must match the number of payloads");
        List<byte[]> withNull = Arrays.asList(new byte[] {1}, null);
        assertThatThrownBy(() -> PayloadTemplates.of(withNull, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void comparesAndHashesByContent() {
        PayloadTemplates templates =
                PayloadTemplates.of(List.of(new byte[] {1, 2}, new byte[] {3}), new int[] {1, 2});
        PayloadTemplates same =
                PayloadTemplates.of(List.of(new byte[] {1, 2}, new byte[] {3}), new int[] {1, 2});

        assertThat(templates)
                .isEqualTo(same)
                .hasSameHashCodeAs(same)
                .isNotEqualTo("other")
                .isNotEqualTo(
                        PayloadTemplates.of(List.of(new byte[] {1, 2}, new byte[] {4}), new int[] {1, 2}))
                .isNotEqualTo(PayloadTemplates.of(List.of(new byte[] {1, 2}), new int[] {1}))
                .isNotEqualTo(
                        PayloadTemplates.of(List.of(new byte[] {1, 2}, new byte[] {3}), new int[] {2, 1}))
                .isNotEqualTo(PayloadTemplates.of(List.of(new byte[] {1, 2}, new byte[] {3}), null));
        assertThat(PayloadTemplates.of(List.of(new byte[] {1, 2}, new byte[] {3}), null))
                .doesNotHaveSameHashCodeAs(
                        PayloadTemplates.of(List.of(new byte[] {3}, new byte[] {1, 2}), null));
    }

    @Test
    void describesTemplatesBySizeWithoutPrintingBytes() {
        assertThat(PayloadTemplates.of(List.of(new byte[1024], new byte[2]), new int[] {3, 1}))
                .hasToString("2 templates, sizes [1024, 2], weights [3, 1]");
        assertThat(PayloadTemplates.of(List.of(new byte[1]), null))
                .hasToString("1 templates, sizes [1], weights null");
    }
}
