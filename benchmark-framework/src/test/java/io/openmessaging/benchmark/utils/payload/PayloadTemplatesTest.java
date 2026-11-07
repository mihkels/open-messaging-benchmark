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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class PayloadTemplatesTest {

    @Test
    void comparesTemplatesByContent() {
        List<byte[]> templates = List.of(new byte[] {1, 2}, new byte[] {3});

        assertThat(PayloadTemplates.equals(templates, templates)).isTrue();
        assertThat(PayloadTemplates.equals(templates, List.of(new byte[] {1, 2}, new byte[] {3})))
                .isTrue();
        assertThat(PayloadTemplates.equals(templates, List.of(new byte[] {1, 2}, new byte[] {4})))
                .isFalse();
        assertThat(PayloadTemplates.equals(templates, List.of(new byte[] {1, 2}))).isFalse();
        assertThat(PayloadTemplates.equals(templates, null)).isFalse();
        assertThat(PayloadTemplates.equals(null, templates)).isFalse();
        assertThat(PayloadTemplates.equals(null, null)).isTrue();
        assertThat(PayloadTemplates.equals(Arrays.asList((byte[]) null), Arrays.asList((byte[]) null)))
                .isTrue();
    }

    @Test
    void hashesTemplatesByContent() {
        assertThat(PayloadTemplates.hashCode(List.of(new byte[] {1, 2}, new byte[] {3})))
                .isEqualTo(PayloadTemplates.hashCode(List.of(new byte[] {1, 2}, new byte[] {3})))
                .isNotEqualTo(PayloadTemplates.hashCode(List.of(new byte[] {3}, new byte[] {1, 2})));
        assertThat(PayloadTemplates.hashCode(null)).isZero();
    }

    @Test
    void describesTemplatesBySizeWithoutPrintingBytes() {
        assertThat(PayloadTemplates.describe(Arrays.asList(new byte[1024], null)))
                .isEqualTo("2 templates, sizes [1024, null]");
        assertThat(PayloadTemplates.describe(null)).isEqualTo("null");
    }
}
