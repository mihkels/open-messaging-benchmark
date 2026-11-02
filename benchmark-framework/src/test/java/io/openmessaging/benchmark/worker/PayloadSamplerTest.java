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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class PayloadSamplerTest {
    private final byte[] small = new byte[1];
    private final byte[] large = new byte[2];

    @Test
    void picksOnlyPayloadsWithWeight() {
        PayloadSampler sampler = PayloadSampler.of(List.of(small, large), new int[] {0, 3});
        Random random = new Random(42);

        for (int i = 0; i < 100; i++) {
            assertThat(sampler.next(random)).isSameAs(large);
        }
    }

    @Test
    void picksEveryPayloadWithoutWeights() {
        PayloadSampler sampler = PayloadSampler.of(List.of(small, large), null);
        Random random = new Random(42);

        List<byte[]> picked =
                java.util.stream.Stream.generate(() -> sampler.next(random)).limit(100).toList();

        assertThat(picked).contains(small, large);
    }

    @Test
    void rejectsInvalidPayloadsAndWeights() {
        assertThatThrownBy(() -> PayloadSampler.of(List.of(), null))
                .hasMessage("At least one payload is required");
        assertThatThrownBy(() -> PayloadSampler.of(List.of(small), new int[] {-1}))
                .hasMessage("Payload weights cannot be negative");
        assertThatThrownBy(() -> PayloadSampler.of(List.of(small), new int[] {0}))
                .hasMessage("Payload weights must sum to a positive value");
        assertThatThrownBy(
                        () -> PayloadSampler.of(List.of(small, large), new int[] {Integer.MAX_VALUE, 1}))
                .hasMessage("Payload weights exceed the supported total");
    }
}
