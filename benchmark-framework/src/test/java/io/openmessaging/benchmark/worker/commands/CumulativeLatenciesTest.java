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

import org.junit.jupiter.api.Test;

class CumulativeLatenciesTest {

    @Test
    void zeroPlus() {
        CumulativeLatencies one = new CumulativeLatencies();
        CumulativeLatencies two = new CumulativeLatencies();
        CumulativeLatencies result = one.plus(two);
        assertThat(result)
                .satisfies(
                        r -> {
                            assertThat(r.getPublishLatency().getTotalCount())
                                    .isEqualTo(two.getPublishLatency().getTotalCount());
                            assertThat(r.getPublishDelayLatency().getTotalCount())
                                    .isEqualTo(two.getPublishDelayLatency().getTotalCount());
                            assertThat(r.getEndToEndLatency().getTotalCount())
                                    .isEqualTo(two.getEndToEndLatency().getTotalCount());
                        });
    }
}
