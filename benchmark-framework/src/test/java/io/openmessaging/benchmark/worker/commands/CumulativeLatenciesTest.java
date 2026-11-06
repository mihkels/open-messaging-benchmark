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
    void combinesCorrectedHistogramsWithoutChangingInputs() {
        CumulativeLatencies one = new CumulativeLatencies();
        CumulativeLatencies two = new CumulativeLatencies();
        one.publishScheduleLatency.recordValue(1_200);
        two.publishScheduleLatency.recordValue(100_000);
        CumulativeLatencies result = one.plus(two);
        assertThat(result.publishScheduleLatency.getTotalCount()).isEqualTo(2);
        assertThat(result.publishScheduleLatency.getValueAtPercentile(99)).isEqualTo(100_000);
        result.publishScheduleLatency.reset();
        assertThat(one.publishScheduleLatency.getTotalCount()).isEqualTo(1);
        assertThat(two.publishScheduleLatency.getTotalCount()).isEqualTo(1);
    }

    @Test
    void zeroPlus() {
        CumulativeLatencies one = new CumulativeLatencies();
        CumulativeLatencies two = new CumulativeLatencies();
        CumulativeLatencies result = one.plus(two);
        assertThat(result)
                .satisfies(
                        r -> {
                            assertThat(r.publishLatency).isEqualTo(two.publishLatency);
                            assertThat(r.publishDelayLatency).isEqualTo(two.publishDelayLatency);
                            assertThat(r.endToEndLatency).isEqualTo(two.endToEndLatency);
                        });
    }
}
