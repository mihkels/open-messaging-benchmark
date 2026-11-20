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
package io.openmessaging.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.openmessaging.benchmark.worker.commands.PartitionPeriod;
import java.util.List;
import java.util.stream.IntStream;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

class PartitionSkewTest {

    @Test
    void evenTrafficHasNoSkew() {
        long[] even = {10, 10, 10, 10};
        assertThat(PartitionSkew.gini(even)).isCloseTo(0, within(1e-9));
        assertThat(PartitionSkew.maxMeanRatio(even)).isCloseTo(1, within(1e-9));
    }

    @Test
    void onePartitionTakingEverythingApproachesOne() {
        long[] skewed = {0, 0, 0, 100};
        // (n - 1) / n for one non-zero value among n
        assertThat(PartitionSkew.gini(skewed)).isCloseTo(0.75, within(1e-9));
        assertThat(PartitionSkew.maxMeanRatio(skewed)).isCloseTo(4, within(1e-9));
    }

    @Test
    void knownGiniValue() {
        // Sorted 1, 2, 3, 4: 2 * 30 / (4 * 10) - 5 / 4 = 0.25
        assertThat(PartitionSkew.gini(new long[] {4, 1, 3, 2})).isCloseTo(0.25, within(1e-9));
    }

    @Test
    void noTrafficGivesZeroRatherThanNaN() {
        assertThat(PartitionSkew.gini(new long[] {0, 0})).isZero();
        assertThat(PartitionSkew.maxMeanRatio(new long[] {0, 0})).isZero();
        assertThat(PartitionSkew.of(0, List.of())).isNull();
    }

    @Test
    void listsTheFivePartitionsWithTheHighestP99() {
        List<PartitionPeriod> partitions =
                IntStream.range(0, 8)
                        .mapToObj(
                                i -> new PartitionPeriod("t", i, 10, 0, latency((i + 1) * 1000L), new Histogram(1)))
                        .toList();

        PartitionSkew skew = PartitionSkew.of(3, partitions);

        assertThat(skew).isNotNull();
        assertThat(skew.period()).isEqualTo(3);
        assertThat(skew.partitions()).isEqualTo(8);
        assertThat(skew.topPublishP99())
                .extracting(PartitionSkew.PartitionLatency::partition)
                .containsExactly(7, 6, 5, 4, 3);
        assertThat(skew.topPublishP99().getFirst().p99Ms()).isCloseTo(8, within(1.0));
        // No partition consumed anything, so there is no end-to-end ranking
        assertThat(skew.topEndToEndP99()).isEmpty();
        assertThat(skew.consumeGini()).isZero();
    }

    private static Histogram latency(long micros) {
        Histogram histogram = new Histogram(1);
        histogram.recordValue(micros);
        return histogram;
    }
}
