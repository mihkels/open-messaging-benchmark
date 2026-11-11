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
package io.openmessaging.benchmark.utils.distributor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ZipfianKeyDistributorTest {
    @Test
    void samplesExpectedTopShares() {
        int keys = 1000;
        int samples = 1_000_000;
        double exponent = 0.99;
        KeyDistributor distributor =
                KeyDistributor.build(KeyDistributorType.ZIPFIAN, new KeyDistribution(keys, exponent));
        int[] counts = new int[keys];
        for (int i = 0; i < samples; i++) {
            String key = distributor.next();
            int rank = Integer.parseInt(key.substring("zipf-".length()));
            assertThat(rank).isBetween(0, keys - 1);
            counts[rank]++;
        }
        double total = 0;
        double top = 0;
        int observed = 0;
        for (int i = 0; i < keys; i++) {
            double weight = Math.pow(i + 1.0, -exponent);
            total += weight;
            if (i < 10) {
                top += weight;
                observed += counts[i];
            }
        }
        assertThat((double) observed / samples).isCloseTo(top / total, org.assertj.core.data.Offset.offset(0.005));
    }

    @Test
    void supportsUniformAndSingleKeyDistributions() {
        KeyDistributor single = KeyDistributor.build(KeyDistributorType.ZIPFIAN, new KeyDistribution(1, 1));
        assertThat(single.next()).isEqualTo("zipf-0");
        KeyDistributor uniform = KeyDistributor.build(KeyDistributorType.ZIPFIAN, new KeyDistribution(2, 0));
        int first = 0;
        for (int i = 0; i < 100_000; i++) {
            if (uniform.next().equals("zipf-0")) {
                first++;
            }
        }
        assertThat(first).isBetween(49_000, 51_000);
    }

    @Test
    void rejectsInvalidParameters() {
        assertThatThrownBy(() -> new KeyDistribution(0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KeyDistribution(KeyDistribution.MAX_KEYS + 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        for (double exponent : new double[] {-1, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThatThrownBy(() -> new KeyDistribution(10, exponent))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
