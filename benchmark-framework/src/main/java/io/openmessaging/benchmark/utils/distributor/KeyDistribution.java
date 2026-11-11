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

/**
 * Parameters for a Zipfian key distribution.
 *
 * @param keys number of distinct keys
 * @param exponent non-negative skew exponent
 */
public record KeyDistribution(int keys, double exponent) {
    public static final KeyDistribution DEFAULT = new KeyDistribution(10_000, 0.99);
    public static final int MAX_KEYS = 1_000_000;

    public KeyDistribution {
        if (keys < 1 || keys > MAX_KEYS) {
            throw new IllegalArgumentException("Zipfian keys must be between 1 and " + MAX_KEYS);
        }
        if (!Double.isFinite(exponent) || exponent < 0) {
            throw new IllegalArgumentException("Zipfian exponent must be finite and non-negative");
        }
    }
}
