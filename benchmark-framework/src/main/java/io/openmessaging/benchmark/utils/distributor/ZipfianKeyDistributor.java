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

import java.util.concurrent.ThreadLocalRandom;

/** Immutable alias table shared by producers on a worker. */
public final class ZipfianKeyDistributor extends KeyDistributor {
    private final String[] keys;
    private final double[] probability;
    private final int[] alias;

    public ZipfianKeyDistributor(KeyDistribution distribution) {
        int size = distribution.keys();
        keys = new String[size];
        probability = new double[size];
        alias = new int[size];
        double sum = 0;
        for (int i = 0; i < size; i++) {
            keys[i] = "zipf-" + i;
            probability[i] = Math.pow(i + 1.0, -distribution.exponent());
            sum += probability[i];
        }
        int[] small = new int[size];
        int[] large = new int[size];
        int smallCount = 0;
        int largeCount = 0;
        for (int i = 0; i < size; i++) {
            probability[i] = probability[i] / sum * size;
            if (probability[i] < 1) {
                small[smallCount++] = i;
            } else {
                large[largeCount++] = i;
            }
        }
        while (smallCount > 0 && largeCount > 0) {
            int low = small[--smallCount];
            int high = large[--largeCount];
            alias[low] = high;
            probability[high] = (probability[high] + probability[low]) - 1;
            if (probability[high] < 1) {
                small[smallCount++] = high;
            } else {
                large[largeCount++] = high;
            }
        }
        while (largeCount > 0) {
            probability[large[--largeCount]] = 1;
        }
        while (smallCount > 0) {
            probability[small[--smallCount]] = 1;
        }
    }

    @Override
    public String next() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int column = random.nextInt(keys.length);
        return keys[random.nextDouble() < probability[column] ? column : alias[column]];
    }
}
