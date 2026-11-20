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
package io.github.mihkels.omb.utils.distributor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RandomNanoTest {
    @Test
    void spreadsKeysAcrossTheWholeRange() {
        KeyDistributor distributor = KeyDistributor.build(KeyDistributorType.RANDOM_NANO);
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < 100_000; i++) {
            keys.add(distributor.next());
        }
        // 100k uniform draws over 10,000 keys leave almost none unused
        assertThat(keys).hasSizeGreaterThan(9_900);
    }
}
