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

import io.micrometer.core.instrument.Tags;
import java.util.List;
import org.junit.jupiter.api.Test;

class DriverProcessMainTest {

    @Test
    void parsesTagArguments() {
        assertThat(DriverProcessMain.commonTags(List.of("run_id=r-1", "driver=Kafka", "empty=")))
                .isEqualTo(Tags.of("run_id", "r-1", "driver", "Kafka", "empty", ""));
    }

    @Test
    void rejectsTagsWithoutKey() {
        assertThatThrownBy(() -> DriverProcessMain.commonTags(List.of("=value")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DriverProcessMain.commonTags(List.of("novalue")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
