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

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ResultFileNameTest {

    @Test
    void explicitOutputWinsOverResultsDirectory() {
        assertThat(Benchmark.resultFileName("out.json", "workload", "Kafka", "/results"))
                .isEqualTo("out.json");
    }

    @Test
    void generatedNameIsRelativeWithoutResultsDirectory() {
        assertThat(Benchmark.resultFileName(null, "workload", "Kafka", null))
                .matches("workload-Kafka-\\d{4}(-\\d{2}){5}\\.json");
        assertThat(Benchmark.resultFileName("", "workload", "Kafka", ""))
                .matches("workload-Kafka-\\d{4}(-\\d{2}){5}\\.json");
    }

    @Test
    void generatedNameIsPlacedInResultsDirectory() {
        Path file = Path.of(Benchmark.resultFileName(null, "workload", "Kafka", "/results"));

        assertThat(file).hasParentRaw(Path.of("/results"));
        assertThat(file.getFileName().toString()).startsWith("workload-Kafka-").endsWith(".json");
    }
}
