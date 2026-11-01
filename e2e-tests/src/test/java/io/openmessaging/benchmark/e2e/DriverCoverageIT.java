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
package io.openmessaging.benchmark.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DriverCoverageIT extends BaseE2eIT {
    @TempDir Path root;

    @Test
    void reportsExecutedSkippedFailedAndMissingContracts() {
        String report =
                DriverCoverageExtension.render(
                        Set.of("kafka", "jms", "new-driver", "redis"),
                        Map.of(
                                "kafka", Map.of("KafkaE2eIT#contract", "PASSED"),
                                "jms", Map.of("JmsKafkaOptionalE2eIT#contract", "SKIPPED"),
                                "redis", Map.of("RedisE2eIT#contract", "FAILED"),
                                "ghost", Map.of("GhostE2eIT#contract", "PASSED")),
                        "KafkaE2eIT");

        assertThat(report)
                .contains("Selection: KafkaE2eIT")
                .contains("kafka | PASSED | KafkaE2eIT#contract=PASSED")
                .contains("jms | SKIPPED | JmsKafkaOptionalE2eIT#contract=SKIPPED")
                .contains("redis | FAILED | RedisE2eIT#contract=FAILED")
                .contains("new-driver | NOT_EXECUTED")
                .contains("ghost | UNKNOWN_PLUGIN | GhostE2eIT#contract=PASSED");
    }

    @Test
    void discoversShippedPluginIdsAndRejectsDuplicateIds() throws Exception {
        Path first = root.resolve("driver-one/src/main/plugin/plugin.yaml");
        Path second = root.resolve("driver-two/src/main/plugin/plugin.yaml");
        Files.createDirectories(first.getParent());
        Files.createDirectories(second.getParent());
        Files.writeString(first, "id: one\n");
        Files.writeString(second, "id: two\n");
        assertThat(DriverCoverageExtension.discoverPluginIds(root)).containsExactly("one", "two");

        Files.writeString(second, "id: one\n");
        assertThatThrownBy(() -> DriverCoverageExtension.discoverPluginIds(root))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate plugin id");
    }
}
