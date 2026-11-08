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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkloadTest {

    @Test
    void acceptsAPlainWorkload() {
        Workload workload = new Workload();
        workload.messageSize = 1024;
        workload.producerRate = 1000;

        workload.validate();

        assertThat(workload.effectiveMessageSize()).isEqualTo(1024);
    }

    @Test
    void rejectsRateDiscoveryWhileBuildingABacklog() {
        Workload workload = new Workload();
        workload.messageSize = 1024;
        workload.consumerBacklogSizeGB = 1;

        assertThatThrownBy(workload::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot probe producer sustainable rate");
    }

    @Test
    void rejectsABacklogWithoutAMessageSize() {
        Workload workload = new Workload();
        workload.producerRate = 1000;
        workload.consumerBacklogSizeGB = 1;

        assertThatThrownBy(workload::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Message size must be positive");
    }

    @Test
    void rejectsAnEmptyRandomizedPayloadPool() {
        Workload workload = new Workload();
        workload.messageSize = 1024;
        workload.useRandomizedPayloads = true;

        assertThatThrownBy(workload::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Randomized payload pool size must be positive");

        workload.randomizedPayloadPoolSize = 1;
        workload.validate();
    }

    @Test
    void aSizeDistributionDeterminesTheEffectiveMessageSize() {
        Workload workload = new Workload();
        workload.messageSize = 1024;
        workload.messageSizeDistribution = Map.of("0-100", 1, "100-200", 1);

        workload.validate();

        // (50 * 1 + 150 * 1) / 2 = 100
        assertThat(workload.effectiveMessageSize()).isEqualTo(100);
    }

    @Test
    void rejectsAnInvalidSizeDistribution() {
        Workload workload = new Workload();
        workload.messageSizeDistribution = Map.of("not-a-range", 1);

        assertThatThrownBy(workload::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsThroughputRatioThresholdForRateDiscovery() {
        Workload workload = new Workload();
        workload.thresholds = new Thresholds();
        workload.thresholds.minThroughputRatio = 0.9;

        assertThatThrownBy(workload::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("minThroughputRatio requires a positive producerRate");
    }

    @Test
    void loadingAWorkloadFileWithoutANameFailsWithTheMissingKey(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("unnamed.yaml");
        Files.writeString(file, "topics: 1\nmessageSize: 1024\nproducerRate: 1000\n");

        List<String> paths = List.of(file.toString());

        assertThatThrownBy(() -> Benchmark.readWorkloads(paths))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid workload " + file + ": name is required");
    }

    @Test
    void loadingANamedWorkloadFileKeysItByFileName(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("named.yaml");
        Files.writeString(file, "name: example\ntopics: 1\nmessageSize: 1024\nproducerRate: 1000\n");

        assertThat(Benchmark.readWorkloads(List.of(file.toString())))
                .containsOnlyKeys("named")
                .extractingByKey("named")
                .satisfies(workload -> assertThat(workload.name).isEqualTo("example"));
    }
}
