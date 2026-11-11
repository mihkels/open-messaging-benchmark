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

import io.openmessaging.benchmark.utils.payload.PayloadGenerator;
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

    @Test
    void loadsAStagedWorkloadAndMeasuresForTheSumOfItsStages(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("ramp.yaml");
        Files.writeString(
                file,
                """
                name: ramp
                topics: 1
                messageSize: 1024
                stages:
                  - durationMinutes: 5
                    targetRate: 50000
                  - durationMinutes: 10
                    targetRate: 150000
                  - durationMinutes: 0
                    targetRate: 200000
                """);

        Workload workload = Benchmark.readWorkloads(List.of(file.toString())).get("ramp");
        workload.validate();

        assertThat(workload.stages)
                .containsExactly(
                        new Workload.Stage(5, 50000),
                        new Workload.Stage(10, 150000),
                        new Workload.Stage(0, 200000));
        assertThat(workload.measuredMinutes()).isEqualTo(15);
    }

    @Test
    void loadsAWorkloadWithAPayloadGenerator(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("compressible.yaml");
        Files.writeString(
                file,
                """
                name: compressible
                messageSize: 1024
                payloadGenerator:
                  type: COMPRESSIBLE
                  targetCompressionRatio: 0.45
                  poolSize: 1000
                """);

        Workload workload = Benchmark.readWorkloads(List.of(file.toString())).get("compressible");
        workload.validate();

        assertThat(workload.payloadGenerator)
                .isEqualTo(new PayloadGenerator(PayloadGenerator.Type.COMPRESSIBLE, 0.45, 1000));
    }

    @Test
    void loadingAPayloadGeneratorWithAnInvalidRatioFails(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("invalid.yaml");
        Files.writeString(
                file,
                """
                name: invalid
                messageSize: 1024
                payloadGenerator:
                  type: COMPRESSIBLE
                  targetCompressionRatio: 1.5
                  poolSize: 10
                """);

        assertThatThrownBy(() -> Benchmark.readWorkloads(List.of(file.toString())))
                .hasStackTraceContaining("payloadGenerator targetCompressionRatio must be in (0, 1]");
    }

    @Test
    void rejectsAPayloadGeneratorMixedWithIncompatibleSettings() {
        assertRejectedGenerator(
                w -> w.payloadFile = "payload/payload-1Kb.data",
                "payloadGenerator and payloadFile are mutually exclusive");
        assertRejectedGenerator(
                w -> w.useRandomizedPayloads = true,
                "payloadGenerator and useRandomizedPayloads are mutually exclusive");
        assertRejectedGenerator(
                w -> w.messageSize = 0, "Message size must be positive with payloadGenerator");
        assertRejectedGenerator(
                w -> w.messageSizeDistribution = Map.of("1KB-1KB", Integer.MAX_VALUE / 2),
                "payloadGenerator poolSize times the distribution weights exceeds the supported total");
    }

    private static void assertRejectedGenerator(
            java.util.function.Consumer<Workload> change, String message) {
        Workload workload = new Workload();
        workload.messageSize = 1024;
        workload.producerRate = 1000;
        workload.payloadGenerator = new PayloadGenerator(PayloadGenerator.Type.COMPRESSIBLE, 0.5, 10);
        workload.validate();
        change.accept(workload);

        assertThatThrownBy(workload::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    @Test
    void rejectsStagesMixedWithIncompatibleSettings() {
        assertRejectedStages(w -> w.producerRate = 1000, "stages and producerRate are mutually exclusive");
        assertRejectedStages(
                w -> w.testDurationMinutes = 5, "stages and testDurationMinutes are mutually exclusive");
        assertRejectedStages(
                w -> w.consumerBacklogSizeGB = 1, "stages cannot be combined with a consumer backlog");
        assertRejectedStages(
                w -> w.stages = List.of(new Workload.Stage(1, 0)), "Stage targetRate must be positive");
        assertRejectedStages(
                w -> w.stages = List.of(new Workload.Stage(-1, 10), new Workload.Stage(2, 10)),
                "Stage durationMinutes must not be negative");
        assertRejectedStages(
                w -> w.stages = List.of(new Workload.Stage(0, 10)),
                "stages must last at least one minute in total");
    }

    private static void assertRejectedStages(
            java.util.function.Consumer<Workload> change, String message) {
        Workload workload = new Workload();
        workload.messageSize = 1024;
        workload.stages = List.of(new Workload.Stage(1, 1000));
        workload.validate();
        change.accept(workload);

        assertThatThrownBy(workload::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }
}
