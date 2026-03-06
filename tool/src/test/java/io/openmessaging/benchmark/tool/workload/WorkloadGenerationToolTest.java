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
package io.openmessaging.benchmark.tool.workload;

import static org.assertj.core.api.Assertions.assertThat;

import io.openmessaging.benchmark.Workload;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;

// TODO: Review this class for test coverage and edge cases
class WorkloadGenerationToolTest {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    @TempDir
    Path tempDir;

    @Test
    void generatesOneFilePerWorkloadCombination() throws Exception {
        Path templateFile = tempDir.resolve("template.yaml");
        Path outputDir = tempDir.resolve("out");
        Files.createDirectories(outputDir);

        Files.writeString(
                templateFile,
                """
                nameFormat: "workload-${topics}-${messageSize}"
                topics: [1, 2]
                partitionsPerTopic: [1]
                messageSize: [128, 256]
                subscriptionsPerTopic: [1]
                producersPerTopic: [1]
                consumerPerSubscription: [1]
                producerRate: [100]
                keyDistributor: NO_KEY
                useRandomizedPayloads: false
                randomBytesRatio: 0.0
                randomizedPayloadPoolSize: 0
                consumerBacklogSizeGB: 0
                testDurationMinutes: 1
                warmupDurationMinutes: 0
                """);

        WorkloadGenerationTool.main(
                new String[] {
                    "-t", templateFile.toString(),
                    "-o", outputDir.toString()
                });

        List<String> generatedFiles =
                Files.list(outputDir)
                        .map(path -> path.getFileName().toString())
                        .sorted()
                        .toList();

        assertThat(generatedFiles)
                .containsExactly(
                        "workload-1-128.yaml",
                        "workload-1-256.yaml",
                        "workload-2-128.yaml",
                        "workload-2-256.yaml");
    }

    @Test
    void writesGeneratedWorkloadsAsReadableYamlFiles() throws Exception {
        Path templateFile = tempDir.resolve("template.yaml");
        Path outputDir = tempDir.resolve("out");
        Files.createDirectories(outputDir);

        Files.writeString(
                templateFile,
                """
                nameFormat: "single-workload"
                topics: [3]
                partitionsPerTopic: [2]
                messageSize: [1024]
                subscriptionsPerTopic: [4]
                producersPerTopic: [5]
                consumerPerSubscription: [6]
                producerRate: [700]
                keyDistributor: NO_KEY
                useRandomizedPayloads: true
                randomBytesRatio: 0.5
                randomizedPayloadPoolSize: 10
                consumerBacklogSizeGB: 7
                testDurationMinutes: 8
                warmupDurationMinutes: 9
                """);

        WorkloadGenerationTool.main(
                new String[] {
                    "-t", templateFile.toString(),
                    "-o", outputDir.toString()
                });

        Path generatedFile = outputDir.resolve("single-workload.yaml");
        assertThat(generatedFile).exists();

        String yaml = Files.readString(generatedFile);
        assertThat(yaml).isNotBlank();
        assertThat(yaml).contains("name: \"single-workload\"");
        assertThat(yaml).contains("topics: 3");
        assertThat(yaml).contains("partitionsPerTopic: 2");
        assertThat(yaml).contains("messageSize: 1024");

        Workload workload = YAML_MAPPER.readValue(generatedFile.toFile(), Workload.class);

        assertThat(workload.name).isEqualTo("single-workload");
        assertThat(workload.topics).isEqualTo(3);
        assertThat(workload.partitionsPerTopic).isEqualTo(2);
        assertThat(workload.messageSize).isEqualTo(1024);
        assertThat(workload.subscriptionsPerTopic).isEqualTo(4);
        assertThat(workload.producersPerTopic).isEqualTo(5);
        assertThat(workload.consumerPerSubscription).isEqualTo(6);
        assertThat(workload.producerRate).isEqualTo(700);
        assertThat(workload.useRandomizedPayloads).isTrue();
        assertThat(workload.randomBytesRatio).isEqualTo(0.5);
        assertThat(workload.randomizedPayloadPoolSize).isEqualTo(10);
        assertThat(workload.consumerBacklogSizeGB).isEqualTo(7);
        assertThat(workload.testDurationMinutes).isEqualTo(8);
        assertThat(workload.warmupDurationMinutes).isEqualTo(9);
    }
}
