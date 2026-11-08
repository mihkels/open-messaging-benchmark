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
package io.openmessaging.benchmark.tool.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.openmessaging.benchmark.tool.report.RunContext.SettingDifference;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class RunContextTest {

    @Test
    void runsOfTheSameWorkloadMatch() {
        List<Run> runs =
                RunContext.all(Fixtures.runs("baseline"), Fixtures.runs("candidate-regression"));
        assertThat(runs).hasSize(6);
        assertThat(RunContext.workloadMismatches(runs)).isEmpty();
        assertThat(RunContext.workloadName(runs)).isEqualTo("kafka-1-topic");
    }

    @Test
    void reportsEachMismatchedWorkloadSettingWithItsFiles() {
        List<Run> runs =
                List.of(
                        Fixtures.run("baseline-1.json"),
                        Fixtures.run("baseline-2.json"),
                        Fixtures.run("mismatched-workload.json"));
        assertThat(RunContext.workloadMismatches(runs))
                .containsExactly(
                        "Workload producerRate differs: 10000 (baseline-1.json, baseline-2.json)"
                                + " vs 20000 (mismatched-workload.json)",
                        "Workload messageSize differs: 1024 (baseline-1.json, baseline-2.json)"
                                + " vs 2048 (mismatched-workload.json)");
    }

    @Test
    void comparesTheStoredSettingsOfVersion1Files() {
        Run legacy = Fixtures.run("legacy-v1.json");
        assertThat(RunContext.workloadIdentity(legacy))
                .containsExactly(
                        Map.entry("name", "kafka-1-topic"),
                        Map.entry("messageSize", "1024"),
                        Map.entry("partitionsPerTopic", "16"));
        assertThat(RunContext.workloadMismatches(List.of(legacy, Fixtures.run("baseline-1.json"))))
                .isEmpty();
    }

    @Test
    void namesEveryWorkloadOrUnknown() {
        assertThat(RunContext.workloadName(List.of(Fixtures.run("baseline-1.json"), renamed("other"))))
                .isEqualTo("kafka-1-topic, other");
        assertThat(RunContext.workloadName(List.of())).isEqualTo("unknown");
    }

    @Test
    void listsTheEnvironmentSettingsThatDifferBetweenSides() {
        List<SettingDifference> differences =
                RunContext.settingDifferences(
                        Fixtures.runs("baseline"), Fixtures.runs("candidate-regression"));
        assertThat(differences)
                .containsExactly(
                        new SettingDifference("jdk", Set.of("27"), Set.of("27.0.1")),
                        new SettingDifference("driverConfig.producerConfig.acks", Set.of("all"), Set.of("1")));
        assertThat(RunContext.settingDifferences(Fixtures.runs("baseline"), Fixtures.runs("baseline")))
                .isEmpty();
    }

    @Test
    void marksSettingsThatOnlyOneSideHas() {
        List<SettingDifference> differences =
                RunContext.settingDifferences(
                        List.of(Fixtures.run("legacy-v1.json")), List.of(Fixtures.run("baseline-1.json")));
        assertThat(differences)
                .first()
                .isEqualTo(new SettingDifference("workerCount", Set.of("(not set)"), Set.of("2")));
        assertThat(
                        RunContext.settingDifferences(
                                List.of(Fixtures.run("baseline-1.json")), List.of(Fixtures.run("legacy-v1.json"))))
                .first()
                .isEqualTo(new SettingDifference("workerCount", Set.of("2"), Set.of("(not set)")));
    }

    @Test
    void flattensNestedMapsIntoDottedKeys() {
        Map<String, String> flattened = new TreeMap<>();
        RunContext.flatten("driverConfig", Map.of("a", 1, "b", Map.of("c", "x")), flattened);
        assertThat(flattened)
                .containsExactly(Map.entry("driverConfig.a", "1"), Map.entry("driverConfig.b.c", "x"));
    }

    private static Run renamed(String name) {
        Run run = Fixtures.run("legacy-v1.json");
        run.result().workload = name;
        return run;
    }
}
