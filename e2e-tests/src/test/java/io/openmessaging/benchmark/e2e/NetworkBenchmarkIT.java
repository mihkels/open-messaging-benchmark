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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;

class NetworkBenchmarkIT extends BaseE2eIT {
    @Test
    void rejectsAFailedSmokeRunAndKeepsItsLog() throws Exception {
        String invalidDriver =
                """
                name: Missing driver
                driverClass: example.missing.NonexistentDriver
                """;
        try (Network network = Network.newNetwork();
                NetworkBenchmark runner = new NetworkBenchmark(network, tempDir)) {
            runner.start();
            assertThatThrownBy(() -> runner.run(invalidDriver, createSimpleWorkload()))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("NonexistentDriver");
            try (var runs = Files.list(tempDir)) {
                var paths = runs.toList();
                assertThat(paths).hasSize(1);
                assertThat(Files.readString(paths.getFirst().resolve("benchmark.log")))
                        .contains("NonexistentDriver");
            }
        }
    }

    @Test
    void rejectsNonzeroExitEvenWithoutFailureLog() {
        assertThatThrownBy(() -> NetworkBenchmark.requireSuccessfulRun(124, "timed out"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("exit code 124");
    }

    @Test
    void retainsAResultProducedBeforeAReportedFailure() throws Exception {
        var artifacts = tempDir.resolve("runs");
        try (Network network = Network.newNetwork();
                NetworkBenchmark runner = new NetworkBenchmark(network, artifacts)) {
            runner.start();
            assertThatThrownBy(
                            () -> runner.run("name: fake\n", createSimpleWorkload(), FailingSmoke.class))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("Failed to run the workload");
            try (var runs = Files.list(artifacts)) {
                var paths = runs.toList();
                assertThat(paths).hasSize(1);
                assertThat(Files.readString(paths.getFirst().resolve("benchmark.log")))
                        .contains("Failed to run the workload");
                assertThat(Files.readString(paths.getFirst().resolve("result.json")))
                        .isEqualTo("{\"partial\":true}");
            }
        }
    }

    public static final class FailingSmoke {
        public static void main(String[] args) throws Exception {
            Files.writeString(Path.of(args[2]), "{\"partial\":true}");
            System.out.println("Failed to run the workload");
        }
    }
}
