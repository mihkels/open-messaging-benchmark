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
package io.github.mihkels.omb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkArgumentsTest {

    @Test
    void workerCaFileComesFromTheOptionThenTheEnvironment() {
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        assertThat(Benchmark.workerCaFile(arguments, Map.of())).isNull();
        assertThat(Benchmark.workerCaFile(arguments, Map.of("OMB_WORKER_CA_FILE", " "))).isNull();
        assertThat(Benchmark.workerCaFile(arguments, Map.of("OMB_WORKER_CA_FILE", "/tls/env.crt\n")))
                .isEqualTo(Path.of("/tls/env.crt"));
        arguments.workerCaFile = Path.of("/tls/flag.crt");
        assertThat(Benchmark.workerCaFile(arguments, Map.of("OMB_WORKER_CA_FILE", "/tls/env.crt")))
                .isEqualTo(Path.of("/tls/flag.crt"));
    }

    @Test
    void httpsWorkersNeedTheirCertificate() {
        List<String> https = List.of("http://w1:8080", "HTTPS://w2:8080");
        assertThat(Benchmark.missingWorkerCa(https, null)).contains("--worker-ca-file");
        assertThat(Benchmark.missingWorkerCa(https, Path.of("ca.crt"))).isNull();
        assertThat(Benchmark.missingWorkerCa(List.of("http://w1:8080"), null)).isNull();
    }

    @Test
    void discoveredWorkersUseHttpsWhenACertificateIsConfigured() {
        List<String> resolved = List.of("http://10.0.0.1:8080", "http://[::1]:8080");
        assertThat(Benchmark.discoveredUrls(resolved, null)).isEqualTo(resolved);
        assertThat(Benchmark.discoveredUrls(resolved, Path.of("ca.crt")))
                .containsExactly("https://10.0.0.1:8080", "https://[::1]:8080");
    }

    @Test
    void rejectsOutputWhenCombinationsWouldOverwriteIt() {
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.workloads = List.of("w1.yaml");
        arguments.drivers = List.of("a.yaml");
        arguments.output = "r.json";
        assertThat(Benchmark.usageError(arguments)).isNull();

        arguments.drivers = List.of("a.yaml", "b.yaml");
        assertThat(Benchmark.usageError(arguments))
                .startsWith("--output can only be used with a single workload and driver");

        arguments.output = null;
        assertThat(Benchmark.usageError(arguments)).isNull();
    }

    @Test
    void requiresWorkloadsAndDrivers() {
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        assertThat(Benchmark.usageError(arguments)).isEqualTo("At least one workload file is required");

        arguments.workloads = List.of("w1.yaml");
        assertThat(Benchmark.usageError(arguments))
                .isEqualTo("At least one driver must be given with --drivers");
    }

    @Test
    void requiresAtLeastTwoExpectedWorkers() {
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.workloads = List.of("w1.yaml");
        arguments.drivers = List.of("a.yaml");
        arguments.expectedWorkers = 1;
        assertThat(Benchmark.usageError(arguments)).startsWith("--expected-workers must be at least 2");
    }

    @Test
    void rejectsNegativeRetries() {
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.workloads = List.of("w1.yaml");
        arguments.drivers = List.of("a.yaml");
        assertThat(arguments.retries).isEqualTo(3);
        arguments.retries = -1;
        assertThat(Benchmark.usageError(arguments)).isEqualTo("--retries must be 0 or more");
    }

    @Test
    void rejectsRepeatBelowOne() {
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.workloads = List.of("w1.yaml");
        arguments.drivers = List.of("a.yaml");
        arguments.repeat = 1;
        assertThat(Benchmark.usageError(arguments)).isNull();
        arguments.repeat = 0;
        assertThat(Benchmark.usageError(arguments)).isEqualTo("--repeat must be at least 1");
    }
}
