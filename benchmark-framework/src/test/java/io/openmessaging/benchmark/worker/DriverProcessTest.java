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

import io.openmessaging.benchmark.driver.Failures;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class DriverProcessTest {

    @Test
    void driverLogFollowsTheSupervisorLogDirectory() {
        assertThat(DriverProcess.driverLogName(null)).isEqualTo("benchmark-driver");
        assertThat(DriverProcess.driverLogName("benchmark-worker")).isEqualTo("benchmark-driver");
        assertThat(DriverProcess.driverLogName("/tmp/benchmark-worker"))
                .isEqualTo(Path.of("/tmp", "benchmark-driver").toString());
    }

    @Test
    void reportsACrashAfterAConcurrentStopHasFinished() throws Exception {
        Process exited =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-version")
                        .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                        .redirectError(ProcessBuilder.Redirect.DISCARD)
                        .start();
        assertThat(exited.waitFor(30, TimeUnit.SECONDS)).isTrue();
        DriverProcess driverProcess = new DriverProcess("fixture driver", exited);

        // The interleaving of a call that fails while terminate() stops the same process
        Failures stopFailures = new Failures();
        driverProcess.stop(stopFailures);
        assertThat(driverProcess.isStopped()).isTrue();
        assertThat(driverProcess.pid()).isNull();

        IllegalStateException callFailure = new IllegalStateException("connection refused");
        assertThat(driverProcess.awaitExit(Duration.ofMillis(10))).isTrue();
        assertThat(driverProcess.crashed(callFailure))
                .hasMessage("Driver process for fixture driver exited with code 0")
                .hasCause(callFailure);
        assertThat(driverProcess.describeFailure(callFailure))
                .isEqualTo("connection refused (driver process exited with code 0)");
        assertThat(stopFailures.hasFailure()).isFalse();
    }
}
