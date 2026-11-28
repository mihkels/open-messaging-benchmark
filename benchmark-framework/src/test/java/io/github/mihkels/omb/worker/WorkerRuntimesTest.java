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
package io.github.mihkels.omb.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.mihkels.omb.worker.commands.WorkerHealth;
import io.github.mihkels.omb.worker.commands.WorkerHealth.DriverState;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkerRuntimesTest {
    private static Worker reporting(String id, WorkerHealth health) {
        Worker worker = mock(Worker.class);
        when(worker.id()).thenReturn(id);
        when(worker.health()).thenReturn(health);
        return worker;
    }

    @Test
    void anInProcessWorkerIsJava() {
        assertThat(WorkerRuntimes.of(mock(Worker.class))).isEqualTo("java");
    }

    @Test
    void listsTheDistinctRuntimesOfRemoteWorkersAndTakesNoneAsJava() throws Exception {
        WorkerHealth running = new WorkerHealth(true, DriverState.RUNNING, null, null);
        Worker failing = mock(Worker.class);
        when(failing.id()).thenReturn("http://w4");
        when(failing.health()).thenThrow(new IllegalStateException("connection refused"));
        try (var ensemble =
                new DistributedWorkersEnsemble(
                        List.of(
                                reporting("http://w1", running.withRuntime("rust")),
                                reporting("http://w2", running),
                                reporting("http://w3", running.withRuntime("rust"))),
                        false)) {
            assertThat(WorkerRuntimes.of(ensemble)).isEqualTo("java,rust");
        }
        try (var ensemble =
                new DistributedWorkersEnsemble(
                        List.of(reporting("http://w1", running.withRuntime("rust")), failing), false)) {
            assertThat(WorkerRuntimes.of(ensemble)).isEqualTo("rust,unknown");
        }
    }
}
