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
package io.github.mihkels.omb.worker.commands;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Identity of a workload run, which a worker adds as {@code run_id} and {@code workload} tags to
 * the metrics of the next driver process it starts.
 *
 * @param runId the run id of the result
 * @param workload the workload name, or {@code null} when unknown (older coordinators send only the
 *     run id)
 */
public record RunContext(
        @JsonProperty("runId") String runId, @JsonProperty("workload") @Nullable String workload) {

    public RunContext {
        Objects.requireNonNull(runId, "runId");
    }
}
