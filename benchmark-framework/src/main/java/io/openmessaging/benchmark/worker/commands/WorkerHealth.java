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
package io.openmessaging.benchmark.worker.commands;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/**
 * What a worker's {@code GET /health} reports.
 *
 * @param ready whether the worker can take a driver; false once it is closed or its cleanup failed
 * @param driver the state of the driver process
 * @param exitCode the driver process's exit code once it has exited, else {@code null}
 * @param lastError the last driver failure, or {@code null}; only sent to callers with the token
 * @param clockMicros the worker's wall clock in microseconds since the epoch, sampled while
 *     answering the request, or {@code null} when not reported (in-process or older workers)
 */
public record WorkerHealth(
        @JsonProperty("ready") boolean ready,
        @JsonProperty("driver") DriverState driver,
        @JsonProperty("exitCode") @Nullable Integer exitCode,
        @JsonProperty("lastError") @Nullable String lastError,
        @JsonProperty("clockMicros") @JsonInclude(JsonInclude.Include.NON_NULL)
                @Nullable Long clockMicros) {

    /** Health without a clock reading. */
    public WorkerHealth(
            boolean ready, DriverState driver, @Nullable Integer exitCode, @Nullable String lastError) {
        this(ready, driver, exitCode, lastError, null);
    }

    /** Health of a worker whose driver runs in-process and is always available. */
    public static final WorkerHealth RUNNING =
            new WorkerHealth(true, DriverState.RUNNING, null, null);

    /** State of a worker's driver process. */
    public enum DriverState {
        NOT_STARTED,
        INITIALIZING,
        RUNNING,
        EXITED
    }

    /**
     * This health without {@link #lastError()}, for callers without the bearer token.
     *
     * @return the redacted health
     */
    public WorkerHealth withoutError() {
        return new WorkerHealth(ready, driver, exitCode, null, clockMicros);
    }

    /**
     * This health with the given wall-clock reading.
     *
     * @param micros the wall clock in microseconds since the epoch
     * @return the health with {@link #clockMicros()} set
     */
    public WorkerHealth withClockMicros(long micros) {
        return new WorkerHealth(ready, driver, exitCode, lastError, micros);
    }
}
