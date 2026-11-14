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

/**
 * A worker failed its health check during a run. The run itself may be retried: the worker that
 * failed is named so it can be replaced or restarted.
 */
public final class WorkerUnhealthyException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    private final String workerId;
    private final String reason;

    /**
     * Creates the failure.
     *
     * @param workerId the id of the unhealthy worker, such as its URL
     * @param reason why it is unhealthy
     */
    public WorkerUnhealthyException(String workerId, String reason) {
        super("Worker " + workerId + " is unhealthy: " + reason);
        this.workerId = workerId;
        this.reason = reason;
    }

    public String workerId() {
        return workerId;
    }

    public String reason() {
        return reason;
    }
}
