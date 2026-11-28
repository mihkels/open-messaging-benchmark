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

import io.github.mihkels.omb.worker.commands.WorkerHealth;
import java.util.SortedSet;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Names the worker implementations of a run, so results from different client stacks are never
 * compared without the reader knowing (ADR 0021).
 */
public final class WorkerRuntimes {
    /** The runtime recorded when a remote worker cannot be asked. */
    static final String UNKNOWN = "unknown";

    private static final Logger log = LoggerFactory.getLogger(WorkerRuntimes.class);

    private WorkerRuntimes() {}

    /**
     * Asks every remote worker for its {@code runtime} on {@code /health}. A worker that does not
     * report one is a Java worker from before the field; in-process and local driver process workers
     * are Java.
     *
     * @param worker the run's worker
     * @return the distinct runtimes, sorted and comma-separated, such as {@code java} or {@code
     *     java,rust}
     */
    public static String of(Worker worker) {
        if (!(worker instanceof DistributedWorkersEnsemble ensemble)) {
            return WorkerHealth.JAVA_RUNTIME;
        }
        SortedSet<String> runtimes = new TreeSet<>();
        for (Worker remote : ensemble.workers()) {
            try {
                String runtime = remote.health().runtime();
                runtimes.add(runtime == null ? WorkerHealth.JAVA_RUNTIME : runtime);
            } catch (RuntimeException e) {
                // Best effort, like the clock probe: the run is not failed for a label
                log.atWarn()
                        .setMessage("Cannot ask worker {} for its runtime: {}")
                        .addArgument(remote::id)
                        .addArgument(e::toString)
                        .log();
                runtimes.add(UNKNOWN);
            }
        }
        return String.join(",", runtimes);
    }
}
