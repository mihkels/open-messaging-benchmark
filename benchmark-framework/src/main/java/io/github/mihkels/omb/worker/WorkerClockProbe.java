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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator-side probing of worker clocks. Kept apart from {@link WorkerClock}, which the
 * isolated driver JVM loads, because probing needs {@link DistributedWorkersEnsemble}.
 */
public final class WorkerClockProbe {
    static final int ROUND_TRIPS = 5;
    static final String LOCAL = "local";

    private static final Logger log = LoggerFactory.getLogger(WorkerClockProbe.class);

    private WorkerClockProbe() {}

    /**
     * Estimates the clock offset of every worker. An in-process worker shares the coordinator's clock
     * and records zero; a remote worker that cannot be probed is logged and left out.
     *
     * @param worker the run's worker
     * @return one entry per probed worker, with no drift
     */
    public static List<WorkerClock> probe(Worker worker) {
        return probe(worker, WorkerClock::nowMicros);
    }

    static List<WorkerClock> probe(Worker worker, LongSupplier coordinatorMicros) {
        if (!(worker instanceof DistributedWorkersEnsemble ensemble)) {
            return List.of(new WorkerClock(worker.id(), LOCAL, 0, 0, 0.0));
        }
        List<WorkerClock> clocks = new ArrayList<>();
        for (Worker remote : ensemble.workers()) {
            probeOne(remote, ensemble.role(remote), coordinatorMicros).ifPresent(clocks::add);
        }
        return clocks;
    }

    private static Optional<WorkerClock> probeOne(
            Worker worker, String role, LongSupplier coordinatorMicros) {
        long bestRtt = Long.MAX_VALUE;
        long bestOffset = 0;
        for (int i = 0; i < ROUND_TRIPS; i++) {
            long sent = coordinatorMicros.getAsLong();
            @Nullable Long workerMicros;
            try {
                workerMicros = worker.health().clockMicros();
            } catch (RuntimeException e) {
                // Probing is best effort: a failure leaves the worker out, never fails the run
                log.atWarn()
                        .setMessage("Cannot probe the clock of worker {}: {}")
                        .addArgument(worker::id)
                        .addArgument(e::toString)
                        .log();
                return Optional.empty();
            }
            long received = coordinatorMicros.getAsLong();
            if (workerMicros == null) {
                log.atWarn()
                        .setMessage("Worker {} does not report its clock; upgrade it to measure clock offset")
                        .addArgument(worker::id)
                        .log();
                return Optional.empty();
            }
            long rtt = received - sent;
            if (rtt < bestRtt) {
                bestRtt = rtt;
                // Assumes the request and the response took equally long
                bestOffset = workerMicros - (sent + rtt / 2);
            }
        }
        return Optional.of(
                new WorkerClock(worker.id(), role, bestOffset / 1000.0, bestRtt / 1000.0, null));
    }
}
