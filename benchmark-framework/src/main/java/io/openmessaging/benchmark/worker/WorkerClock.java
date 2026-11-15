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

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One worker's wall-clock offset from the coordinator, estimated NTP-style from the clock reading
 * in the worker's {@code GET /health} response. Each probe makes {@value #ROUND_TRIPS} round trips
 * and keeps the one with the lowest round-trip time; the true offset is within {@code rttMs / 2} of
 * the estimate.
 *
 * @param worker the worker's id
 * @param role {@code producer}, {@code consumer}, or {@code local} for an in-process run
 * @param offsetMs the worker's clock minus the coordinator's at worker setup, in milliseconds
 * @param rttMs the round-trip time of the sample the offset comes from, in milliseconds
 * @param driftMs how much the offset changed between setup and the end of the run, in milliseconds,
 *     or {@code null} when the end probe failed
 */
public record WorkerClock(
        String worker,
        String role,
        double offsetMs,
        double rttMs,
        @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable Double driftMs) {
    static final int ROUND_TRIPS = 5;
    static final String LOCAL = "local";

    private static final Logger log = LoggerFactory.getLogger(WorkerClock.class);

    /**
     * The current wall clock in microseconds since the epoch.
     *
     * @return the time
     */
    public static long nowMicros() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000;
    }

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
            WorkerClock clock = probeOne(remote, ensemble.role(remote), coordinatorMicros);
            if (clock != null) {
                clocks.add(clock);
            }
        }
        return clocks;
    }

    private static @Nullable WorkerClock probeOne(
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
                return null;
            }
            long received = coordinatorMicros.getAsLong();
            if (workerMicros == null) {
                log.atWarn()
                        .setMessage("Worker {} does not report its clock; upgrade it to measure clock offset")
                        .addArgument(worker::id)
                        .log();
                return null;
            }
            long rtt = received - sent;
            if (rtt < bestRtt) {
                bestRtt = rtt;
                // Assumes the request and the response took equally long
                bestOffset = workerMicros - (sent + rtt / 2);
            }
        }
        return new WorkerClock(worker.id(), role, bestOffset / 1000.0, bestRtt / 1000.0, null);
    }

    /**
     * Adds each worker's drift, the change from its offset at setup to its offset at the end.
     *
     * @param start the offsets probed at setup
     * @param end the offsets probed at the end of the run
     * @return the setup offsets with their drift; null drift for a worker missing at the end
     */
    public static List<WorkerClock> withDrift(List<WorkerClock> start, List<WorkerClock> end) {
        return start.stream()
                .map(s ->
                        end.stream()
                                .filter(e -> Objects.equals(e.worker, s.worker))
                                .findFirst()
                                .map(e ->
                                        new WorkerClock(s.worker, s.role, s.offsetMs, s.rttMs, e.offsetMs - s.offsetMs))
                                .orElse(s))
                .toList();
    }

    /**
     * The largest offset difference between a producer and a consumer worker, at setup or at the end
     * of the run, which end-to-end latency absorbs.
     *
     * @param clocks the workers' offsets
     * @return the spread in milliseconds; zero without both a producer and a consumer worker
     */
    public static double spreadMs(List<WorkerClock> clocks) {
        return Math.max(spreadMs(clocks, false), spreadMs(clocks, true));
    }

    private static double spreadMs(List<WorkerClock> clocks, boolean atEnd) {
        double minProducer = Double.POSITIVE_INFINITY;
        double maxProducer = Double.NEGATIVE_INFINITY;
        double minConsumer = Double.POSITIVE_INFINITY;
        double maxConsumer = Double.NEGATIVE_INFINITY;
        for (WorkerClock clock : clocks) {
            double offset = clock.offsetMs + (atEnd && clock.driftMs != null ? clock.driftMs : 0);
            if ("producer".equals(clock.role)) {
                minProducer = Math.min(minProducer, offset);
                maxProducer = Math.max(maxProducer, offset);
            } else if ("consumer".equals(clock.role)) {
                minConsumer = Math.min(minConsumer, offset);
                maxConsumer = Math.max(maxConsumer, offset);
            }
        }
        if (minProducer == Double.POSITIVE_INFINITY || minConsumer == Double.POSITIVE_INFINITY) {
            return 0;
        }
        return Math.max(maxConsumer - minProducer, maxProducer - minConsumer);
    }
}
