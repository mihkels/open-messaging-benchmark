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

import io.openmessaging.benchmark.worker.commands.WorkerHealth;
import io.openmessaging.benchmark.worker.commands.WorkerHealth.DriverState;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches a workload run from a virtual thread. Every health interval it polls each worker's {@link
 * Worker#health()}; a worker whose driver is not running, or that cannot be reached {@value
 * #MAX_MISSES} times in a row, fails the run with a {@link WorkerUnhealthyException}. When the run
 * makes no progress for the progress timeout, it logs the coordinator's thread dump, so the call
 * the run is blocked on is visible.
 *
 * <p>Use from the run thread, in a try-with-resources around the run:
 *
 * <pre>{@code
 * try (WorkerWatchdog watchdog = WorkerWatchdog.watch(worker, progress)) {
 *     try {
 *         return run();
 *     } catch (Exception e) {
 *         throw watchdog.explain(e);
 *     }
 * }
 * }</pre>
 */
public final class WorkerWatchdog implements AutoCloseable {
    static final int MAX_MISSES = 3;

    private static final Logger log = LoggerFactory.getLogger(WorkerWatchdog.class);

    private final List<Worker> workers;
    private final @Nullable DistributedWorkersEnsemble ensemble;
    private final Thread runThread;
    private final LongSupplier progress;
    private final long intervalNanos;
    private final long progressTimeoutNanos;
    private final Map<Worker, Integer> misses = new HashMap<>();
    private final Thread poller;

    private final Object lock = new Object();
    // Guarded by lock
    private boolean closed;
    private boolean interruptedRun;
    private boolean reported;
    private @Nullable WorkerUnhealthyException failure;

    /**
     * Starts watching a run on the calling thread, with the intervals of {@link WorkerTimeouts}.
     *
     * @param worker the run's worker; each worker of a {@link DistributedWorkersEnsemble} is polled
     * @param progress a value that changes whenever the run makes progress
     * @return the running watchdog
     */
    public static WorkerWatchdog watch(Worker worker, LongSupplier progress) {
        return new WorkerWatchdog(
                worker, progress, WorkerTimeouts.healthInterval(), WorkerTimeouts.progressTimeout());
    }

    WorkerWatchdog(
            Worker worker, LongSupplier progress, Duration interval, Duration progressTimeout) {
        this.ensemble = worker instanceof DistributedWorkersEnsemble e ? e : null;
        this.workers = ensemble != null ? ensemble.workers() : List.of(worker);
        this.runThread = Thread.currentThread();
        this.progress = progress;
        this.intervalNanos = interval.toNanos();
        this.progressTimeoutNanos = progressTimeout.toNanos();
        this.poller = Thread.ofVirtual().name("worker-watchdog").start(this::poll);
    }

    /**
     * The failure to report for a run that threw: the watchdog's own when it aborted the run, with
     * {@code e} suppressed into it, else {@code e}.
     *
     * @param e what the run threw
     * @return the exception to rethrow
     */
    public Exception explain(Exception e) {
        synchronized (lock) {
            if (failure == null) {
                return e;
            }
            if (interruptedRun) {
                // The interrupt was ours, not a request to stop the benchmark
                Thread.interrupted();
                interruptedRun = false;
            }
            reported = true;
            WorkerUnhealthyException explained =
                    new WorkerUnhealthyException(failure.workerId(), failure.reason());
            explained.addSuppressed(e);
            return explained;
        }
    }

    /**
     * Stops watching. Clears the interrupt the watchdog delivered, if any. Call from the run thread.
     *
     * @throws WorkerUnhealthyException if a worker failed after the run had already returned
     */
    @Override
    public void close() {
        WorkerUnhealthyException unreported;
        synchronized (lock) {
            closed = true;
            if (interruptedRun) {
                Thread.interrupted();
                interruptedRun = false;
            }
            unreported = reported ? null : failure;
            reported = true;
        }
        // Not joined: a health call in flight can take its full timeout, and once closed the poller
        // can no longer interrupt the run thread
        poller.interrupt();
        if (unreported != null) {
            throw unreported;
        }
    }

    private void poll() {
        long lastProgress = progress.getAsLong();
        long lastProgressAt = System.nanoTime();
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(Duration.ofNanos(intervalNanos));
                if (!checkWorkers()) {
                    return;
                }
                long now = System.nanoTime();
                long current = progress.getAsLong();
                if (current != lastProgress) {
                    lastProgress = current;
                    lastProgressAt = now;
                } else if (now - lastProgressAt >= progressTimeoutNanos) {
                    logThreadDump(Duration.ofNanos(now - lastProgressAt));
                    lastProgressAt = now;
                }
            }
        } catch (InterruptedException e) {
            // Closed; the interrupt ends the poller thread
            Thread.currentThread().interrupt();
        }
    }

    // Returns false once a worker failed, which ends the polling
    private boolean checkWorkers() throws InterruptedException {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<WorkerHealth>> checks =
                    workers.stream().map(worker -> executor.submit(worker::health)).toList();
            for (int i = 0; i < workers.size(); i++) {
                Worker worker = workers.get(i);
                String reason = unhealthyReason(worker, checks.get(i));
                if (reason != null) {
                    fail(worker.id(), reason);
                    return false;
                }
            }
        }
        return true;
    }

    private @Nullable String unhealthyReason(Worker worker, Future<WorkerHealth> check)
            throws InterruptedException {
        WorkerHealth health;
        try {
            health = check.get();
        } catch (ExecutionException e) {
            int missed = misses.merge(worker, 1, Integer::sum);
            String cause = String.valueOf(e.getCause());
            log.warn(
                    "Health check {} of {} failed for worker {}: {}", missed, MAX_MISSES, worker.id(), cause);
            return missed >= MAX_MISSES
                    ? "unreachable after " + missed + " health checks: " + cause
                    : null;
        }
        misses.remove(worker);
        if (health.driver() == DriverState.RUNNING) {
            return null;
        }
        StringBuilder reason = new StringBuilder("driver process is ").append(health.driver());
        if (health.exitCode() != null) {
            reason.append(" with exit code ").append(health.exitCode());
        }
        if (health.lastError() != null) {
            reason.append(": ").append(health.lastError());
        }
        return reason.toString();
    }

    private void fail(String workerId, String reason) {
        WorkerUnhealthyException unhealthy = new WorkerUnhealthyException(workerId, reason);
        String message = "Worker " + workerId + " is unhealthy: " + reason;
        synchronized (lock) {
            if (closed || failure != null) {
                return;
            }
            failure = unhealthy;
            interruptedRun = true;
            runThread.interrupt();
        }
        log.error("{}; aborting the workload", message);
        DistributedWorkersEnsemble watched = ensemble;
        if (watched != null) {
            // Polling a dead worker can take the full request timeout; the abort must not wait for it
            Thread.ofVirtual()
                    .name("worker-watchdog-counters")
                    .start(() -> watched.logCountersPerWorker(message));
        }
    }

    private void logThreadDump(Duration stalled) {
        StringBuilder dump = new StringBuilder();
        // Platform threads only: the run thread is one, controllers on virtual threads are not listed
        Thread.getAllStackTraces()
                .forEach((thread, frames) -> {
                    dump.append('"')
                            .append(thread.getName())
                            .append("\" ")
                            .append(thread.getState())
                            .append('\n');
                    for (StackTraceElement frame : frames) {
                        dump.append("    at ").append(frame).append('\n');
                    }
                });
        log.warn(
                "No workload progress for {} s; coordinator thread dump:\n{}", stalled.toSeconds(), dump);
    }
}
