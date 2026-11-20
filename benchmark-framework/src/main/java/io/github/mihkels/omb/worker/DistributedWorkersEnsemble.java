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

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;

import io.github.mihkels.omb.driver.Failures;
import io.github.mihkels.omb.utils.ListPartition;
import io.github.mihkels.omb.worker.commands.ConsumerAssignment;
import io.github.mihkels.omb.worker.commands.CountersStats;
import io.github.mihkels.omb.worker.commands.CumulativeLatencies;
import io.github.mihkels.omb.worker.commands.PeriodStats;
import io.github.mihkels.omb.worker.commands.ProducerWorkAssignment;
import io.github.mihkels.omb.worker.commands.RunContext;
import io.github.mihkels.omb.worker.commands.RunPhase;
import io.github.mihkels.omb.worker.commands.TopicSubscription;
import io.github.mihkels.omb.worker.commands.TopicsInfo;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DistributedWorkersEnsemble implements Worker {
    private final Thread shutdownHook = new Thread(this::stopAll);
    private final List<Worker> workers;
    private final List<Worker> producerWorkers;
    private final List<Worker> consumerWorkers;
    private final Worker leader;

    // Producers on each producer worker that has any, from the last createProducers
    private Map<Worker, Integer> producersPerWorker = Map.of();
    private int totalProducers;

    public DistributedWorkersEnsemble(List<Worker> workers, boolean extraConsumerWorkers) {
        if (workers.size() < 2) {
            throw new IllegalArgumentException("At least two workers are required");
        }

        this.workers = unmodifiableList(workers);
        leader = workers.getFirst();
        int numberOfProducerWorkers = getNumberOfProducerWorkers(workers, extraConsumerWorkers);
        // Producers come from the front of the list, consumers from the back
        List<Worker> reversed = workers.reversed();
        int numberOfConsumerWorkers = workers.size() - numberOfProducerWorkers;
        this.producerWorkers = reversed.subList(numberOfConsumerWorkers, workers.size());
        this.consumerWorkers = reversed.subList(0, numberOfConsumerWorkers);

        log.atInfo()
                .setMessage("Workers list - producers: [{}]")
                .addArgument(() -> producerWorkers.stream().map(Worker::id).collect(joining(",")))
                .log();
        log.atInfo()
                .setMessage("Workers list - consumers: {}")
                .addArgument(() -> consumerWorkers.stream().map(Worker::id).collect(joining(",")))
                .log();

        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    /*
     * For driver-jms extra consumers are required. If there is an odd number of workers then allocate the extra
     * to consumption.
     */
    static int getNumberOfProducerWorkers(List<Worker> workers, boolean extraConsumerWorkers) {
        return extraConsumerWorkers ? (workers.size() + 2) / 3 : workers.size() / 2;
    }

    @Override
    public void initializeDriver(Path configurationFile) throws Exception {
        callAllLoggingCounters(
                workers,
                worker -> {
                    worker.initializeDriver(configurationFile);
                    return null;
                });
    }

    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) {
        return leader.createTopics(topicsInfo);
    }

    @Override
    public void createProducers(List<String> topics) {
        List<List<String>> topicsPerProducer =
                ListPartition.partitionList(topics, producerWorkers.size());
        Map<Worker, List<String>> topicsPerProducerMap = new LinkedHashMap<>();
        int i = 0;
        for (List<String> assignedTopics : topicsPerProducer) {
            topicsPerProducerMap.put(producerWorkers.get(i++), assignedTopics);
        }

        // Each list holds one topic per producer, so its size is the worker's producer count
        Map<Worker, Integer> counts = new LinkedHashMap<>();
        topicsPerProducerMap.forEach((worker, assigned) -> {
            if (!assigned.isEmpty()) {
                counts.put(worker, assigned.size());
            }
        });
        producersPerWorker = counts;
        totalProducers = topics.size();
        log.debug(
                "Producing worker count: {} of {}", producersPerWorker.size(), producerWorkers.size());
        forAll(
                List.copyOf(topicsPerProducerMap.entrySet()),
                entry -> entry.getKey().createProducers(entry.getValue()));
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        forEachProducingWorker(
                producerWorkAssignment.publishRate(),
                (worker, rate) -> worker.startLoad(producerWorkAssignment.withPublishRate(rate)));
    }

    @Override
    public void probeProducers() {
        forAll(producerWorkers, Worker::probeProducers);
    }

    @Override
    public void adjustPublishRate(double publishRate) {
        forEachProducingWorker(publishRate, Worker::adjustPublishRate);
    }

    /*
     * Splits the rate by producer, not by worker, so every producer gets the same share. Workers without
     * producers are skipped: a worker rejects a start rate of 0.
     */
    private void forEachProducingWorker(double publishRate, BiConsumer<Worker, Double> action) {
        if (totalProducers == 0) {
            throw new IllegalStateException(
                    "No producers to set a publish rate for: call createProducers with at least one topic first");
        }
        forAll(
                List.copyOf(producersPerWorker.entrySet()),
                entry -> {
                    double rate = publishRate * entry.getValue() / totalProducers;
                    log.debug("Setting publish rate of {} to {} msgs/sec", entry.getKey().id(), rate);
                    action.accept(entry.getKey(), rate);
                });
    }

    @Override
    public void stopAll() {
        // Not callAllLoggingCounters: also runs from the shutdown hook, where polling dead workers only
        // delays exit.
        try {
            callAll(
                    workers,
                    worker -> {
                        worker.stopAll();
                        return null;
                    });
        } catch (Exception | Error e) {
            throw new IllegalStateException("Failed to stop one or more workers", e);
        }
    }

    @Override
    public String id() {
        return "Ensemble[" + workers.stream().map(Worker::id).collect(joining(",")) + "]";
    }

    @Override
    public void setRunContext(RunContext run) {
        forAll(workers, worker -> worker.setRunContext(run));
    }

    @Override
    public void setPhase(RunPhase phase) {
        forAll(workers, worker -> worker.setPhase(phase));
    }

    List<Worker> workers() {
        return workers;
    }

    public int workerCount() {
        return workers.size();
    }

    @Override
    public boolean canReuseAfterFailure() {
        return workers.stream().allMatch(Worker::canReuseAfterFailure);
    }

    @Override
    public void pauseConsumers() {
        forAll(consumerWorkers, Worker::pauseConsumers);
    }

    @Override
    public void resumeConsumers() {
        forAll(consumerWorkers, Worker::resumeConsumers);
    }

    @Override
    public void createConsumers(ConsumerAssignment overallConsumerAssignment) {
        List<List<TopicSubscription>> subscriptionsPerConsumer =
                ListPartition.partitionList(
                        overallConsumerAssignment.topicsSubscriptions, consumerWorkers.size());
        Map<Worker, ConsumerAssignment> topicsPerWorkerMap = new LinkedHashMap<>();
        int i = 0;
        for (List<TopicSubscription> tsl : subscriptionsPerConsumer) {
            ConsumerAssignment individualAssignment = new ConsumerAssignment();
            individualAssignment.topicsSubscriptions = tsl;
            individualAssignment.verifyDelivery = overallConsumerAssignment.verifyDelivery;
            individualAssignment.partitionTelemetry = overallConsumerAssignment.partitionTelemetry;
            topicsPerWorkerMap.put(consumerWorkers.get(i++), individualAssignment);
        }
        forAll(
                List.copyOf(topicsPerWorkerMap.entrySet()),
                entry -> entry.getKey().createConsumers(entry.getValue()));
    }

    @Override
    public PeriodStats getPeriodStats() {
        return PeriodStats.aggregate(
                mapAll(workers, worker -> worker.getPeriodStats().withWorkerId(worker.id())));
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        return CumulativeLatencies.aggregate(mapAll(workers, Worker::getCumulativeLatencies));
    }

    @Override
    public CountersStats getCountersStats() {
        return mapAll(workers, Worker::getCountersStats).stream()
                .reduce(new CountersStats(), CountersStats::plus);
    }

    /**
     * Polls every worker's counters without summing them, so a stalled worker stays visible. Never
     * throws for a failing worker: its entry carries the failure instead of counters.
     *
     * @return one entry per worker, in worker order
     */
    public List<WorkerCounters> countersPerWorker() {
        try {
            return callAll(workers, this::countersOf);
        } catch (Exception _) {
            // Only an interruption gets here, since countersOf catches every worker failure.
            return List.of();
        }
    }

    /**
     * Logs one line per worker with its role and counters, so the failure log shows every worker side
     * by side. Never throws.
     *
     * @param reason why the breakdown is logged, e.g. the failure or stall it explains
     */
    public void logCountersPerWorker(String reason) {
        if (Thread.currentThread().isInterrupted()) {
            return;
        }
        log.atWarn()
                .setMessage("{}; counters per worker:\n{}")
                .addArgument(reason)
                .addArgument(() -> countersPerWorker().stream().map(c -> "  " + c).collect(joining("\n")))
                .log();
    }

    String role(Worker worker) {
        return producerWorkers.contains(worker) ? "producer" : "consumer";
    }

    private WorkerCounters countersOf(Worker worker) {
        String role = role(worker);
        try {
            return new WorkerCounters(worker.id(), role, worker.getCountersStats(), null);
        } catch (Exception e) {
            return new WorkerCounters(worker.id(), role, null, e.toString());
        }
    }

    /**
     * One worker's counters, or the failure that kept them from being read.
     *
     * @param workerId the worker's id
     * @param role {@code producer} or {@code consumer}
     * @param counters the worker's counters, or {@code null} if polling failed
     * @param failure why polling failed, or {@code null}
     */
    public record WorkerCounters(
            String workerId, String role, @Nullable CountersStats counters, @Nullable String failure) {
        @Override
        public String toString() {
            if (counters == null) {
                return workerId + " [" + role + "] counters unavailable: " + failure;
            }
            return workerId
                    + " ["
                    + role
                    + "] sent="
                    + counters.messagesSent()
                    + " sendErrors="
                    + counters.messageSendErrors()
                    + " received="
                    + counters.messagesReceived();
        }
    }

    @Override
    public void resetStats() {
        forAll(workers, Worker::resetStats);
    }

    @Override
    public void close() throws Exception {
        Failures failures = new Failures();
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException _) {
            // JVM shutdown is already in progress.
        } catch (Throwable t) {
            failures.add(t);
        }
        workers.forEach(failures::close);
        failures.throwIfAny("Failed to close one or more workers");
    }

    private <T> void forAll(List<T> targets, Consumer<T> action) {
        mapAll(
                targets,
                target -> {
                    action.accept(target);
                    return null;
                });
    }

    private <T, R extends @Nullable Object> List<R> mapAll(List<T> targets, Function<T, R> action) {
        try {
            return callAllLoggingCounters(targets, action::apply);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private <T, R extends @Nullable Object> List<R> callAllLoggingCounters(
            List<T> targets, Call<T, R> action) throws Exception {
        try {
            return callAll(targets, action);
        } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted()) {
                String reason = "Worker call failed: " + e;
                // Off the caller's thread: polling a dead worker would hold the failure for its
                // whole request timeout
                Thread.ofVirtual().name("worker-counters-log").start(() -> logCountersPerWorker(reason));
            }
            throw e;
        }
    }

    // Calls every target concurrently, since each call is a blocking request to a remote worker. All
    // calls finish before this returns; the first failure in target order is thrown with the others
    // suppressed into it.
    private static <T, R> List<R> callAll(List<T> targets, Call<T, R> action) throws Exception {
        Failures failures = new Failures();
        List<R> results = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<R>> calls =
                    targets.stream().map(target -> executor.submit(() -> action.call(target))).toList();
            for (Future<R> call : calls) {
                try {
                    results.add(call.get());
                } catch (ExecutionException e) {
                    failures.add(e.getCause());
                } catch (InterruptedException e) {
                    failures.addInterruption(e);
                    calls.forEach(pending -> pending.cancel(true));
                    break;
                }
            }
        }
        failures.throwIfAny();
        return results;
    }

    @FunctionalInterface
    private interface Call<T, R> {
        @SuppressWarnings("java:S112") // Wraps arbitrary Worker calls, which may throw Exception
        @Nullable R call(T target) throws Exception;
    }

    private static final Logger log = LoggerFactory.getLogger(DistributedWorkersEnsemble.class);
}
