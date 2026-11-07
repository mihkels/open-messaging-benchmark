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

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.openmessaging.benchmark.driver.Failures;
import io.openmessaging.benchmark.utils.ListPartition;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DistributedWorkersEnsemble implements Worker {
    private final Thread shutdownHook = new Thread(this::stopAll);
    private final List<Worker> workers;
    private final List<Worker> producerWorkers;
    private final List<Worker> consumerWorkers;
    private final Worker leader;

    private int numberOfUsedProducerWorkers;

    public DistributedWorkersEnsemble(List<Worker> workers, boolean extraConsumerWorkers) {
        Preconditions.checkArgument(workers.size() > 1, "At least two workers are required");

        this.workers = unmodifiableList(workers);
        leader = workers.getFirst();
        int numberOfProducerWorkers = getNumberOfProducerWorkers(workers, extraConsumerWorkers);
        List<List<Worker>> partitions =
                Lists.partition(Lists.reverse(workers), workers.size() - numberOfProducerWorkers);
        this.producerWorkers = partitions.get(1);
        this.consumerWorkers = partitions.get(0);

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
    @VisibleForTesting
    static int getNumberOfProducerWorkers(List<Worker> workers, boolean extraConsumerWorkers) {
        return extraConsumerWorkers ? (workers.size() + 2) / 3 : workers.size() / 2;
    }

    @Override
    public void initializeDriver(Path configurationFile) throws Exception {
        callAll(
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

        // Number of actually used workers might be less than available workers
        numberOfUsedProducerWorkers =
                (int) topicsPerProducerMap.values().stream().filter(t -> !t.isEmpty()).count();
        log.debug(
                "Producing worker count: {} of {}", numberOfUsedProducerWorkers, producerWorkers.size());
        forAll(
                List.copyOf(topicsPerProducerMap.entrySet()),
                entry -> entry.getKey().createProducers(entry.getValue()));
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        // Reduce the publish rate across all the brokers
        double newRate = producerWorkAssignment.publishRate() / numberOfUsedProducerWorkers;
        log.debug("Setting worker assigned publish rate to {} msgs/sec", newRate);
        ProducerWorkAssignment assignment = producerWorkAssignment.withPublishRate(newRate);
        forAll(producerWorkers, worker -> worker.startLoad(assignment));
    }

    @Override
    public void probeProducers() {
        forAll(producerWorkers, Worker::probeProducers);
    }

    @Override
    public void adjustPublishRate(double publishRate) {
        double newRate = publishRate / numberOfUsedProducerWorkers;
        log.debug("Adjusting producer publish rate to {} msgs/sec", newRate);
        forAll(producerWorkers, worker -> worker.adjustPublishRate(newRate));
    }

    @Override
    public void stopAll() {
        try {
            forAll(workers, Worker::stopAll);
        } catch (RuntimeException | Error e) {
            throw new IllegalStateException("Failed to stop one or more workers", e);
        }
    }

    @Override
    public String id() {
        return "Ensemble[" + workers.stream().map(Worker::id).collect(joining(",")) + "]";
    }

    @Override
    public void setRunId(String runId) {
        forAll(workers, worker -> worker.setRunId(runId));
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
            topicsPerWorkerMap.put(consumerWorkers.get(i++), individualAssignment);
        }
        forAll(
                List.copyOf(topicsPerWorkerMap.entrySet()),
                entry -> entry.getKey().createConsumers(entry.getValue()));
    }

    @Override
    public PeriodStats getPeriodStats() {
        return PeriodStats.aggregate(mapAll(workers, Worker::getPeriodStats));
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        return mapAll(workers, Worker::getCumulativeLatencies).stream()
                .reduce(new CumulativeLatencies(), CumulativeLatencies::plus);
    }

    @Override
    public CountersStats getCountersStats() {
        return mapAll(workers, Worker::getCountersStats).stream()
                .reduce(new CountersStats(), CountersStats::plus);
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

    private static <T> void forAll(List<T> targets, Consumer<T> action) {
        mapAll(
                targets,
                target -> {
                    action.accept(target);
                    return null;
                });
    }

    private static <T, R> List<R> mapAll(List<T> targets, Function<T, R> action) {
        try {
            return callAll(targets, action::apply);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
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
        R call(T target) throws Exception;
    }

    private static final Logger log = LoggerFactory.getLogger(DistributedWorkersEnsemble.class);
}
