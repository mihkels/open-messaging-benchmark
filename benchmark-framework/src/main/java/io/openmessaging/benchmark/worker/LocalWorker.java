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

import static java.util.stream.Collectors.toList;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.DriverConfiguration;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkDriver.ConsumerInfo;
import io.openmessaging.benchmark.driver.BenchmarkDriver.ProducerInfo;
import io.openmessaging.benchmark.driver.BenchmarkDriver.TopicInfo;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.utils.IsolatedDriverLoader;
import io.openmessaging.benchmark.utils.RandomGenerator;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.UniformRateLimiter;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.yaml.YAMLFactory;

public class LocalWorker implements Worker, ConsumerCallback {

    private BenchmarkDriver benchmarkDriver = null;
    private final List<BenchmarkProducer> producers = new ArrayList<>();
    private final List<BenchmarkConsumer> consumers = new ArrayList<>();
    private volatile MessageProducer messageProducer;
    private final ExecutorService executor =
            Executors.newCachedThreadPool(Thread.ofVirtual().name("local-worker-", 0).factory());
    private final WorkerStats stats;
    private boolean testCompleted = false;
    private volatile boolean consumersArePaused = false;

    public LocalWorker() {
        this(new PrometheusMeterRegistry(l -> null));
    }

    public LocalWorker(PrometheusMeterRegistry statsLogger) {
        stats = new WorkerStats(statsLogger);
        updateMessageProducer(1.0);
    }

    @Override
    public void initializeDriver(Path driverConfigFile) throws Exception {
        initializeDriver(driverConfigFile, null);
    }

    @Override
    public void initializeDriver(Path driverConfigFile, Path isolatedDriverHome) throws Exception {
        DriverConfiguration driverConfiguration =
                reader.readValue(Files.newInputStream(driverConfigFile));
        log.info("Initializing driver: {}", writer.writeValueAsString(driverConfiguration));

        try {
            // Validate driver class is present
            if (driverConfiguration.driverClass == null || driverConfiguration.driverClass.isEmpty()) {
                throw new IllegalArgumentException("Driver configuration must specify a driverClass");
            }

            // Check if this driver should be loaded in isolation
            var shouldIsolate = isolatedDriverHome != null;
            if (shouldIsolate) {
                log.info("Using isolated driver home {}", isolatedDriverHome);
                log.info(
                        "Loading driver {} with isolated classloader from {}",
                        driverConfiguration.driverClass,
                        isolatedDriverHome);
                ClassLoader isolatedCL = IsolatedDriverLoader.forDriverFolder(isolatedDriverHome);
                benchmarkDriver =
                        IsolatedDriverLoader.newInstance(isolatedCL, driverConfiguration.driverClass);
            } else {
                log.info("Loading driver {} from classpath", driverConfiguration.driverClass);
                // Standard loading for non-isolated drivers
                benchmarkDriver =
                        (BenchmarkDriver)
                                Class.forName(driverConfiguration.driverClass)
                                        .getDeclaredConstructor()
                                        .newInstance();
            }

            benchmarkDriver.initialize(driverConfigFile, stats.getMeterRegistry());

        } catch (InstantiationException
                | IllegalAccessException
                | ClassNotFoundException
                | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) {
        Timer timer = new Timer();

        List<TopicInfo> topicInfos =
                IntStream.range(0, topicsInfo.numberOfTopics)
                        .mapToObj(
                                i -> new TopicInfo(generateTopicName(i), topicsInfo.numberOfPartitionsPerTopic))
                        .toList();

        benchmarkDriver.createTopics(topicInfos).join();

        List<String> topics = topicInfos.stream().map(TopicInfo::topic).collect(toList());

        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());
        return topics;
    }

    private String generateTopicName(int i) {
        return String.format(
                "%s-%07d-%s", benchmarkDriver.getTopicNamePrefix(), i, RandomGenerator.getRandomString());
    }

    @Override
    public void createProducers(List<String> topics) {
        Timer timer = new Timer();
        AtomicInteger index = new AtomicInteger();

        producers.addAll(
                benchmarkDriver
                        .createProducers(
                                topics.stream().map(t -> new ProducerInfo(index.getAndIncrement(), t)).toList())
                        .join());

        log.info("Created {} producers in {} ms", producers.size(), timer.elapsedMillis());
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) {
        Timer timer = new Timer();
        AtomicInteger index = new AtomicInteger();

        consumers.addAll(
                benchmarkDriver
                        .createConsumers(
                                consumerAssignment.topicsSubscriptions.stream()
                                        .map(
                                                c ->
                                                        new ConsumerInfo(
                                                                index.getAndIncrement(), c.topic, c.subscription, this))
                                        .toList())
                        .join());

        log.info("Created {} consumers in {} ms", consumers.size(), timer.elapsedMillis());
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        log.info(
                "Starting load with {} producers, publish rate: {}",
                producers.size(),
                producerWorkAssignment.publishRate());

        testCompleted = false;
        int processors = Runtime.getRuntime().availableProcessors();

        updateMessageProducer(producerWorkAssignment.publishRate());

        Map<Integer, List<BenchmarkProducer>> processorAssignment = new TreeMap<>();

        int processorIdx = 0;
        for (BenchmarkProducer p : producers) {
            processorAssignment
                    .computeIfAbsent(processorIdx, x -> new ArrayList<BenchmarkProducer>())
                    .add(p);

            processorIdx = (processorIdx + 1) % processors;
        }

        processorAssignment
                .values()
                .forEach(
                        benchmarkProducers ->
                                submitProducersToExecutor(
                                        benchmarkProducers,
                                        KeyDistributor.build(producerWorkAssignment.keyDistributorType()),
                                        producerWorkAssignment.payloadData()));
    }

    @Override
    public void probeProducers() throws IOException {
        producers.forEach(
                producer ->
                        producer.sendAsync(Optional.of("key"), new byte[10]).thenRun(stats::recordMessageSent));
    }

    private void submitProducersToExecutor(
            List<BenchmarkProducer> producers, KeyDistributor keyDistributor, List<byte[]> payloads) {
        log.info(
                "Submitting {} producers to executor, testCompleted: {}", producers.size(), testCompleted);

        ThreadLocalRandom r = ThreadLocalRandom.current();
        int payloadCount = payloads.size();
        for (BenchmarkProducer p : producers) {
            executor.submit(
                    () -> {
                        try {
                            while (!testCompleted) {
                                messageProducer.sendMessage(
                                        p,
                                        Optional.ofNullable(keyDistributor.next()),
                                        payloads.get(r.nextInt(payloadCount)));
                            }
                        } catch (Throwable t) {
                            log.error("Producer thread failed", t);
                        }
                    });
        }
    }

    @Override
    public void adjustPublishRate(double publishRate) {
        if (publishRate < 1.0) {
            updateMessageProducer(1.0);
            return;
        }
        updateMessageProducer(publishRate);
    }

    private void updateMessageProducer(double publishRate) {
        messageProducer = new MessageProducer(new UniformRateLimiter(publishRate), stats);
    }

    @Override
    public PeriodStats getPeriodStats() {
        return stats.toPeriodStats();
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        return stats.toCumulativeLatencies();
    }

    @Override
    public CountersStats getCountersStats() {
        return stats.toCountersStats();
    }

    @Override
    public void messageReceived(byte[] data, long publishTimestamp) {
        internalMessageReceived(data.length, publishTimestamp);
    }

    @Override
    public void messageReceived(ByteBuffer data, long publishTimestamp) {
        internalMessageReceived(data.remaining(), publishTimestamp);
    }

    public void internalMessageReceived(int size, long publishTimestamp) {
        long now = System.currentTimeMillis();
        long endToEndLatencyMicros = TimeUnit.MILLISECONDS.toMicros(now - publishTimestamp);
        stats.recordMessageReceived(size, endToEndLatencyMicros);

        while (consumersArePaused) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupt status
                log.warn("Thread interrupted while consumers paused", e);
                break; // Exit the loop if interrupted
            }
        }
    }

    @Override
    public void pauseConsumers() throws IOException {
        consumersArePaused = true;
        log.info("Pausing consumers");
    }

    @Override
    public void resumeConsumers() throws IOException {
        consumersArePaused = false;
        log.info("Resuming consumers");
    }

    @Override
    public void resetStats() throws IOException {
        stats.resetLatencies();
    }

    @Override
    public void stopAll() {
        testCompleted = true;
        consumersArePaused = false;
        stats.reset();

        try {
            Thread.sleep(100);

            for (BenchmarkProducer producer : producers) {
                producer.close();
            }
            producers.clear();

            for (BenchmarkConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();

            if (benchmarkDriver != null) {
                benchmarkDriver.close();
                benchmarkDriver = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String id() {
        return "local";
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate in time, forcing shutdown");
                executor.shutdownNow();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Executor did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for executor shutdown");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectReader reader =
            mapper
                    .readerFor(DriverConfiguration.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);
}
