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
package com.example.omb.loopback;

import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A broker-less driver that hands every message directly to the consumers of each subscription of
 * the topic. Useful to check plugin installation and the benchmark harness itself.
 */
public class LoopbackBenchmarkDriver implements BenchmarkDriver {

    private static final Logger log = LoggerFactory.getLogger(LoopbackBenchmarkDriver.class);

    private final Map<String, Map<String, Subscription>> topics = new ConcurrentHashMap<>();

    @Override
    public void initialize(Path configurationFile, MeterRegistry statsLogger) {
        log.info(
                "Loopback driver initialized from {} using bundled commons-lang3 {}",
                configurationFile,
                StringUtils.defaultString(StringUtils.class.getPackage().getImplementationVersion()));
    }

    @Override
    public String getTopicNamePrefix() {
        return "loopback";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        topics.putIfAbsent(topic, new ConcurrentHashMap<>());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        Map<String, Subscription> subscriptions =
                topics.computeIfAbsent(topic, t -> new ConcurrentHashMap<>());
        BenchmarkProducer producer =
                new BenchmarkProducer() {
                    @Override
                    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
                        long now = System.currentTimeMillis();
                        subscriptions.values().forEach(s -> s.deliver(payload, now));
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void close() {}
                };
        return CompletableFuture.completedFuture(producer);
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        Subscription subscription =
                topics
                        .computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
                        .computeIfAbsent(subscriptionName, s -> new Subscription());
        subscription.consumers.add(consumerCallback);
        BenchmarkConsumer consumer = () -> subscription.consumers.remove(consumerCallback);
        return CompletableFuture.completedFuture(consumer);
    }

    @Override
    public void close() {
        topics.clear();
    }

    private static final class Subscription {
        private final List<ConsumerCallback> consumers = new CopyOnWriteArrayList<>();
        private final AtomicInteger next = new AtomicInteger();

        void deliver(byte[] payload, long publishTimestamp) {
            List<ConsumerCallback> current = consumers;
            if (current.isEmpty()) {
                return;
            }
            int index = Math.floorMod(next.getAndIncrement(), current.size());
            current.get(index).messageReceived(payload, publishTimestamp);
        }
    }
}
