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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.github.mihkels.omb.utils.distributor.KeyDistributorType;
import io.github.mihkels.omb.utils.payload.PayloadTemplates;
import io.github.mihkels.omb.worker.commands.ConsumerAssignment;
import io.github.mihkels.omb.worker.commands.CountersStats;
import io.github.mihkels.omb.worker.commands.CumulativeLatencies;
import io.github.mihkels.omb.worker.commands.ProducerWorkAssignment;
import io.github.mihkels.omb.worker.commands.RunPhase;
import io.github.mihkels.omb.worker.commands.TopicSubscription;
import io.github.mihkels.omb.worker.commands.TopicsInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the worker's real send, receive and stats path against {@link LoopbackDriver} and checks
 * that the reported numbers match the injected ones. The scheduler never fires early, so the
 * minimums are hard bounds; the upper bounds leave room for scheduler jitter on a busy machine.
 */
class LoopbackMeasurementTest {
    private static final long ACK_DELAY_MILLIS = 20;
    private static final long DELIVERY_DELAY_MILLIS = 30;
    private static final int DROP_EVERY = 100;
    private static final int DUPLICATE_EVERY = 50;
    private static final int RATE = 1000;
    private static final long MEASURE_MILLIS = 3000;

    @Test
    @Timeout(30)
    void reportsInjectedLatencyRateAndDeliveryFaults(@TempDir Path directory) throws Exception {
        Path config =
                Files.writeString(
                        directory.resolve("loopback.yaml"),
                        """
                        name: loopback
                        driverClass: io.github.mihkels.omb.worker.LoopbackDriver
                        ackDelayMillis: %d
                        deliveryDelayMillis: %d
                        dropEvery: %d
                        duplicateEvery: %d
                        """
                                .formatted(ACK_DELAY_MILLIS, DELIVERY_DELAY_MILLIS, DROP_EVERY, DUPLICATE_EVERY));

        try (LocalWorker worker = new LocalWorker()) {
            worker.initializeDriver(config);
            List<String> topics = worker.createTopics(new TopicsInfo(1, 1));
            ConsumerAssignment consumers = new ConsumerAssignment();
            consumers.verifyDelivery = true;
            consumers.topicsSubscriptions.add(new TopicSubscription(topics.getFirst(), "sub"));
            worker.createConsumers(consumers);
            worker.createProducers(topics);
            worker.startLoad(
                    new ProducerWorkAssignment(
                            PayloadTemplates.of(List.of(new byte[100]), null),
                            RATE,
                            KeyDistributorType.NO_KEY,
                            null,
                            true));

            // Warm-up, then measure a clean window
            TimeUnit.SECONDS.sleep(1);
            worker.resetStats();
            CountersStats start = worker.getCountersStats();
            long startNanos = System.nanoTime();
            TimeUnit.MILLISECONDS.sleep(MEASURE_MILLIS);
            CountersStats end = worker.getCountersStats();
            double elapsedSeconds = (System.nanoTime() - startNanos) / 1e9;
            CumulativeLatencies latencies = worker.getCumulativeLatencies();

            worker.setPhase(RunPhase.DELIVERY_DRAIN);
            CountersStats drained = awaitSettled(worker);
            worker.stopAll();

            double rate = (end.messagesSent() - start.messagesSent()) / elapsedSeconds;
            assertThat(rate).isCloseTo(RATE, within(RATE * 0.03));

            assertMillis(latencies.publishLatency, ACK_DELAY_MILLIS);
            // Wall-clock millisecond stamps on both ends: floor(b) - floor(a) >= floor(b - a)
            assertMillis(latencies.endToEndLatency, DELIVERY_DELAY_MILLIS);

            long sent = drained.verifiedSent();
            long dropped = sent / DROP_EVERY;
            // DROP_EVERY is a multiple of DUPLICATE_EVERY, and a dropped send is never duplicated
            long duplicated = sent / DUPLICATE_EVERY - sent / DROP_EVERY;
            assertThat(sent).isEqualTo(drained.messagesSent()).isGreaterThan(3000);
            assertThat(drained.messageSendErrors()).isZero();
            assertThat(drained.delivered()).isEqualTo(sent - dropped);
            assertThat(drained.duplicated()).isEqualTo(duplicated);
            assertThat(drained.outOfOrder()).isZero();
        }
    }

    private static void assertMillis(Histogram micros, long expectedMillis) {
        long expected = TimeUnit.MILLISECONDS.toMicros(expectedMillis);
        assertThat(micros.getMinValue()).isGreaterThanOrEqualTo(expected);
        assertThat(micros.getValueAtPercentile(50)).isLessThan(expected + 2_000);
        assertThat(micros.getValueAtPercentile(99)).isLessThan(expected + 10_000);
    }

    private static CountersStats awaitSettled(LocalWorker worker) throws InterruptedException {
        CountersStats last = worker.getCountersStats();
        while (true) {
            TimeUnit.MILLISECONDS.sleep(200);
            CountersStats now = worker.getCountersStats();
            if (now.equals(last)) {
                return now;
            }
            last = now;
        }
    }
}
