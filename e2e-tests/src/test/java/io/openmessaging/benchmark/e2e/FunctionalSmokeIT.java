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
package io.openmessaging.benchmark.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class FunctionalSmokeIT extends BaseE2eIT {
    @Test
    void stopsAsSoonAsTheRequiredMessagesAreDelivered() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 2;
        workload.producersPerTopic = 3;
        workload.subscriptionsPerTopic = 2;
        workload.consumerPerSubscription = 2;
        FakeWorker worker = new FakeWorker(new CountersStats(20, 0, 40));

        SmokeResult result = FunctionalSmoke.run(worker, workload, Duration.ofSeconds(1));

        validateResults(result, workload);
        assertThat(worker.loadPolls).isEqualTo(1);
        assertThat(worker.probed).isTrue();
        assertThat(worker.consumers.topicsSubscriptions).hasSize(8);
        assertThat(worker.producers).hasSize(6);
        assertThat(worker.assignment.payloadData())
                .allSatisfy(payload -> assertThat(payload).hasSize(1024));
        assertThat(worker.assignment.publishRate()).isEqualTo(workload.producerRate);
        assertThat(worker.stopped).isTrue();
    }

    @Test
    void failsAndStopsOnPublishErrors() {
        FakeWorker worker = new FakeWorker(new CountersStats(1, 1, 0));
        assertThatThrownBy(
                        () -> FunctionalSmoke.run(worker, createSimpleWorkload(), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("publish errors");
        assertThat(worker.stopped).isTrue();
    }

    @Test
    void failsAndStopsWhenDeliveryDoesNotProgress() {
        FakeWorker worker = new FakeWorker(new CountersStats());
        assertThatThrownBy(
                        () -> FunctionalSmoke.run(worker, createSimpleWorkload(), Duration.ofMillis(20)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Timed out waiting");
        assertThat(worker.stopped).isTrue();
    }

    private static final class FakeWorker implements Worker {
        private final CountersStats counters;
        private ConsumerAssignment consumers;
        private List<String> producers;
        private ProducerWorkAssignment assignment;
        private int loadPolls;
        private boolean probed;
        private boolean stopped;

        private FakeWorker(CountersStats counters) {
            this.counters = counters;
        }

        @Override
        public void initializeDriver(Path config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> createTopics(TopicsInfo topics) {
            List<String> names = new ArrayList<>();
            for (int i = 0; i < topics.numberOfTopics; i++) {
                names.add("topic-" + i);
            }
            return names;
        }

        @Override
        public void createProducers(List<String> topics) {
            producers = topics;
        }

        @Override
        public void createConsumers(ConsumerAssignment assignment) {
            consumers = assignment;
        }

        @Override
        public void startLoad(ProducerWorkAssignment assignment) {
            this.assignment = assignment;
        }

        @Override
        public CountersStats getCountersStats() {
            long readySent = producers.size();
            long subscriptions =
                    consumers.topicsSubscriptions.stream()
                            .map(subscription -> subscription.topic + ":" + subscription.subscription)
                            .distinct()
                            .count();
            long readyReceived =
                    readySent
                            * subscriptions
                            / consumers.topicsSubscriptions.stream()
                                    .map(subscription -> subscription.topic)
                                    .distinct()
                                    .count();
            if (assignment == null) {
                return new CountersStats(readySent, 0, readyReceived);
            }
            loadPolls++;
            return new CountersStats(
                    readySent + counters.messagesSent(),
                    counters.messageSendErrors(),
                    readyReceived + counters.messagesReceived());
        }

        @Override
        public CumulativeLatencies getCumulativeLatencies() {
            CumulativeLatencies latencies = new CumulativeLatencies();
            latencies.endToEndLatency.recordValue(2_000);
            return latencies;
        }

        @Override
        public void stopAll() {
            stopped = true;
        }

        @Override
        public void close() {}

        @Override
        public void probeProducers() {
            probed = true;
        }

        @Override
        public void adjustPublishRate(double rate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseConsumers() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resumeConsumers() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PeriodStats getPeriodStats() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resetStats() {}

        @Override
        public String id() {
            return "fake";
        }
    }
}
