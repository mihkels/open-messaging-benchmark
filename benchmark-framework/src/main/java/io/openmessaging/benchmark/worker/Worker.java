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

import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.nio.file.Path;
import java.util.List;

public interface Worker extends AutoCloseable {

    /**
     * Initializes the driver selected by a driver configuration file. Where the driver is loaded from
     * is decided by the worker itself, for example by {@link ProcessWorker}'s driver location.
     *
     * @param driverConfigFile the driver configuration file
     * @throws Exception if the driver cannot be initialized
     */
    void initializeDriver(Path driverConfigFile) throws Exception;

    List<String> createTopics(TopicsInfo topicsInfo);

    void createProducers(List<String> topics);

    void createConsumers(ConsumerAssignment consumerAssignment);

    void probeProducers();

    void startLoad(ProducerWorkAssignment producerWorkAssignment);

    void adjustPublishRate(double publishRate);

    void pauseConsumers();

    void resumeConsumers();

    CountersStats getCountersStats();

    PeriodStats getPeriodStats();

    CumulativeLatencies getCumulativeLatencies();

    void resetStats();

    void stopAll();

    /**
     * Whether cleanup has left this worker in a known state suitable for another driver. The
     * conservative default prevents reusing remote or custom workers after a cleanup failure.
     *
     * @return true only when reuse after a failed cleanup is known to be safe
     */
    default boolean canReuseAfterFailure() {
        return false;
    }

    String id();
}
