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
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface Worker extends AutoCloseable {

    void initializeDriver(Path driverConfigFile) throws Exception;

    /**
     * Initializes the driver, optionally loading it from an explicit plugin bundle directory or jar
     * instead of the installed plugin bundles.
     *
     * @param driverConfigFile the driver configuration file
     * @param driverLocation a plugin bundle directory or jar, or {@code null} to use installed
     *     bundles
     * @throws Exception if the driver cannot be initialized
     */
    default void initializeDriver(Path driverConfigFile, Path driverLocation) throws Exception {
        initializeDriver(driverConfigFile);
    }

    List<String> createTopics(TopicsInfo topicsInfo) throws IOException;

    void createProducers(List<String> topics) throws IOException;

    void createConsumers(ConsumerAssignment consumerAssignment) throws IOException;

    void probeProducers() throws IOException;

    void startLoad(ProducerWorkAssignment producerWorkAssignment) throws IOException;

    void adjustPublishRate(double publishRate) throws IOException;

    void pauseConsumers() throws IOException;

    void resumeConsumers() throws IOException;

    CountersStats getCountersStats() throws IOException;

    PeriodStats getPeriodStats() throws IOException;

    CumulativeLatencies getCumulativeLatencies() throws IOException;

    void resetStats() throws IOException;

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
