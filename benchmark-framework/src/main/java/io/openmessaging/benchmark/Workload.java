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
package io.openmessaging.benchmark;

import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import io.openmessaging.benchmark.utils.payload.MessageSizeDistribution;
import java.util.Map;

public class Workload {
    public String name;

    /** Number of topics to create in the test. */
    public int topics;

    /** Number of partitions each topic will contain. */
    public int partitionsPerTopic;

    public KeyDistributorType keyDistributor = KeyDistributorType.NO_KEY;

    public int messageSize;

    /** Optional weighted size buckets; when configured, this takes precedence over messageSize. */
    public Map<String, Integer> messageSizeDistribution;

    public boolean usesMessageSizeDistribution() {
        return messageSizeDistribution != null && !messageSizeDistribution.isEmpty();
    }

    public boolean useRandomizedPayloads;
    public double randomBytesRatio;
    public int randomizedPayloadPoolSize;

    public String payloadFile;

    public int subscriptionsPerTopic;

    public int producersPerTopic;

    public int consumerPerSubscription;

    public int producerRate;

    /**
     * If the consumer backlog is > 0, the generator will accumulate messages until the requested
     * amount of storage is retained and then it will start the consumers to drain it.
     *
     * <p>The testDurationMinutes will be overruled to allow the test to complete when the consumer
     * has drained all the backlog and it's on par with the producer
     */
    public long consumerBacklogSizeGB = 0;

    /**
     * The ratio of the backlog that can remain and yet the backlog still be considered empty, and
     * thus the workload can complete at the end of the configured duration. In some systems it is not
     * feasible for the backlog to be drained fully and thus the workload will run indefinitely. In
     * such circumstances, one may be content to achieve a partial drain such as 99% of the backlog.
     * The value should be on somewhere between 0.0 and 1.0, where 1.0 indicates that the backlog
     * should be fully drained, and 0.0 indicates a best effort, where the workload will complete
     * after the specified time irrespective of how much of the backlog has been drained.
     */
    public double backlogDrainRatio = 1.0;

    public int testDurationMinutes;

    public int warmupDurationMinutes = 1;

    /**
     * The message size used for backlog and result calculations.
     *
     * @return the distribution's weighted average size, or {@link #messageSize}
     */
    public int effectiveMessageSize() {
        return usesMessageSizeDistribution()
                ? new MessageSizeDistribution(messageSizeDistribution).getAvgSize()
                : messageSize;
    }

    /**
     * Rejects settings that would otherwise fail only after broker resources are created.
     *
     * @throws IllegalArgumentException if the workload is invalid
     */
    public void validate() {
        if (useRandomizedPayloads && !usesMessageSizeDistribution() && randomizedPayloadPoolSize <= 0) {
            throw new IllegalArgumentException("Randomized payload pool size must be positive");
        }
        if (usesMessageSizeDistribution()) {
            new MessageSizeDistribution(messageSizeDistribution);
        }
        if (consumerBacklogSizeGB > 0) {
            if (producerRate == 0) {
                throw new IllegalArgumentException(
                        "Cannot probe producer sustainable rate when building backlog");
            }
            if (effectiveMessageSize() <= 0) {
                throw new IllegalArgumentException("Message size must be positive when building a backlog");
            }
        }
    }
}
