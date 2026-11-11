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

import com.fasterxml.jackson.annotation.JsonInclude;
import io.openmessaging.benchmark.utils.distributor.KeyDistribution;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import io.openmessaging.benchmark.utils.payload.MessageSizeDistribution;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

public class Workload {
    public @Nullable String name;

    /** Number of topics to create in the test. */
    public int topics;

    /** Number of partitions each topic will contain. */
    public int partitionsPerTopic;

    public KeyDistributorType keyDistributor = KeyDistributorType.NO_KEY;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public @Nullable KeyDistribution keyDistribution;

    public int messageSize;

    /** Optional weighted size buckets; when configured, this takes precedence over messageSize. */
    public @Nullable Map<String, Integer> messageSizeDistribution;

    public boolean usesMessageSizeDistribution() {
        return messageSizeDistribution != null && !messageSizeDistribution.isEmpty();
    }

    public boolean useRandomizedPayloads;
    public double randomBytesRatio;
    public int randomizedPayloadPoolSize;

    public @Nullable String payloadFile;

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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public @Nullable Thresholds thresholds;

    /**
     * One step of a rate profile: the rate moves linearly from the previous stage's target to this
     * one over the duration. The first stage starts at its own target, so it holds.
     *
     * @param durationMinutes the stage length; 0 jumps straight to the target
     * @param targetRate the total msg/s across all producers at the end of the stage
     */
    public record Stage(int durationMinutes, int targetRate) {}

    /** Optional rate profile replacing {@link #producerRate} and {@link #testDurationMinutes}. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public @Nullable List<Stage> stages;

    public boolean usesStages() {
        return stages != null && !stages.isEmpty();
    }

    /**
     * The measured window length.
     *
     * @return the sum of the stage durations, or {@link #testDurationMinutes}
     */
    public int measuredMinutes() {
        List<Stage> profile = stages;
        return profile != null && !profile.isEmpty()
                ? profile.stream().mapToInt(Stage::durationMinutes).sum()
                : testDurationMinutes;
    }

    /**
     * The message size used for backlog and result calculations.
     *
     * @return the distribution's weighted average size, or {@link #messageSize}
     */
    public int effectiveMessageSize() {
        Map<String, Integer> distribution = messageSizeDistribution;
        return distribution != null && !distribution.isEmpty()
                ? new MessageSizeDistribution(distribution).getAvgSize()
                : messageSize;
    }

    /**
     * Rejects settings that would otherwise fail only after broker resources are created.
     *
     * @throws IllegalArgumentException if the workload is invalid
     */
    public void validate() {
        if (thresholds != null) {
            thresholds.validate(this);
        }
        if (useRandomizedPayloads && !usesMessageSizeDistribution() && randomizedPayloadPoolSize <= 0) {
            throw new IllegalArgumentException("Randomized payload pool size must be positive");
        }
        Map<String, Integer> distribution = messageSizeDistribution;
        if (distribution != null && !distribution.isEmpty()) {
            new MessageSizeDistribution(distribution);
        }
        List<Stage> profile = stages;
        if (profile != null && !profile.isEmpty()) {
            if (producerRate != 0) {
                throw new IllegalArgumentException("stages and producerRate are mutually exclusive");
            }
            if (testDurationMinutes != 0) {
                throw new IllegalArgumentException("stages and testDurationMinutes are mutually exclusive");
            }
            if (consumerBacklogSizeGB > 0) {
                throw new IllegalArgumentException("stages cannot be combined with a consumer backlog");
            }
            for (Stage stage : profile) {
                if (stage.targetRate() <= 0) {
                    throw new IllegalArgumentException("Stage targetRate must be positive");
                }
                if (stage.durationMinutes() < 0) {
                    throw new IllegalArgumentException("Stage durationMinutes must not be negative");
                }
            }
            if (measuredMinutes() <= 0) {
                throw new IllegalArgumentException("stages must last at least one minute in total");
            }
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
