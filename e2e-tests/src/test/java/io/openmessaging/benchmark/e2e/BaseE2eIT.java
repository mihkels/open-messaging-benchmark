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

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.TestResult;
import io.openmessaging.benchmark.Workload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for E2E tests using the Open Messaging Benchmark framework.
 */
public abstract class BaseE2eIT {

    protected static final Logger log = LoggerFactory.getLogger(BaseE2eIT.class);

    protected PrometheusMeterRegistry statsLogger;
    protected Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        statsLogger = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        tempDir = Files.createTempDirectory("omb-e2e-test-");
        log.info("Created temporary directory: {}", tempDir);
    }

    @AfterEach
    void tearDown() {
        if (statsLogger != null) {
            statsLogger.close();
        }

        if (tempDir != null) {
            deleteDirectory(tempDir.toFile());
        }
    }

    /**
     * Create a simple workload for testing.
     *
     * @return a configured workload instance
     */
    protected Workload createSimpleWorkload() {
        Workload workload = new Workload();
        workload.name = "e2e-test-workload";
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.messageSize = 1024;
        workload.producersPerTopic = 1;
        workload.consumerPerSubscription = 1;
        workload.subscriptionsPerTopic = 1;
        workload.producerRate = 100;
        workload.consumerBacklogSizeGB = 0;
        workload.testDurationMinutes = 1;
        return workload;
    }

    /**
     * Create a workload configuration file.
     *
     * @param workload the workload configuration
     * @return the created workload file
     * @throws IOException if file creation fails
     */
    protected File createWorkloadFile(Workload workload) throws IOException {
        Path workloadPath = tempDir.resolve("workload.yaml");
        // You would serialize workload to YAML here
        // For now, create a simple workload file
        String yaml = String.format("""
                name: %s
                topics: %d
                partitionsPerTopic: %d
                messageSize: %d

                subscriptionsPerTopic: %d
                consumerPerSubscription: %d
                producersPerTopic: %d
                producerRate: %d
                consumerBacklogSizeGB: %d
                testDurationMinutes: %d
                """,
                workload.name,
                workload.topics,
                workload.partitionsPerTopic,
                workload.messageSize,
                workload.subscriptionsPerTopic,
                workload.consumerPerSubscription,
                workload.producersPerTopic,
                workload.producerRate,
                workload.consumerBacklogSizeGB,
                workload.testDurationMinutes
        );
        Files.writeString(workloadPath, yaml);
        return workloadPath.toFile();
    }

    /**
     * Validate test results.
     *
     * @param result the test result to validate
     * @param workload the workload configuration
     */
    protected void validateResults(TestResult result, Workload workload) {
        var wholeTestDuration = workload.testDurationMinutes + workload.warmupDurationMinutes;

        // Calculate actual aggregate rates
        double aggregatePublishRate = result.publishRate.stream()
                .mapToDouble(Double::doubleValue)
                .sum() / result.publishRate.size();

        double aggregateConsumeRate = result.consumeRate.stream()
                .mapToDouble(Double::doubleValue)
                .sum() / result.consumeRate.size();

        log.info("Test completed - Aggregate publish rate: {} msg/s, consume rate: {} msg/s, throughput: {} Mbit/s",
                aggregatePublishRate,
                aggregateConsumeRate,
                aggregatePublishRate * workload.messageSize * 8.0 / 1024 / 1024);

        // Validate that messages were published
        assertThat(aggregatePublishRate)
                .as("Should have published messages")
                .isGreaterThan(0);

        // Validate that messages were consumed
        assertThat(aggregateConsumeRate)
                .as("Should have consumed messages")
                .isGreaterThan(0);

        // Optional: Validate latencies are reasonable
        assertThat(result.aggregatedPublishLatencyAvg)
                .as("Publish latency should be positive")
                .isGreaterThan(0);

//        assertThat(result.aggregatedEndToEndLatencyAvg)
//                .as("End-to-end latency should be positive")
//                .isGreaterThan(0);
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    if (file.delete()) {
                        log.info("Deleted file {}", file.getAbsolutePath());
                    } else {
                        log.warn("Failed to delete file {}", file.getAbsolutePath());
                    }
                }
            }
        }

        if (directory.delete()) {
            log.info("Deleted directory {}", directory.getAbsolutePath());
        } else {
            log.warn("Failed to delete directory {}", directory.getAbsolutePath());
        }
    }
}
