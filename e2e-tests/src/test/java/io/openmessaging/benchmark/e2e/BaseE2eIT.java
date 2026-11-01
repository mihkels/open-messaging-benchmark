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

import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.worker.ProcessWorker;
import java.nio.file.Path;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared workload, lifecycle and result assertions for broker integration tests. */
@ExtendWith(DriverCoverageExtension.class)
public abstract class BaseE2eIT {

    protected static final Logger log = LoggerFactory.getLogger(BaseE2eIT.class);

    @TempDir protected Path tempDir;

    protected Workload createSimpleWorkload() {
        Workload workload = new Workload();
        workload.name = "e2e-test-workload";
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.messageSize = 1024;
        workload.producersPerTopic = 1;
        workload.consumerPerSubscription = 1;
        workload.subscriptionsPerTopic = 1;
        workload.producerRate = 20;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;
        return workload;
    }

    protected SmokeResult runBenchmark(String driver, Path config, Workload workload)
            throws Exception {
        log.info("Checking {} message delivery", driver);
        try (ProcessWorker worker = new ProcessWorker()) {
            worker.initializeDriver(config);
            return FunctionalSmoke.run(worker, workload);
        }
    }

    protected void validateResults(SmokeResult result, Workload workload) {
        assertThat(result).as("smoke result").isNotNull();
        assertThat(workload.topics).as("topics").isPositive();
        assertThat(workload.subscriptionsPerTopic).as("subscriptions per topic").isPositive();
        assertThat(result.messagesSent())
                .as("published messages")
                .isGreaterThanOrEqualTo(FunctionalSmoke.minimumMessages(workload));
        assertThat(result.messagesReceived())
                .as("delivered messages")
                .isGreaterThanOrEqualTo(
                        FunctionalSmoke.minimumMessages(workload) * workload.subscriptionsPerTopic);
        assertThat(result.messageSendErrors()).as("publish errors").isZero();
        assertThat(result.endToEndLatencyMillis()).as("end-to-end latency").isFinite().isNotNegative();
    }
}
