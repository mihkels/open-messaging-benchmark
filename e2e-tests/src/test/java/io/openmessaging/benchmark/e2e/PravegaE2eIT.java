package io.openmessaging.benchmark.e2e;

import io.openmessaging.benchmark.TestResult;
import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.WorkloadGenerator;
import io.openmessaging.benchmark.worker.LocalWorker;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for Pravega using a standalone container.
 */
@Testcontainers
class PravegaE2eIT extends BaseE2eIT {

    private static final Logger log = LoggerFactory.getLogger(PravegaE2eIT.class);

    private static final String SCOPE = "ombscope";

    @Container
    static PravegaContainer pravega = new PravegaContainer()
            .waitingFor(
                    Wait.forLogMessage(".*Pravega Sandbox is running.*", 1)
                            .withStartupTimeout(Duration.ofMinutes(5))
            );

    private static File driverConfigFile;

    @BeforeAll
    static void setupDriver() throws Exception {
        pravega.start();
        var host = pravega.getHost();

        log.info("Pravega controller URI: {}", pravega.getControllerURI());
        log.info("Pravega segment store: {}:{}", host, pravega.getSegmentStoreEndpoint());

        verifyPravegaConnection();

        // Fixed config with proper structure from reference file
        String driverConfig = """
            name: Pravega
            driverClass: io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver

            client:
              controllerURI: %s
              scopeName: %s

            writer:
              enableConnectionPooling: true

            includeTimestampInEvent: false
            """.formatted(pravega.getControllerURI(), SCOPE);

        Path configPath = Files.createTempFile("pravega-driver-", ".yaml");
        Files.writeString(configPath, driverConfig);
        driverConfigFile = configPath.toFile();
        driverConfigFile.deleteOnExit();

        log.info("Created Pravega driver config at: {}", driverConfigFile.getAbsolutePath());
        log.info("Config content:\n{}", driverConfig);
    }


    private static void verifyPravegaConnection() {
        var clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(pravega.getControllerURI()))
                .build();
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            boolean created = streamManager.createScope(SCOPE);
            log.info("Scope '{}' ensure/create result: {}", SCOPE, created);
        }
    }

    @AfterAll
    static void tearDownDriver() {
        if (driverConfigFile != null && driverConfigFile.exists()) {
            driverConfigFile.delete();
        }
    }

    @Test
    void testSimpleProduceConsume() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 2;
        workload.producersPerTopic = 2;
        workload.consumerPerSubscription = 2;
        workload.producerRate = 500;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;
        workload.testDurationMinutes = 1;

        TestResult result = runBenchmark(workload);

        // Debug logging before validation
        log.info("Test result - Topics: {}", result.topics);
        log.info("Publish rate size: {}, values: {}", result.publishRate.size(), result.publishRate);
        log.info("Consume rate size: {}, values: {}", result.consumeRate.size(), result.consumeRate);
        log.info("Aggregate publish: {} msg/s", result.publishRate.stream().reduce(0.0, Double::sum));

        validateResults(result, workload);

        var total = result.publishRate.stream().reduce(0.0, Double::sum);
        assertThat(total)
                .as("Should publish expected volume")
                .isGreaterThan(workload.producerRate * workload.testDurationMinutes * 0.7);
    }

    @Test
    void testMultipleStreams() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 3;
        workload.partitionsPerTopic = 2;
        workload.producerRate = 300;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;
        workload.testDurationMinutes = 1;

        TestResult result = runBenchmark(workload);
        validateResults(result, workload);
        assertThat(result.topics).isEqualTo(workload.topics);
    }

    @Test
    void testHighThroughput() throws Exception {
        Workload workload = createSimpleWorkload();
        workload.topics = 1;
        workload.partitionsPerTopic = 4;
        workload.messageSize = 200;
        workload.producersPerTopic = 4;
        workload.consumerPerSubscription = 4;
        workload.producerRate = 800;
        workload.useRandomizedPayloads = true;
        workload.randomizedPayloadPoolSize = 10;
        workload.randomBytesRatio = 0.5;
        workload.testDurationMinutes = 1;

        TestResult result = runBenchmark(workload);
        validateResults(result, workload);

        double total = result.publishRate.stream().reduce(0.0, Double::sum);
        double actualRate = total / workload.testDurationMinutes;
        log.info("Actual Pravega publish rate: {} msg/s", actualRate);
        assertThat(actualRate).isGreaterThan(workload.producerRate * 0.5);
    }

    private TestResult runBenchmark(Workload workload) throws Exception {
        var worker = new LocalWorker(statsLogger);
        try {
            worker.initializeDriver(driverConfigFile);
            WorkloadGenerator generator = new WorkloadGenerator("Pravega", workload, worker);
            TestResult result = generator.run();
            generator.close();
            return result;
        } finally {
            worker.stopAll();
            worker.close();
        }
    }
}


