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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.javalin.Javalin;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

class WorkerHttpRoundTripTest {
    @TempDir Path directory;

    private final Worker worker = mock(Worker.class);
    private final WorkerHandler handler = new WorkerHandler(worker);
    private Javalin app;
    private HttpWorkerClient remote;
    private String host;

    @BeforeEach
    void startServer() {
        app =
                Javalin.create(
                        config -> {
                            config.startup.showJavalinBanner = false;
                            handler.configure(config);
                        });
        app.start("127.0.0.1", 0);
        host = "http://127.0.0.1:" + app.port();
        remote = new HttpWorkerClient(host);
    }

    @AfterEach
    void closeResources() throws Exception {
        try {
            if (remote != null) {
                remote.close();
            }
        } finally {
            try {
                if (app != null) {
                    app.stop();
                }
            } finally {
                handler.close();
                verify(worker).close();
            }
        }
    }

    @Test
    void roundTripsTopicProducerConsumerAndControlEndpoints() {
        TopicsInfo info = new TopicsInfo(2, 7);
        List<String> topics = List.of("topic-α", "topic-\"quoted\"");
        when(worker.createTopics(any())).thenReturn(topics);
        assertThat(remote.id()).isEqualTo(host);
        remote.setRunId("018f0000-0000-7000-8000-000000000001");
        assertThat(remote.createTopics(info)).containsExactlyElementsOf(topics);
        ArgumentCaptor<TopicsInfo> capturedInfo = ArgumentCaptor.forClass(TopicsInfo.class);
        verify(worker).createTopics(capturedInfo.capture());
        verify(worker).setRunId("018f0000-0000-7000-8000-000000000001");
        assertThat(capturedInfo.getValue()).usingRecursiveComparison().isEqualTo(info);

        ConsumerAssignment consumers = new ConsumerAssignment();
        consumers.topicsSubscriptions =
                List.of(
                        new TopicSubscription(topics.get(0), "subscription-1"),
                        new TopicSubscription(topics.get(0), "subscription-2"),
                        new TopicSubscription(topics.get(1), "subscription-1"));
        ProducerWorkAssignment load =
                new ProducerWorkAssignment(
                        List.of(new byte[] {0, 1, -1}, new byte[] {2, 3}),
                        123.5,
                        KeyDistributorType.NO_KEY,
                        new int[] {3, 1});
        remote.createProducers(topics);
        remote.createConsumers(consumers);
        remote.probeProducers();
        remote.startLoad(load);
        remote.adjustPublishRate(456.75);
        remote.pauseConsumers();
        remote.resumeConsumers();
        remote.resetStats();
        remote.stopAll();

        verify(worker).createProducers(topics);
        ArgumentCaptor<ConsumerAssignment> capturedConsumers =
                ArgumentCaptor.forClass(ConsumerAssignment.class);
        verify(worker).createConsumers(capturedConsumers.capture());
        assertThat(capturedConsumers.getValue()).usingRecursiveComparison().isEqualTo(consumers);
        ArgumentCaptor<ProducerWorkAssignment> capturedLoad =
                ArgumentCaptor.forClass(ProducerWorkAssignment.class);
        verify(worker).startLoad(capturedLoad.capture());
        assertThat(capturedLoad.getValue()).usingRecursiveComparison().isEqualTo(load);
        verify(worker).probeProducers();
        verify(worker).adjustPublishRate(456.75);
        verify(worker).pauseConsumers();
        verify(worker).resumeConsumers();
        verify(worker).resetStats();
        verify(worker).stopAll();
        verifyNoMoreInteractions(worker);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void transfersConfigurationBytesAndDeletesServerCopyEvenWhenInitializationFails(boolean fail)
            throws Exception {
        Path config = directory.resolve("driver.yaml");
        byte[] content =
                "name: fixture-α\npassword: \"quoted\"\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(config, content);
        AtomicReference<Path> serverCopy = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            Path path = invocation.getArgument(0);
                            serverCopy.set(path);
                            assertThat(path).isNotEqualTo(config);
                            assertThat(Files.readAllBytes(path)).containsExactly(content);
                            if (fail) {
                                throw new IOException("driver initialization rejected");
                            }
                            return null;
                        })
                .when(worker)
                .initializeDriver(any());

        if (fail) {
            assertThatThrownBy(() -> remote.initializeDriver(config))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("HTTP post " + host + WorkerHandler.INITIALIZE_DRIVER)
                    .hasMessageContaining("code 500: java.io.IOException: driver initialization rejected");
        } else {
            remote.initializeDriver(config);
        }
        assertThat(serverCopy.get()).isNotNull().doesNotExist();
        assertThat(Files.readAllBytes(config)).containsExactly(content);
        verify(worker).initializeDriver(serverCopy.get());
    }

    @Test
    void roundTripsEveryStatisticsFieldAndHistogram() {
        Histogram publish = histogram(11, 33);
        Histogram delay = histogram(22, 44);
        Histogram endToEnd = histogram(55, 77);
        PeriodStats period = new PeriodStats(1, 2, 3, 4, 5, 6, 7, 8, publish, delay, endToEnd);
        CumulativeLatencies cumulative = new CumulativeLatencies();
        cumulative.publishLatency = publish;
        cumulative.publishDelayLatency = delay;
        cumulative.endToEndLatency = endToEnd;
        CountersStats counters = new CountersStats(123, 17, 91);
        when(worker.getPeriodStats()).thenReturn(period);
        when(worker.getCumulativeLatencies()).thenReturn(cumulative);
        when(worker.getCountersStats()).thenReturn(counters);

        assertThat(remote.getPeriodStats()).isEqualTo(period);
        assertThat(remote.getCountersStats()).isEqualTo(counters);
        CumulativeLatencies received = remote.getCumulativeLatencies();
        assertThat(received.publishLatency).isEqualTo(publish);
        assertThat(received.publishDelayLatency).isEqualTo(delay);
        assertThat(received.endToEndLatency).isEqualTo(endToEnd);
        received.publishLatency.reset();
        assertThat(publish.getTotalCount()).isEqualTo(2);
        verify(worker).getPeriodStats();
        verify(worker).getCountersStats();
        verify(worker).getCumulativeLatencies();
        verifyNoMoreInteractions(worker);
    }

    @ParameterizedTest
    @ValueSource(strings = {"topics", "stats", "reset", "stop"})
    void mapsWorkerFailuresToHttpErrorsWithoutCompletionExceptionWrappers(String operation) {
        IllegalStateException failure =
                new IllegalStateException("worker rejected request", new IOException("driver cause"));
        IOException cleanup = new IOException("cleanup failure");
        failure.addSuppressed(cleanup);
        cleanup.addSuppressed(failure);
        String path;
        String method;
        org.assertj.core.api.ThrowableAssert.ThrowingCallable request;
        switch (operation) {
            case "topics" -> {
                when(worker.createTopics(any())).thenThrow(failure);
                path = WorkerHandler.CREATE_TOPICS;
                method = "post";
                request = () -> remote.createTopics(new TopicsInfo(1, 1));
            }
            case "stats" -> {
                when(worker.getPeriodStats()).thenThrow(failure);
                path = WorkerHandler.PERIOD_STATS;
                method = "get";
                request = remote::getPeriodStats;
            }
            case "reset" -> {
                doThrow(failure).when(worker).resetStats();
                path = WorkerHandler.RESET_STATS;
                method = "post";
                request = remote::resetStats;
            }
            default -> {
                doThrow(failure).when(worker).stopAll();
                path = WorkerHandler.STOP_ALL;
                method = "post";
                request = remote::stopAll;
            }
        }
        assertThatThrownBy(request)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "HTTP "
                                + method
                                + " "
                                + host
                                + path
                                + " failed with code 500: java.lang.IllegalStateException: worker rejected request"
                                + " | caused by: java.io.IOException: driver cause"
                                + " | suppressed: java.io.IOException: cleanup failure")
                .hasNoCause();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "temporarily unavailable"})
    void reportsNon500StatusesWithAndWithoutResponseBodies(String body) throws Exception {
        Javalin unavailable =
                Javalin.create(
                        config -> {
                            config.startup.showJavalinBanner = false;
                            config.routes.get(WorkerHandler.COUNTERS_STATS, ctx -> ctx.status(503).result(body));
                            config.routes.post(WorkerHandler.RESET_STATS, ctx -> ctx.status(503).result(body));
                            config.routes.post(WorkerHandler.CREATE_TOPICS, ctx -> ctx.status(503).result(body));
                            config.routes.post(WorkerHandler.STOP_ALL, ctx -> ctx.status(503).result(body));
                        });
        try {
            unavailable.start("127.0.0.1", 0);
            String unavailableHost = "http://127.0.0.1:" + unavailable.port();
            try (HttpWorkerClient client = new HttpWorkerClient(unavailableHost)) {
                String suffix = " failed with code 503" + (body.isEmpty() ? "" : ": " + body);
                assertThatThrownBy(client::getCountersStats)
                        .hasMessage("HTTP get " + unavailableHost + WorkerHandler.COUNTERS_STATS + suffix);
                assertThatThrownBy(client::resetStats)
                        .hasMessage("HTTP post " + unavailableHost + WorkerHandler.RESET_STATS + suffix);
                assertThatThrownBy(() -> client.createTopics(new TopicsInfo(1, 1)))
                        .hasMessage("HTTP post " + unavailableHost + WorkerHandler.CREATE_TOPICS + suffix);
                assertThatThrownBy(client::stopAll)
                        .hasMessage("HTTP post " + unavailableHost + WorkerHandler.STOP_ALL + suffix);
            }
        } finally {
            unavailable.stop();
        }
    }

    @Test
    void passesEmptyPayloadsToWorkerValidation() {
        doThrow(new IllegalArgumentException("payload list must not be empty"))
                .when(worker)
                .startLoad(any());
        assertThatThrownBy(
                        () ->
                                remote.startLoad(
                                        new ProducerWorkAssignment(List.of(), 10, KeyDistributorType.NO_KEY)))
                .hasMessageContaining(
                        "code 500: java.lang.IllegalArgumentException: payload list must not be empty");
        verify(worker).startLoad(any());
    }

    @Test
    void wrapsConnectionFailuresWithTheWorkerAddress() {
        app.stop();
        app = null;
        assertThatThrownBy(remote::getCountersStats)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("HTTP request to worker " + host + " failed")
                .hasCauseInstanceOf(java.net.ConnectException.class);
    }

    @Test
    void handlerPropagatesOwnedWorkerCloseFailures() throws Exception {
        Worker ownedWorker = mock(Worker.class);
        IOException failure = new IOException("worker close failed");
        doThrow(failure).when(ownedWorker).close();
        WorkerHandler ownedHandler = new WorkerHandler(ownedWorker);
        assertThat(catchThrowable(ownedHandler::close)).isSameAs(failure);
        verify(ownedWorker).close();
    }

    private static Histogram histogram(long first, long second) {
        Histogram histogram = new Histogram(5);
        histogram.recordValue(first);
        histogram.recordValue(second);
        return histogram;
    }
}
