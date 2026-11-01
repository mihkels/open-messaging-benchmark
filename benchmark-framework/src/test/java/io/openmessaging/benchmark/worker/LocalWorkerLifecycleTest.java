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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.driver.AbandonableCreation;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreator;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalWorkerLifecycleTest {
    @TempDir Path directory;

    @AfterEach
    void clearFixtures() {
        FixtureDriver.fixtures.clear();
    }

    @Test
    void interruptsRateWaitAndWaitsForOutstandingCompletion() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CompletableFuture<Void> send = new CompletableFuture<>();
        CountDownLatch sent = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        when(producer.sendAsync(any(), any()))
                .thenAnswer(
                        ignored -> {
                            sent.countDown();
                            return send;
                        });
        doAnswer(
                        ignored -> {
                            closed.countDown();
                            return null;
                        })
                .when(producer)
                .close();
        Scenario scenario = new Scenario(List.of(producer));
        try (LocalWorker worker = worker(scenario, Duration.ofSeconds(2))) {
            worker.createProducers(List.of("topic"));
            worker.startLoad(assignment());
            assertThat(sent.await(1, TimeUnit.SECONDS)).isTrue();

            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread stopper = thread(() -> worker.stopAll(), failure);
            assertThat(closed.await(1, TimeUnit.SECONDS)).isTrue();
            stopper.join(100);
            assertThat(stopper.isAlive()).isTrue();
            send.complete(null);
            stopper.join(5000);
            assertThat(stopper.isAlive()).isFalse();
            assertThat(failure.get()).isNull();
            verify(producer, times(1)).sendAsync(any(), any());
            worker.stopAll();
            verify(producer, times(1)).close();
        }
    }

    @Test
    void ignoresCallbacksFromAnEarlierRun() throws Exception {
        BenchmarkConsumer firstConsumer = mock(BenchmarkConsumer.class);
        Scenario first = new Scenario(List.of());
        first.consumers = List.of(firstConsumer);
        try (LocalWorker worker = worker(first, Duration.ofSeconds(2))) {
            worker.createConsumers(assignmentForConsumer());
            ConsumerCallback oldCallback = first.callback;
            oldCallback.messageReceived(new byte[8], System.currentTimeMillis());
            assertThat(worker.getCountersStats().messagesReceived()).isEqualTo(1);
            worker.stopAll();

            Scenario second = new Scenario(List.of());
            initialize(worker, second);
            oldCallback.messageReceived(new byte[8], System.currentTimeMillis());
            assertThat(worker.getCountersStats().messagesReceived()).isZero();
            worker.createConsumers(assignmentForConsumer());
            second.callback.messageReceived(new byte[8], System.currentTimeMillis());
            assertThat(worker.getCountersStats().messagesReceived()).isEqualTo(1);
        }
    }

    @Test
    void resumesPausedCallbacksBeforeReset() throws Exception {
        Scenario scenario = new Scenario(List.of());
        scenario.consumers = List.of(mock(BenchmarkConsumer.class));
        try (LocalWorker worker = worker(scenario, Duration.ofSeconds(2))) {
            worker.createConsumers(assignmentForConsumer());
            worker.pauseConsumers();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread callback =
                    thread(
                            () -> scenario.callback.messageReceived(new byte[8], System.currentTimeMillis()),
                            failure);
            waitUntil(() -> worker.getCountersStats().messagesReceived() == 1);
            assertThat(callback.isAlive()).isTrue();
            worker.stopAll();
            callback.join(1000);
            assertThat(callback.isAlive()).isFalse();
            assertThat(failure.get()).isNull();
        }
    }

    @Test
    void waitsForCreationTaskToExitAndClosesItsLateResult() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Scenario scenario = new Scenario(List.of());
        scenario.createProducers =
                () -> {
                    entered.countDown();
                    awaitIgnoringInterrupts(release);
                    return CompletableFuture.completedFuture(List.of(producer));
                };
        try (LocalWorker worker = worker(scenario, Duration.ofSeconds(2))) {
            AtomicReference<Throwable> creationFailure = new AtomicReference<>();
            Thread creation = thread(() -> worker.createProducers(List.of("topic")), creationFailure);
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            AtomicReference<Throwable> stopFailure = new AtomicReference<>();
            Thread stopper = thread(worker::stopAll, stopFailure);
            stopper.join(100);
            assertThat(stopper.isAlive()).isTrue();
            release.countDown();
            stopper.join(5000);
            creation.join(1000);
            assertThat(stopper.isAlive()).isFalse();
            assertThat(creation.isAlive()).isFalse();
            assertThat(creationFailure.get()).isNotNull();
            assertThat(stopFailure.get()).isNull();
            verify(producer, times(1)).close();
        } finally {
            release.countDown();
        }
    }

    @Test
    void closesLateAsynchronousCreationWithoutDiscardingItsFuture() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CompletableFuture<List<BenchmarkProducer>> creation = new CompletableFuture<>();
        Scenario scenario = new Scenario(List.of());
        scenario.createProducers = () -> creation;
        try (LocalWorker worker = worker(scenario, Duration.ofSeconds(2))) {
            AtomicReference<Throwable> creationFailure = new AtomicReference<>();
            Thread creator = thread(() -> worker.createProducers(List.of("topic")), creationFailure);
            waitUntil(() -> scenario.producerCreationStarted.get() == 1);
            AtomicReference<Throwable> stopFailure = new AtomicReference<>();
            Thread stopper = thread(worker::stopAll, stopFailure);
            creator.join(1000);
            assertThat(creator.isAlive()).isFalse();
            assertThat(creation.isCancelled()).isFalse();
            stopper.join(100);
            assertThat(stopper.isAlive()).isTrue();
            creation.complete(List.of(producer));
            stopper.join(5000);
            creator.join(1000);
            assertThat(stopFailure.get()).isNull();
            assertThat(stopper.isAlive()).isFalse();
            assertThat(creationFailure.get()).isNotNull();
            verify(producer, times(1)).close();
        }
    }

    @Test
    void requestsCooperativeCreationAbandonmentAndWaitsForItsResult() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CountDownLatch abandoned = new CountDownLatch(1);
        class CooperativeCreation extends CompletableFuture<List<BenchmarkProducer>>
                implements AbandonableCreation {
            @Override
            public void abandonCreation() {
                abandoned.countDown();
            }
        }
        CooperativeCreation creation = new CooperativeCreation();
        Scenario scenario = new Scenario(List.of());
        scenario.createProducers = () -> creation;
        try (LocalWorker worker = worker(scenario, Duration.ofSeconds(2))) {
            AtomicReference<Throwable> creationFailure = new AtomicReference<>();
            Thread creator = thread(() -> worker.createProducers(List.of("topic")), creationFailure);
            waitUntil(() -> scenario.producerCreationStarted.get() == 1);
            AtomicReference<Throwable> stopFailure = new AtomicReference<>();
            Thread stopper = thread(worker::stopAll, stopFailure);
            assertThat(abandoned.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.isCancelled()).isFalse();
            stopper.join(100);
            assertThat(stopper.isAlive()).isTrue();
            creation.complete(List.of(producer));
            stopper.join(5000);
            creator.join(1000);
            assertThat(stopFailure.get()).isNull();
            assertThat(stopper.isAlive()).isFalse();
            assertThat(creationFailure.get()).isNotNull();
            verify(producer, times(1)).close();
        }
    }

    @Test
    void reportsCleanupFailuresSuppressedOnAbandonedLegacyCreation() throws Exception {
        CountDownLatch dispatched = new CountDownLatch(1);
        class CooperativeCreation extends CompletableFuture<List<BenchmarkProducer>>
                implements AbandonableCreation {
            @Override
            public void abandonCreation() {
                var cancellation = new java.util.concurrent.CancellationException("creation abandoned");
                cancellation.addSuppressed(new IOException("partial producer close failed"));
                completeExceptionally(cancellation);
            }
        }
        CooperativeCreation creation = new CooperativeCreation();
        Scenario scenario = new Scenario(List.of());
        scenario.createProducers =
                () -> {
                    dispatched.countDown();
                    return creation;
                };
        try (LocalWorker worker = worker(scenario, Duration.ofSeconds(2))) {
            AtomicReference<Throwable> creationFailure = new AtomicReference<>();
            Thread creator = thread(() -> worker.createProducers(List.of("topic")), creationFailure);
            assertThat(dispatched.await(1, TimeUnit.SECONDS)).isTrue();
            Throwable stopFailure = catchThrowable(worker::stopAll);
            creator.join(1000);
            assertThat(stopFailure).hasStackTraceContaining("partial producer close failed");
            assertThat(worker.canReuseAfterFailure()).isFalse();
            assertThat(creationFailure.get()).isNotNull();
        }
    }

    @Test
    void timesOutPendingLegacyCreationWithoutDiscardingItsLateResult() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CompletableFuture<List<BenchmarkProducer>> creation = new CompletableFuture<>();
        Scenario scenario = new Scenario(List.of());
        scenario.createProducers = () -> creation;
        try (LocalWorker worker = worker(scenario, Duration.ofMillis(150))) {
            AtomicReference<Throwable> creationFailure = new AtomicReference<>();
            Thread creator = thread(() -> worker.createProducers(List.of("topic")), creationFailure);
            waitUntil(() -> scenario.producerCreationStarted.get() == 1);
            Throwable stopFailure = catchThrowable(worker::stopAll);
            creator.join(1000);
            assertThat(stopFailure)
                    .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class)
                    .hasStackTraceContaining("Resource creation callbacks did not finish");
            assertThat(worker.canReuseAfterFailure()).isFalse();
            assertThat(creation.isCancelled()).isFalse();
            creation.complete(List.of(producer));
            verify(producer, times(1)).close();
            assertThat(creationFailure.get()).isNotNull();
        }
    }

    @Test
    void waitsForCancelledResourceBatchToDisposeItsLateCreation() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CompletableFuture<BenchmarkProducer> delayed = new CompletableFuture<>();
        CountDownLatch dispatched = new CountDownLatch(1);
        ResourceCreator<String, BenchmarkProducer> batch =
                new ResourceCreator<>(
                        "producer",
                        1,
                        0,
                        0,
                        topic -> {
                            dispatched.countDown();
                            return delayed;
                        });
        AtomicReference<CompletableFuture<List<BenchmarkProducer>>> result = new AtomicReference<>();
        Scenario scenario = new Scenario(List.of());
        scenario.createProducers =
                () -> {
                    CompletableFuture<List<BenchmarkProducer>> created = batch.create(List.of("topic"));
                    result.set(created);
                    return created;
                };
        try (LocalWorker worker = worker(scenario, Duration.ofSeconds(2))) {
            AtomicReference<Throwable> creationFailure = new AtomicReference<>();
            Thread creator = thread(() -> worker.createProducers(List.of("topic")), creationFailure);
            assertThat(dispatched.await(1, TimeUnit.SECONDS)).isTrue();
            AtomicReference<Throwable> stopFailure = new AtomicReference<>();
            Thread stopper = thread(worker::stopAll, stopFailure);
            waitUntil(() -> result.get() != null && result.get().isDone());
            stopper.join(100);
            assertThat(stopper.isAlive()).isTrue();
            delayed.complete(producer);
            stopper.join(5000);
            creator.join(1000);
            assertThat(stopFailure.get()).isNull();
            assertThat(stopper.isAlive()).isFalse();
            assertThat(creationFailure.get()).isNotNull();
            assertThat(worker.canReuseAfterFailure()).isTrue();
            verify(producer, times(1)).close();
        }
    }

    @Test
    void reportsFailureClosingCancelledResourceBatchLateResult() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        doThrow(new IOException("late producer close failed")).when(producer).close();
        CompletableFuture<BenchmarkProducer> delayed = new CompletableFuture<>();
        CountDownLatch dispatched = new CountDownLatch(1);
        ResourceCreator<String, BenchmarkProducer> batch =
                new ResourceCreator<>(
                        "producer",
                        1,
                        0,
                        0,
                        topic -> {
                            dispatched.countDown();
                            return delayed;
                        });
        AtomicReference<CompletableFuture<List<BenchmarkProducer>>> result = new AtomicReference<>();
        Scenario scenario = new Scenario(List.of());
        scenario.createProducers =
                () -> {
                    CompletableFuture<List<BenchmarkProducer>> created = batch.create(List.of("topic"));
                    result.set(created);
                    return created;
                };
        try (LocalWorker worker = worker(scenario, Duration.ofSeconds(2))) {
            AtomicReference<Throwable> creationFailure = new AtomicReference<>();
            Thread creator = thread(() -> worker.createProducers(List.of("topic")), creationFailure);
            assertThat(dispatched.await(1, TimeUnit.SECONDS)).isTrue();
            AtomicReference<Throwable> stopFailure = new AtomicReference<>();
            Thread stopper = thread(worker::stopAll, stopFailure);
            waitUntil(() -> result.get() != null && result.get().isDone());
            delayed.complete(producer);
            stopper.join(5000);
            creator.join(1000);
            assertThat(stopper.isAlive()).isFalse();
            assertThat(stopFailure.get()).hasStackTraceContaining("late producer close failed");
            assertThat(creationFailure.get()).isNotNull();
            assertThat(worker.canReuseAfterFailure()).isFalse();
            verify(producer, times(1)).close();
        }
    }

    @Test
    void timesOutCancelledResourceBatchThatCannotQuiesce() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CompletableFuture<BenchmarkProducer> delayed = new CompletableFuture<>();
        CountDownLatch dispatched = new CountDownLatch(1);
        ResourceCreator<String, BenchmarkProducer> batch =
                new ResourceCreator<>(
                        "producer",
                        1,
                        0,
                        0,
                        topic -> {
                            dispatched.countDown();
                            return delayed;
                        });
        Scenario scenario = new Scenario(List.of());
        scenario.createProducers = () -> batch.create(List.of("topic"));
        try (LocalWorker worker = worker(scenario, Duration.ofMillis(200))) {
            AtomicReference<Throwable> creationFailure = new AtomicReference<>();
            Thread creator = thread(() -> worker.createProducers(List.of("topic")), creationFailure);
            assertThat(dispatched.await(1, TimeUnit.SECONDS)).isTrue();
            Throwable stopFailure = catchThrowable(worker::stopAll);
            creator.join(1000);
            assertThat(stopFailure)
                    .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class)
                    .hasStackTraceContaining("Resource creation callbacks did not finish");
            assertThat(worker.canReuseAfterFailure()).isFalse();
            delayed.complete(producer);
            verify(producer, times(1)).close();
            assertThat(creationFailure.get()).isNotNull();
        }
    }

    @Test
    void rejectsReuseAfterOutstandingCallbacksTimeOut() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CompletableFuture<Void> send = new CompletableFuture<>();
        CountDownLatch sent = new CountDownLatch(1);
        when(producer.sendAsync(any(), any()))
                .thenAnswer(
                        ignored -> {
                            sent.countDown();
                            return send;
                        });
        Scenario scenario = new Scenario(List.of(producer));
        try (LocalWorker worker = worker(scenario, Duration.ofMillis(150))) {
            worker.createProducers(List.of("topic"));
            worker.startLoad(assignment());
            assertThat(sent.await(1, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(worker::stopAll)
                    .hasRootCauseMessage("Outstanding send or consumer callbacks did not finish");
            assertThatThrownBy(() -> initialize(worker, new Scenario(List.of())))
                    .hasMessageContaining("could not be safely stopped");
            send.complete(null);
        }
    }

    @Test
    void boundsCallbackGateSealingWhenAStatisticsUpdateStalls() throws Exception {
        WorkerStats stats = new WorkerStats(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
        RunCallbacks callbacks = new RunCallbacks(stats, ignored -> {});
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        assertThat(callbacks.beginSend()).isTrue();
        Thread completion =
                Thread.ofVirtual()
                        .start(
                                () ->
                                        callbacks.completeSend(
                                                8,
                                                0,
                                                0,
                                                () -> {
                                                    entered.countDown();
                                                    awaitIgnoringInterrupts(release);
                                                    return 0L;
                                                },
                                                null));
        try {
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(150);
            assertThat(callbacks.seal(deadline)).isFalse();
        } finally {
            release.countDown();
            completion.join(2000);
        }
        assertThat(completion.isAlive()).isFalse();
        assertThat(callbacks.awaitQuiescence(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))).isTrue();
    }

    @Test
    void sealingReleasesPausedConsumersButStillWaitsForOutstandingSends() throws Exception {
        WorkerStats stats = new WorkerStats(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        RunCallbacks callbacks = new RunCallbacks(stats, failure::set);
        callbacks.pause();
        assertThat(callbacks.beginSend()).isTrue();
        Thread receiver = thread(() -> callbacks.receive(8, System.currentTimeMillis()), failure);
        boolean sendFinished = false;
        try {
            waitUntil(() -> stats.toCountersStats().messagesReceived() == 1);
            assertThat(receiver.isAlive()).isTrue();
            assertThat(callbacks.awaitQuiescence(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50)))
                    .isFalse();
            assertThat(callbacks.seal(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))).isTrue();
            receiver.join(1000);
            assertThat(receiver.isAlive()).isFalse();
            assertThat(callbacks.awaitQuiescence(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50)))
                    .isFalse();
            callbacks.abortSend();
            sendFinished = true;
            assertThat(callbacks.awaitQuiescence(System.nanoTime() + TimeUnit.SECONDS.toNanos(1)))
                    .isTrue();
            assertThat(callbacks.beginSend()).isFalse();
            assertThat(failure.get()).isNull();
        } finally {
            callbacks.resume();
            if (!sendFinished) {
                callbacks.abortSend();
            }
            receiver.join(1000);
        }
    }

    @Test
    void reportsAnInterruptIgnoringProducerAndForbidsReuse() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(producer.sendAsync(any(), any()))
                .thenAnswer(
                        ignored -> {
                            entered.countDown();
                            awaitIgnoringInterrupts(release);
                            return CompletableFuture.completedFuture(null);
                        });
        LocalWorker worker = worker(new Scenario(List.of(producer)), Duration.ofMillis(150));
        try {
            worker.createProducers(List.of("topic"));
            worker.startLoad(assignment());
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            Throwable failure = catchThrowable(worker::stopAll);
            assertThat(messages(failure)).contains("producer task did not exit after cancellation");
            assertThat(worker.canReuseAfterFailure()).isFalse();
            verify(producer).close();
        } finally {
            release.countDown();
            worker.close();
        }
    }

    @Test
    void propagatesUnexpectedTaskFailureButCountsOrdinarySendFailure() throws Exception {
        BenchmarkProducer broken = mock(BenchmarkProducer.class);
        when(broken.sendAsync(any(), any())).thenThrow(new IllegalStateException("unexpected send"));
        try (LocalWorker worker = worker(new Scenario(List.of(broken)), Duration.ofSeconds(2))) {
            worker.createProducers(List.of("topic"));
            worker.startLoad(assignment());
            waitUntil(
                    () -> {
                        try {
                            worker.getCountersStats();
                            return false;
                        } catch (IllegalStateException e) {
                            return e.getMessage().contains("Producer or callback task failed");
                        }
                    });
            worker.stopAll();

            BenchmarkProducer failedSend = mock(BenchmarkProducer.class);
            when(failedSend.sendAsync(any(), any()))
                    .thenReturn(CompletableFuture.failedFuture(new IOException("ordinary failure")));
            initialize(worker, new Scenario(List.of(failedSend)));
            worker.createProducers(List.of("topic"));
            worker.startLoad(assignment());
            waitUntil(() -> worker.getCountersStats().messageSendErrors() == 1);
            assertThat(worker.getCountersStats().messagesSent()).isZero();
        }
    }

    @Test
    void readinessFailsPromptlyEvenIfAnotherProbeNeverCompletes() throws Exception {
        BenchmarkProducer waiting = mock(BenchmarkProducer.class);
        BenchmarkProducer failing = mock(BenchmarkProducer.class);
        CompletableFuture<Void> pending = new CompletableFuture<>();
        when(waiting.sendAsync(any(), any())).thenReturn(pending);
        when(failing.sendAsync(any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new IOException("probe rejected")));
        doAnswer(
                        ignored -> {
                            pending.completeExceptionally(new IOException("closed"));
                            return null;
                        })
                .when(waiting)
                .close();
        try (LocalWorker worker =
                worker(new Scenario(List.of(waiting, failing)), Duration.ofSeconds(2))) {
            worker.createProducers(List.of("first", "second"));
            long start = System.nanoTime();
            assertThatThrownBy(worker::probeProducers).hasRootCauseMessage("probe rejected");
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));
        }
    }

    @Test
    void failedProbeIsNotBlockedByAnotherProducersSendCall() throws Exception {
        BenchmarkProducer failing = mock(BenchmarkProducer.class);
        BenchmarkProducer waiting = mock(BenchmarkProducer.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(failing.sendAsync(any(), any()))
                .thenAnswer(
                        ignored -> {
                            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
                            return CompletableFuture.failedFuture(new IOException("probe rejected"));
                        });
        when(waiting.sendAsync(any(), any()))
                .thenAnswer(
                        ignored -> {
                            entered.countDown();
                            awaitIgnoringInterrupts(release);
                            return CompletableFuture.completedFuture(null);
                        });
        LocalWorker worker = worker(new Scenario(List.of(failing, waiting)), Duration.ofSeconds(2));
        try {
            worker.createProducers(List.of("first", "second"));
            long start = System.nanoTime();
            assertThatThrownBy(worker::probeProducers).hasRootCauseMessage("probe rejected");
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));
        } finally {
            release.countDown();
            worker.close();
        }
    }

    @Test
    void boundsAReadinessProbeWhoseSendCallBlocks() throws Exception {
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CountDownLatch release = new CountDownLatch(1);
        when(producer.sendAsync(any(), any()))
                .thenAnswer(
                        ignored -> {
                            awaitIgnoringInterrupts(release);
                            return CompletableFuture.completedFuture(null);
                        });
        LocalWorker worker =
                new LocalWorker(
                        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT),
                        Duration.ofSeconds(2),
                        Duration.ofMillis(150));
        initialize(worker, new Scenario(List.of(producer)));
        try {
            worker.createProducers(List.of("topic"));
            assertThatThrownBy(worker::probeProducers)
                    .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class);
        } finally {
            release.countDown();
            worker.close();
        }
    }

    @Test
    void attemptsEveryCloseAndPreservesFailures() throws Exception {
        BenchmarkProducer first = mock(BenchmarkProducer.class);
        BenchmarkProducer second = mock(BenchmarkProducer.class);
        BenchmarkConsumer consumer = mock(BenchmarkConsumer.class);
        doThrow(new IOException("first close")).when(first).close();
        doThrow(new IOException("second close")).when(second).close();
        doThrow(new IOException("consumer close")).when(consumer).close();
        Scenario scenario = new Scenario(List.of(first, second));
        scenario.consumers = List.of(consumer);
        scenario.closeFailure = new IOException("driver close");
        try (LocalWorker worker = worker(scenario, Duration.ofSeconds(2))) {
            worker.createProducers(List.of("first", "second"));
            worker.createConsumers(assignmentForConsumer());
            Throwable failure = catchThrowable(worker::stopAll);
            assertThat(messages(failure))
                    .contains("first close", "second close", "consumer close", "driver close");
            verify(first).close();
            verify(second).close();
            verify(consumer).close();
            assertThat(scenario.closeCount.get()).isEqualTo(1);
            worker.stopAll();
        }
    }

    @Test
    void allowsResourceAndDriverClosesTheirOwnGracePeriods() throws Exception {
        BenchmarkConsumer consumer = mock(BenchmarkConsumer.class);
        CountDownLatch consumerEntered = new CountDownLatch(1);
        CountDownLatch consumerRelease = new CountDownLatch(1);
        CountDownLatch driverEntered = new CountDownLatch(1);
        CountDownLatch driverRelease = new CountDownLatch(1);
        doAnswer(
                        ignored -> {
                            consumerEntered.countDown();
                            consumerRelease.await();
                            return null;
                        })
                .when(consumer)
                .close();
        Scenario scenario = new Scenario(List.of());
        scenario.consumers = List.of(consumer);
        scenario.onClose =
                () -> {
                    driverEntered.countDown();
                    awaitIgnoringInterrupts(driverRelease);
                };
        LocalWorker worker =
                new LocalWorker(
                        new WorkerStats(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)),
                        Duration.ofMillis(100),
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(2));
        initialize(worker, scenario);
        try {
            worker.createConsumers(assignmentForConsumer());
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread stopper = thread(worker::stopAll, failure);
            assertThat(consumerEntered.await(2, TimeUnit.SECONDS)).isTrue();
            stopper.join(250);
            assertThat(stopper.isAlive()).isTrue();
            consumerRelease.countDown();
            assertThat(driverEntered.await(2, TimeUnit.SECONDS)).isTrue();
            stopper.join(250);
            assertThat(stopper.isAlive()).isTrue();
            driverRelease.countDown();
            stopper.join(3000);
            assertThat(stopper.isAlive()).isFalse();
            assertThat(failure.get()).isNull();
            verify(consumer).close();
            assertThat(scenario.closeCount.get()).isEqualTo(1);
        } finally {
            consumerRelease.countDown();
            driverRelease.countDown();
            worker.close();
        }
    }

    @Test
    void appliesResourceCloseDeadlineIndependentlyOfTaskDeadline() throws Exception {
        BenchmarkConsumer consumer = mock(BenchmarkConsumer.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        doAnswer(
                        ignored -> {
                            entered.countDown();
                            try {
                                awaitIgnoringInterrupts(release);
                            } finally {
                                exited.countDown();
                            }
                            return null;
                        })
                .when(consumer)
                .close();
        Scenario scenario = new Scenario(List.of());
        scenario.consumers = List.of(consumer);
        LocalWorker worker =
                new LocalWorker(
                        new WorkerStats(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)),
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(2),
                        Duration.ofMillis(200));
        initialize(worker, scenario);
        try {
            worker.createConsumers(assignmentForConsumer());
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread stopper = thread(worker::stopAll, failure);
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            stopper.join(1200);
            assertThat(stopper.isAlive()).isFalse();
            assertThat(messages(failure.get()))
                    .anyMatch(message -> message != null && message.contains("close timed out"));
            assertThat(scenario.closeCount.get()).isEqualTo(1);
            assertThat(worker.canReuseAfterFailure()).isFalse();
        } finally {
            release.countDown();
            assertThat(exited.await(2, TimeUnit.SECONDS)).isTrue();
            worker.close();
        }
    }

    @Test
    void timesOutBlockingProducerAndConsumerClosesButStillClosesOtherResources() throws Exception {
        BenchmarkProducer blockedProducer = mock(BenchmarkProducer.class);
        BenchmarkProducer healthyProducer = mock(BenchmarkProducer.class);
        BenchmarkConsumer blockedConsumer = mock(BenchmarkConsumer.class);
        BenchmarkConsumer healthyConsumer = mock(BenchmarkConsumer.class);
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(2);
        doAnswer(
                        ignored -> {
                            entered.countDown();
                            try {
                                awaitIgnoringInterrupts(release);
                            } finally {
                                exited.countDown();
                            }
                            return null;
                        })
                .when(blockedProducer)
                .close();
        doAnswer(
                        ignored -> {
                            entered.countDown();
                            try {
                                awaitIgnoringInterrupts(release);
                            } finally {
                                exited.countDown();
                            }
                            return null;
                        })
                .when(blockedConsumer)
                .close();
        Scenario scenario = new Scenario(List.of(blockedProducer, healthyProducer));
        scenario.consumers = List.of(blockedConsumer, healthyConsumer);
        LocalWorker worker = worker(scenario, Duration.ofMillis(500));
        try {
            worker.createProducers(List.of("first", "second"));
            worker.createConsumers(assignmentForConsumer());
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread stopper = thread(worker::stopAll, failure);
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            stopper.join(5000);
            assertThat(stopper.isAlive()).isFalse();
            assertThat(
                            messages(failure.get()).stream()
                                    .filter(m -> m != null && m.contains("close timed out")))
                    .hasSize(2);
            verify(healthyProducer).close();
            verify(healthyConsumer).close();
            assertThat(scenario.closeCount.get()).isEqualTo(1);
            assertThat(worker.canReuseAfterFailure()).isFalse();
        } finally {
            release.countDown();
            assertThat(exited.await(2, TimeUnit.SECONDS)).isTrue();
            worker.close();
        }
    }

    @Test
    void timesOutAStalledStatisticsResetAndRejectsReuse() throws Exception {
        WorkerStats stats = mock(WorkerStats.class);
        when(stats.getMeterRegistry())
                .thenReturn(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        doAnswer(
                        ignored -> {
                            entered.countDown();
                            try {
                                awaitIgnoringInterrupts(release);
                            } finally {
                                exited.countDown();
                            }
                            return null;
                        })
                .when(stats)
                .reset();
        LocalWorker worker =
                new LocalWorker(
                        stats, Duration.ofMillis(500), Duration.ofSeconds(2), Duration.ofMillis(500));
        initialize(worker, new Scenario(List.of()));
        try {
            long start = System.nanoTime();
            assertThatThrownBy(worker::stopAll).hasRootCauseMessage("Statistics reset timed out");
            assertThat(entered.getCount()).isZero();
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(3));
            assertThat(worker.canReuseAfterFailure()).isFalse();
        } finally {
            release.countDown();
            assertThat(exited.await(2, TimeUnit.SECONDS)).isTrue();
            worker.close();
        }
    }

    private LocalWorker worker(Scenario scenario, Duration timeout) throws Exception {
        LocalWorker worker =
                new LocalWorker(
                        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT), timeout, Duration.ofSeconds(2));
        initialize(worker, scenario);
        return worker;
    }

    private void initialize(LocalWorker worker, Scenario scenario) throws Exception {
        Path config = directory.resolve("driver-" + FixtureDriver.nextId.incrementAndGet() + ".yaml");
        Files.writeString(config, "driverClass: " + FixtureDriver.class.getName() + "\n");
        FixtureDriver.fixtures.put(config.toAbsolutePath(), scenario);
        worker.initializeDriver(config);
    }

    private static ProducerWorkAssignment assignment() {
        return new ProducerWorkAssignment(List.of(new byte[8]), 0.001, KeyDistributorType.NO_KEY);
    }

    private static ConsumerAssignment assignmentForConsumer() {
        ConsumerAssignment assignment = new ConsumerAssignment();
        assignment.topicsSubscriptions.add(new TopicSubscription("topic", "subscription"));
        return assignment;
    }

    private static Thread thread(ThrowingRunnable action, AtomicReference<Throwable> failure) {
        return Thread.ofVirtual()
                .start(
                        () -> {
                            try {
                                action.run();
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });
    }

    private static void waitUntil(Check condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.get()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Condition was not met before the deadline");
            }
            Thread.sleep(5);
        }
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static List<String> messages(Throwable failure) {
        List<String> messages = new ArrayList<>();
        collectMessages(failure, messages);
        return messages;
    }

    private static void collectMessages(Throwable failure, List<String> messages) {
        if (failure == null) {
            return;
        }
        messages.add(failure.getMessage());
        collectMessages(failure.getCause(), messages);
        for (Throwable suppressed : failure.getSuppressed()) {
            collectMessages(suppressed, messages);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface Check {
        boolean get() throws Exception;
    }

    private static final class Scenario {
        private Supplier<CompletableFuture<List<BenchmarkProducer>>> createProducers;
        private List<BenchmarkConsumer> consumers = List.of();
        private final AtomicInteger producerCreationStarted = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();
        private volatile ConsumerCallback callback;
        private Runnable onClose = () -> {};
        private Exception closeFailure;

        private Scenario(List<BenchmarkProducer> producers) {
            createProducers = () -> CompletableFuture.completedFuture(producers);
        }
    }

    public static final class FixtureDriver implements BenchmarkDriver {
        private static final Map<Path, Scenario> fixtures = new ConcurrentHashMap<>();
        private static final AtomicInteger nextId = new AtomicInteger();
        private Scenario scenario;

        @Override
        public void initialize(Path configurationFile, MeterRegistry registry) {
            scenario = fixtures.get(configurationFile.toAbsolutePath());
        }

        @Override
        public String getTopicNamePrefix() {
            return "fixture";
        }

        @Override
        public CompletableFuture<Void> createTopic(String topic, int partitions) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<List<BenchmarkProducer>> createProducers(
                List<ProducerInfo> producers) {
            scenario.producerCreationStarted.incrementAndGet();
            return scenario.createProducers.get();
        }

        @Override
        public CompletableFuture<BenchmarkConsumer> createConsumer(
                String topic, String subscription, ConsumerCallback callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<List<BenchmarkConsumer>> createConsumers(
                List<ConsumerInfo> consumers) {
            if (!consumers.isEmpty()) {
                scenario.callback = consumers.getFirst().consumerCallback();
            }
            return CompletableFuture.completedFuture(scenario.consumers);
        }

        @Override
        public void close() throws Exception {
            scenario.closeCount.incrementAndGet();
            scenario.onClose.run();
            if (scenario.closeFailure != null) {
                throw scenario.closeFailure;
            }
        }
    }
}
