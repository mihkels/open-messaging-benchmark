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
package io.openmessaging.benchmark.driver.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreator;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;

class KafkaBenchmarkConsumerReadinessTest {
    @Test
    void waitsForGroupAssignmentAndInitialPositionsWithoutSeekingOrCommitting() throws Exception {
        Fixture fixture = new Fixture();
        TopicPartition partition = new TopicPartition("topic", 0);
        AtomicBoolean positionReady = new AtomicBoolean();
        CountDownLatch positionChecked = new CountDownLatch(1);
        when(fixture.consumer.position(eq(partition), eq(Duration.ZERO)))
                .thenAnswer(
                        ignored -> {
                            positionChecked.countDown();
                            if (!positionReady.get()) {
                                throw new TimeoutException("position pending");
                            }
                            return 42L;
                        });
        CompletableFuture<BenchmarkConsumer> future = fixture.start();
        try {
            assertTrue(fixture.pollStarted.await(5, TimeUnit.SECONDS));
            assertFalse(future.isDone());
            fixture.assign(Set.of(partition));
            assertTrue(positionChecked.await(5, TimeUnit.SECONDS));
            assertFalse(future.isDone());

            positionReady.set(true);
            fixture.emptyPoll();
            BenchmarkConsumer handle = future.get(5, TimeUnit.SECONDS);
            verify(fixture.consumer).subscribe(eq(Set.of("topic")), any(ConsumerRebalanceListener.class));
            verify(fixture.consumer, never()).seek(any(TopicPartition.class), anyLong());
            verify(fixture.consumer, never()).commitSync();
            verify(fixture.consumer, never()).commitAsync();
            handle.close();
            assertTrue(fixture.closed.await(5, TimeUnit.SECONDS));
        } finally {
            fixture.shutdown();
        }
    }

    @Test
    void emptyAssignmentIsReadyAfterJoiningTheGroup() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<BenchmarkConsumer> future = fixture.start();
        try {
            assertTrue(fixture.pollStarted.await(5, TimeUnit.SECONDS));
            assertFalse(future.isDone());
            fixture.assign(Set.of());
            future.get(5, TimeUnit.SECONDS).close();
            verify(fixture.consumer, never()).position(any(TopicPartition.class), any(Duration.class));
        } finally {
            fixture.shutdown();
        }
    }

    @Test
    void emptyAssignmentWaitsUntilTopicMetadataExists() throws Exception {
        Fixture fixture = new Fixture();
        fixture.metadata.set(List.of());
        CompletableFuture<BenchmarkConsumer> future = fixture.start();
        try {
            fixture.assign(Set.of());
            assertTrue(fixture.metadataChecked.await(5, TimeUnit.SECONDS));
            assertFalse(future.isDone());
            fixture.metadata.set(List.of(partitionInfo()));
            fixture.emptyPoll();
            future.get(5, TimeUnit.SECONDS).close();
        } finally {
            fixture.shutdown();
        }
    }

    @Test
    void retriablePollingFailureDoesNotCompleteCreation() throws Exception {
        Fixture fixture = new Fixture();
        CountDownLatch failedPoll = new CountDownLatch(1);
        CompletableFuture<BenchmarkConsumer> future = fixture.start();
        try {
            fixture.steps.add(
                    () -> {
                        failedPoll.countDown();
                        throw new TimeoutException("transient broker failure");
                    });
            assertTrue(failedPoll.await(5, TimeUnit.SECONDS));
            assertFalse(future.isDone());
            fixture.assign(Set.of());
            future.get(5, TimeUnit.SECONDS).close();
        } finally {
            fixture.shutdown();
        }
    }

    @Test
    void fatalStartupFailureClosesConsumerAndPreservesCleanupFailure() throws Exception {
        Fixture fixture = new Fixture();
        RuntimeException failure = new IllegalStateException("subscription rejected");
        RuntimeException cleanup = new IllegalStateException("close failed");
        doAnswer(
                        ignored -> {
                            fixture.closed.countDown();
                            throw cleanup;
                        })
                .when(fixture.consumer)
                .close();
        CompletableFuture<BenchmarkConsumer> future = fixture.start();
        try {
            fixture.steps.add(
                    () -> {
                        throw failure;
                    });
            ExecutionException result =
                    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
            assertSame(failure, result.getCause());
            assertEquals(List.of(cleanup), List.of(failure.getSuppressed()));
            verify(fixture.consumer).close();
        } finally {
            fixture.shutdown();
        }
    }

    @Test
    void cancellationInterruptsPollingAndClosesTheUntransferredConsumer() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<BenchmarkConsumer> future = fixture.start();
        try {
            assertTrue(fixture.pollStarted.await(5, TimeUnit.SECONDS));
            assertTrue(future.cancel(false));
            assertTrue(future.isCancelled());
            assertTrue(fixture.closed.await(5, TimeUnit.SECONDS));
            verify(fixture.consumer).wakeup();
            verify(fixture.consumer).close();
            assertTrue(future.cancel(false));
        } finally {
            fixture.shutdown();
        }
    }

    @Test
    void abandonedBatchStopsPendingConsumerWithoutCancellingItsFuture() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<BenchmarkConsumer> creation = fixture.start();
        CompletableFuture<BenchmarkConsumer> batch =
                ResourceCreator.compose(
                        CompletableFuture.completedFuture(List.of(1)),
                        ignored -> creation,
                        consumers -> consumers.get(0));
        try {
            assertTrue(fixture.pollStarted.await(5, TimeUnit.SECONDS));
            assertTrue(batch.cancel(false));
            assertTrue(fixture.closed.await(5, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> creation.get(5, TimeUnit.SECONDS));
            assertFalse(creation.isCancelled());
            verify(fixture.consumer).wakeup();
            verify(fixture.consumer).close();
        } finally {
            fixture.shutdown();
        }
    }

    @Test
    void closingFromHandoffCallbackDoesNotDeadlockThePollThread() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<BenchmarkConsumer> future = fixture.start();
        CountDownLatch callbackFinished = new CountDownLatch(1);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        try {
            future.whenComplete(
                    (handle, error) -> {
                        try {
                            if (error == null) {
                                handle.close();
                            }
                        } catch (Exception closeError) {
                            callbackFailure.set(closeError);
                        } finally {
                            callbackFinished.countDown();
                        }
                    });
            fixture.assign(Set.of());
            assertTrue(callbackFinished.await(5, TimeUnit.SECONDS));
            assertTrue(fixture.closed.await(5, TimeUnit.SECONDS));
            assertSame(null, callbackFailure.get());
            verify(fixture.consumer).close();
        } finally {
            fixture.shutdown();
        }
    }

    @Test
    void concurrentCloseWaitsForAnInFlightCallback() throws Exception {
        Fixture fixture = new Fixture();
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        doAnswer(
                        ignored -> {
                            callbackStarted.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting for callback release");
                            }
                            return null;
                        })
                .when(callback)
                .messageReceived(any(byte[].class), anyLong());
        CompletableFuture<BenchmarkConsumer> future = fixture.start(callback);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            fixture.assign(Set.of());
            BenchmarkConsumer handle = future.get(5, TimeUnit.SECONDS);
            TopicPartition partition = new TopicPartition("topic", 0);
            fixture.steps.add(
                    () ->
                            new ConsumerRecords<>(
                                    java.util.Map.of(
                                            partition,
                                            List.of(
                                                    new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                                                            "topic", 0, 1, "key", new byte[] {1})))));
            assertTrue(callbackStarted.await(5, TimeUnit.SECONDS));
            Future<?> close =
                    executor.submit(
                            () -> {
                                try {
                                    handle.close();
                                } catch (Exception error) {
                                    throw new RuntimeException(error);
                                }
                            });
            assertTrue(fixture.wakeupRequested.await(5, TimeUnit.SECONDS));
            assertFalse(close.isDone());
            release.countDown();
            close.get(5, TimeUnit.SECONDS);
            assertTrue(fixture.closed.await(5, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            fixture.shutdown();
            executor.shutdownNow();
        }
    }

    @FunctionalInterface
    private interface PollStep {
        ConsumerRecords<String, byte[]> poll() throws Exception;
    }

    private static PartitionInfo partitionInfo() {
        return new PartitionInfo("topic", 0, null, new Node[0], new Node[0]);
    }

    private static final class Fixture {
        private final KafkaConsumer<String, byte[]> consumer;
        private final AtomicReference<ConsumerRebalanceListener> listener = new AtomicReference<>();
        private final AtomicReference<Set<TopicPartition>> assignment = new AtomicReference<>(Set.of());
        private final AtomicReference<List<PartitionInfo>> metadata =
                new AtomicReference<>(List.of(partitionInfo()));
        private final BlockingQueue<PollStep> steps = new LinkedBlockingQueue<>();
        private final CountDownLatch pollStarted = new CountDownLatch(1);
        private final CountDownLatch wakeupRequested = new CountDownLatch(1);
        private final CountDownLatch metadataChecked = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private CompletableFuture<BenchmarkConsumer> future;

        @SuppressWarnings("unchecked")
        private Fixture() {
            consumer = mock(KafkaConsumer.class);
            doAnswer(
                            invocation -> {
                                listener.set(invocation.getArgument(1));
                                return null;
                            })
                    .when(consumer)
                    .subscribe(anyCollection(), any(ConsumerRebalanceListener.class));
            when(consumer.assignment()).thenAnswer(ignored -> assignment.get());
            when(consumer.partitionsFor(eq("topic"), eq(Duration.ZERO)))
                    .thenAnswer(
                            ignored -> {
                                metadataChecked.countDown();
                                return metadata.get();
                            });
            when(consumer.poll(any(Duration.class)))
                    .thenAnswer(
                            ignored -> {
                                pollStarted.countDown();
                                PollStep step = steps.poll(5, TimeUnit.SECONDS);
                                if (step == null) {
                                    throw new TimeoutException("no scripted poll");
                                }
                                return step.poll();
                            });
            doAnswer(
                            ignored -> {
                                wakeupRequested.countDown();
                                steps.add(
                                        () -> {
                                            throw new WakeupException();
                                        });
                                return null;
                            })
                    .when(consumer)
                    .wakeup();
            doAnswer(
                            ignored -> {
                                closed.countDown();
                                return null;
                            })
                    .when(consumer)
                    .close();
        }

        private CompletableFuture<BenchmarkConsumer> start() {
            return start(mock(ConsumerCallback.class));
        }

        private CompletableFuture<BenchmarkConsumer> start(ConsumerCallback callback) {
            future = KafkaBenchmarkConsumer.create(consumer, "topic", new Properties(), callback, 100);
            return future;
        }

        private void assign(Set<TopicPartition> partitions) {
            steps.add(
                    () -> {
                        assignment.set(partitions);
                        listener.get().onPartitionsAssigned(partitions);
                        return ConsumerRecords.empty();
                    });
        }

        private void emptyPoll() {
            steps.add(ConsumerRecords::empty);
        }

        private void shutdown() throws Exception {
            if (future == null) {
                return;
            }
            if (!future.isDone()) {
                future.cancel(false);
            } else if (!future.isCompletedExceptionally()) {
                future.join().close();
            }
            assertTrue(closed.await(5, TimeUnit.SECONDS));
        }
    }
}
