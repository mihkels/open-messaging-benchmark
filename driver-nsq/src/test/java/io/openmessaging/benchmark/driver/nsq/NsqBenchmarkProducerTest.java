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
package io.openmessaging.benchmark.driver.nsq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.exceptions.NSQException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class NsqBenchmarkProducerTest {
    @Test
    void failuresCompleteExceptionallyAndReturnThePermit() throws Exception {
        NSQProducer client = mock(NSQProducer.class);
        NSQException nsqFailure = new NSQException("nsq");
        TimeoutException timeout = new TimeoutException("timeout");
        IllegalStateException unexpected = new IllegalStateException("unexpected");
        doThrow(nsqFailure)
                .doThrow(timeout)
                .doThrow(unexpected)
                .doNothing()
                .when(client)
                .produce(anyString(), any(byte[].class));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (NsqBenchmarkProducer producer = new NsqBenchmarkProducer(client, "topic", executor, 1)) {
            for (Exception failure : new Exception[] {nsqFailure, timeout, unexpected}) {
                CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
                assertThatThrownBy(() -> send.get(2, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCause(failure);
            }
            producer.sendAsync(Optional.empty(), new byte[] {2}).get(2, TimeUnit.SECONDS);
        }
        assertThat(executor.isShutdown()).isTrue();
        verify(client).shutdown();
    }

    @Test
    void interruptionDoesNotCreateAnExtraPermit() throws Exception {
        NSQProducer client = mock(NSQProducer.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            entered.countDown();
                            release.await();
                            return null;
                        })
                .when(client)
                .produce(anyString(), any(byte[].class));
        ExecutorService executor = Executors.newCachedThreadPool();
        try (NsqBenchmarkProducer producer = new NsqBenchmarkProducer(client, "topic", executor, 1)) {
            CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();

            AtomicReference<CompletableFuture<Void>> interruptedSend = new AtomicReference<>();
            AtomicBoolean interruptPreserved = new AtomicBoolean();
            Thread interrupted =
                    new Thread(
                            () -> {
                                Thread.currentThread().interrupt();
                                interruptedSend.set(producer.sendAsync(Optional.empty(), new byte[] {2}));
                                interruptPreserved.set(Thread.currentThread().isInterrupted());
                            });
            interrupted.start();
            interrupted.join(2000);
            assertThat(interruptPreserved).isTrue();
            assertThatThrownBy(() -> interruptedSend.get().get(2, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(InterruptedException.class);

            CountDownLatch waiterStarted = new CountDownLatch(1);
            CountDownLatch waiterReturned = new CountDownLatch(1);
            AtomicReference<CompletableFuture<Void>> waitingSend = new AtomicReference<>();
            Thread waiter =
                    new Thread(
                            () -> {
                                waiterStarted.countDown();
                                waitingSend.set(producer.sendAsync(Optional.empty(), new byte[] {3}));
                                waiterReturned.countDown();
                            });
            waiter.start();
            assertThat(waiterStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(waiterReturned.await(150, TimeUnit.MILLISECONDS)).isFalse();
            release.countDown();
            first.get(2, TimeUnit.SECONDS);
            assertThat(waiterReturned.await(2, TimeUnit.SECONDS)).isTrue();
            waitingSend.get().get(2, TimeUnit.SECONDS);
            waiter.join(2000);
        } finally {
            release.countDown();
        }
    }

    @Test
    void rejectedExecutionReleasesCapacityAndAllowsAnotherSend() throws Exception {
        NSQProducer client = mock(NSQProducer.class);
        RejectedExecutionException rejection = new RejectedExecutionException("rejected");
        AtomicBoolean reject = new AtomicBoolean(true);
        ExecutorService executor =
                new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>()) {
                    @Override
                    public void execute(Runnable task) {
                        if (reject.getAndSet(false)) {
                            throw rejection;
                        }
                        super.execute(task);
                    }
                };
        try (NsqBenchmarkProducer producer = new NsqBenchmarkProducer(client, "topic", executor, 1)) {
            CompletableFuture<Void> rejected = producer.sendAsync(Optional.empty(), new byte[] {1});
            assertThatThrownBy(() -> rejected.get(2, TimeUnit.SECONDS)).hasCause(rejection);
            producer.sendAsync(Optional.empty(), new byte[] {2}).get(2, TimeUnit.SECONDS);
            verify(client, times(1)).produce(anyString(), any(byte[].class));
        }
    }

    @Test
    void wrappedInterruptionFromClientRemainsVisible() throws Exception {
        NSQProducer client = mock(NSQProducer.class);
        InterruptedException interrupted = new InterruptedException("interrupted");
        NSQException failure = new NSQException("client interrupted", interrupted);
        doThrow(failure).when(client).produce(anyString(), any(byte[].class));
        ExecutorService executor = mock(ExecutorService.class);
        doAnswer(
                        invocation -> {
                            invocation.<Runnable>getArgument(0).run();
                            return null;
                        })
                .when(executor)
                .execute(any(Runnable.class));
        NsqBenchmarkProducer producer = new NsqBenchmarkProducer(client, "topic", executor, 1);
        try {
            assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                    .hasCause(failure);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
            producer.close();
        }
    }

    @Test
    void shutdownIsNotReportedAsSuccessWhenClientSwallowsInterruption() throws Exception {
        NSQProducer client = mock(NSQProducer.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            entered.countDown();
                            try {
                                new CountDownLatch(1).await();
                            } catch (InterruptedException ignored) {
                                // The NSQ client can swallow interruption and return normally.
                            }
                            return null;
                        })
                .when(client)
                .produce(anyString(), any(byte[].class));
        ExecutorService executor =
                new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>()) {
                    @Override
                    public java.util.List<Runnable> shutdownNow() {
                        java.util.List<Runnable> abandoned = super.shutdownNow();
                        try {
                            if (!completed.await(2, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("send did not complete");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(e);
                        }
                        return abandoned;
                    }
                };
        NsqBenchmarkProducer producer = new NsqBenchmarkProducer(client, "topic", executor, 1);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        send.whenComplete((result, failure) -> completed.countDown());
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        producer.close();
        assertThatThrownBy(send::join).hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeSettlesRunningAndQueuedSendsAndStopsExecutor() throws Exception {
        NSQProducer client = mock(NSQProducer.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            entered.countDown();
                            boolean interrupted = false;
                            while (true) {
                                try {
                                    release.await();
                                    break;
                                } catch (InterruptedException e) {
                                    interrupted = true;
                                }
                            }
                            if (interrupted) {
                                Thread.currentThread().interrupt();
                            }
                            return null;
                        })
                .when(client)
                .produce(anyString(), any(byte[].class));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        NsqBenchmarkProducer producer = new NsqBenchmarkProducer(client, "topic", executor, 2);
        try {
            CompletableFuture<Void> running = producer.sendAsync(Optional.empty(), new byte[] {1});
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> queued = producer.sendAsync(Optional.empty(), new byte[] {2});
            CountDownLatch waiterStarted = new CountDownLatch(1);
            CountDownLatch waiterReturned = new CountDownLatch(1);
            AtomicReference<CompletableFuture<Void>> waitingSend = new AtomicReference<>();
            Thread waiter =
                    new Thread(
                            () -> {
                                waiterStarted.countDown();
                                waitingSend.set(producer.sendAsync(Optional.empty(), new byte[] {3}));
                                waiterReturned.countDown();
                            });
            waiter.start();
            assertThat(waiterStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(waiterReturned.await(150, TimeUnit.MILLISECONDS)).isFalse();
            producer.close();
            assertThatThrownBy(() -> running.get(2, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> queued.get(2, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(waiterReturned.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> waitingSend.get().join())
                    .hasCauseInstanceOf(IllegalStateException.class);
            waiter.join(2000);
            assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {3}).join())
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(executor.isShutdown()).isTrue();
            producer.close();
            verify(client, times(1)).shutdown();
        } finally {
            release.countDown();
            assertThat(executor.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void concurrentCloseWaitsForOutstandingSendsToBeSettled() throws Exception {
        NSQProducer client = mock(NSQProducer.class);
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch releasePublish = new CountDownLatch(1);
        CountDownLatch stoppingExecutor = new CountDownLatch(1);
        CountDownLatch releaseShutdown = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            publishing.countDown();
                            releasePublish.await();
                            return null;
                        })
                .when(client)
                .produce(anyString(), any(byte[].class));
        ExecutorService executor =
                new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>()) {
                    @Override
                    public java.util.List<Runnable> shutdownNow() {
                        stoppingExecutor.countDown();
                        try {
                            if (!releaseShutdown.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("executor shutdown was not released");
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                        return super.shutdownNow();
                    }
                };
        NsqBenchmarkProducer producer = new NsqBenchmarkProducer(client, "topic", executor, 1);
        ExecutorService closers = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
            assertThat(publishing.await(2, TimeUnit.SECONDS)).isTrue();
            Future<?> firstClose = closers.submit(producer::close);
            assertThat(stoppingExecutor.await(2, TimeUnit.SECONDS)).isTrue();
            CountDownLatch secondStarted = new CountDownLatch(1);
            Future<?> secondClose =
                    closers.submit(
                            () -> {
                                secondStarted.countDown();
                                producer.close();
                            });
            assertThat(secondStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> secondClose.get(150, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(send).isNotDone();

            releaseShutdown.countDown();
            firstClose.get(2, TimeUnit.SECONDS);
            secondClose.get(2, TimeUnit.SECONDS);
            assertThat(send).isCompletedExceptionally();
            verify(client, times(1)).shutdown();
        } finally {
            releaseShutdown.countDown();
            releasePublish.countDown();
            closers.shutdownNow();
        }
    }

    @Test
    void completionCallbackCanReenterCloseDuringPendingSendSettlement() throws Exception {
        NSQProducer client = mock(NSQProducer.class);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        executor.execute(
                () -> {
                    occupied.countDown();
                    try {
                        releaseWorker.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                });
        NsqBenchmarkProducer producer = new NsqBenchmarkProducer(client, "topic", executor, 1);
        ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            assertThat(occupied.await(2, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
            AtomicReference<Thread> callbackThread = new AtomicReference<>();
            CompletableFuture<Void> callback =
                    send.handle(
                            (result, failure) -> {
                                callbackThread.set(Thread.currentThread());
                                producer.close();
                                return null;
                            });
            AtomicReference<Thread> closeThread = new AtomicReference<>();
            Future<?> closing =
                    closer.submit(
                            () -> {
                                closeThread.set(Thread.currentThread());
                                producer.close();
                            });
            closing.get(2, TimeUnit.SECONDS);
            callback.get(2, TimeUnit.SECONDS);
            assertThat(callbackThread.get()).isSameAs(closeThread.get());
            assertThat(send).isCompletedExceptionally();
            verify(client, times(1)).shutdown();
        } finally {
            releaseWorker.countDown();
            closer.shutdownNow();
        }
    }

    @Test
    void repeatedClosePreservesShutdownFailure() {
        NSQProducer client = mock(NSQProducer.class);
        RuntimeException failure = new RuntimeException("client shutdown failed");
        doThrow(failure).when(client).shutdown();
        NsqBenchmarkProducer producer =
                new NsqBenchmarkProducer(client, "topic", Executors.newSingleThreadExecutor(), 1);

        assertThatThrownBy(producer::close).isSameAs(failure);
        assertThatThrownBy(producer::close).isSameAs(failure);
        verify(client, times(1)).shutdown();
    }

    @Test
    void canceledSendDoesNotRetainTheCallerBuffer() throws Exception {
        NSQProducer client = mock(NSQProducer.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<byte[]> wirePayload = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            wirePayload.set(invocation.getArgument(1));
                            entered.countDown();
                            release.await();
                            return null;
                        })
                .when(client)
                .produce(anyString(), any(byte[].class));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (NsqBenchmarkProducer producer = new NsqBenchmarkProducer(client, "topic", executor, 1)) {
            byte[] payload = {1, 2, 3};
            CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), payload);
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(future.cancel(false)).isTrue();
            Arrays.fill(payload, (byte) 9);
            assertThat(wirePayload.get()).containsExactly(1, 2, 3);
        } finally {
            release.countDown();
        }
    }

    @Test
    void completionCallbackCanChainAnotherSend() throws Exception {
        assertCompletionCallbackCanSend(false);
    }

    @Test
    void failureCallbackCanChainAnotherSend() throws Exception {
        assertCompletionCallbackCanSend(true);
    }

    private static void assertCompletionCallbackCanSend(boolean failFirst) throws Exception {
        NSQProducer client = mock(NSQProducer.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        doAnswer(
                        invocation -> {
                            if (calls.getAndIncrement() == 0) {
                                entered.countDown();
                                if (!release.await(5, TimeUnit.SECONDS)) {
                                    throw new AssertionError("first publish was not released");
                                }
                                if (failFirst) {
                                    throw new NSQException("publish failed");
                                }
                            }
                            return null;
                        })
                .when(client)
                .produce(anyString(), any(byte[].class));
        ExecutorService executor =
                Executors.newSingleThreadExecutor(
                        task -> {
                            Thread thread = new Thread(task, "nsq-chained-send-test");
                            thread.setDaemon(true);
                            return thread;
                        });
        try (NsqBenchmarkProducer producer = new NsqBenchmarkProducer(client, "topic", executor, 1)) {
            CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> chained =
                    first
                            .handle((result, failure) -> producer.sendAsync(Optional.empty(), new byte[] {2}))
                            .thenCompose(result -> result);
            release.countDown();
            chained.get(2, TimeUnit.SECONDS);
            assertThat(first.isCompletedExceptionally()).isEqualTo(failFirst);
            assertThat(calls).hasValue(2);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }
}
