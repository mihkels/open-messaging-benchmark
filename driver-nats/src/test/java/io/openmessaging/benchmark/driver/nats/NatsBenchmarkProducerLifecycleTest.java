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
package io.openmessaging.benchmark.driver.nats;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

class NatsBenchmarkProducerLifecycleTest {
    private static final String TOPIC = "test-topic";

    @Test
    void closeSettlesOnlyItsOwnSendsAndLeavesTheSharedClientUsable() {
        List<CompletableFuture<PublishAck>> acknowledgements = new ArrayList<>();
        JetStream shared =
                jetStream(
                        (topic, message) -> {
                            CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
                            acknowledgements.add(acknowledgement);
                            return acknowledgement;
                        });
        NatsBenchmarkProducer producer = new NatsBenchmarkProducer(shared, TOPIC);
        NatsBenchmarkProducer other = new NatsBenchmarkProducer(shared, TOPIC);
        byte[] payload = samplePayload();
        byte[] original = payload.clone();

        CompletableFuture<Void> acknowledged = producer.sendAsync(Optional.empty(), payload);
        acknowledgements.get(0).complete(null);
        assertNull(acknowledged.join());
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), payload);
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), payload);
        CompletableFuture<Void> otherSend = other.sendAsync(Optional.empty(), payload);
        CompletableFuture<Void> cancelled = producer.sendAsync(Optional.empty(), payload);
        assertTrue(cancelled.cancel(false));

        producer.close();

        Throwable firstFailure = assertClosed(first);
        Throwable secondFailure = assertClosed(second);
        assertNull(acknowledged.join());
        assertTrue(cancelled.isCancelled());
        assertFalse(acknowledgements.get(1).isDone());
        assertFalse(acknowledgements.get(2).isDone());
        assertFalse(otherSend.isDone());

        acknowledgements.get(1).complete(null);
        acknowledgements.get(2).completeExceptionally(new IOException("late rejection"));
        assertSame(firstFailure, assertClosed(first));
        assertSame(secondFailure, assertClosed(second));
        acknowledgements.get(3).complete(null);
        assertNull(otherSend.join());
        CompletableFuture<Void> subsequent = other.sendAsync(Optional.empty(), payload);
        acknowledgements.get(5).complete(null);
        assertNull(subsequent.join());
        producer.close();
        other.close();
        assertArrayEquals(original, payload);
    }

    @Test
    void sendsAfterCloseFailWithoutPublishing() {
        AtomicInteger publishCalls = new AtomicInteger();
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(
                        jetStream(
                                (topic, message) -> {
                                    publishCalls.incrementAndGet();
                                    return new CompletableFuture<>();
                                }),
                        TOPIC);
        byte[] payload = samplePayload();
        byte[] original = payload.clone();
        producer.close();

        assertClosed(producer.sendAsync(Optional.empty(), payload));
        assertClosed(producer.sendAsync(Optional.empty(), new byte[0]));
        assertEquals(0, publishCalls.get());
        assertArrayEquals(original, payload);
    }

    @Test
    void callbackCloseIsRejectedWithoutClosingTheProducer() {
        List<CompletableFuture<PublishAck>> acknowledgements = new ArrayList<>();
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(
                        jetStream(
                                (topic, message) -> {
                                    CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
                                    acknowledgements.add(acknowledgement);
                                    return acknowledgement;
                                }),
                        TOPIC);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), samplePayload());
        CompletableFuture<Void> callback =
                send.thenRun(() -> assertThrows(IllegalStateException.class, producer::close));

        acknowledgements.getFirst().complete(null);

        callback.join();
        CompletableFuture<Void> next = producer.sendAsync(Optional.empty(), samplePayload());
        assertFalse(next.isDone());
        producer.close();
        assertClosed(next);
    }

    @Test
    void failureCallbacksCannotRecursivelyCloseTheProducer() {
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(jetStream((topic, message) -> new CompletableFuture<>()), TOPIC);
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), samplePayload());
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), samplePayload());
        CompletableFuture<Void> callback =
                first.handle(
                        (unused, failure) -> {
                            assertThrows(IllegalStateException.class, producer::close);
                            return null;
                        });

        producer.close();

        callback.join();
        assertClosed(first);
        assertClosed(second);
    }

    @Test
    void synchronousPublishFailureFailsTheSendAndLeavesTheProducerUsable() {
        AtomicInteger publishCalls = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("publish failed synchronously");
        CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(
                        jetStream(
                                (topic, message) -> {
                                    if (publishCalls.getAndIncrement() == 0) {
                                        throw failure;
                                    }
                                    return acknowledgement;
                                }),
                        TOPIC);
        byte[] payload = samplePayload();
        byte[] original = payload.clone();

        CompletableFuture<Void> failed = producer.sendAsync(Optional.empty(), payload);
        assertSame(failure, assertThrows(CompletionException.class, failed::join).getCause());
        CompletableFuture<Void> next = producer.sendAsync(Optional.empty(), payload);
        assertFalse(next.isDone());
        acknowledgement.complete(null);
        assertNull(next.join());
        producer.close();
        assertSame(failure, assertThrows(CompletionException.class, failed::join).getCause());
        assertArrayEquals(original, payload);
    }

    @Test
    void cancelledAcknowledgementRetainsDependentFutureSemantics() {
        CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(jetStream((topic, message) -> acknowledgement), TOPIC);

        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), samplePayload());
        assertTrue(acknowledgement.cancel(false));

        assertFalse(send.isCancelled());
        CompletionException failure = assertThrows(CompletionException.class, send::join);
        assertInstanceOf(CancellationException.class, failure.getCause());
        producer.close();
    }

    @Test
    void closeDoesNotWaitForAnInProgressPublish() throws Exception {
        assertCloseDuringPublish(false);
    }

    @Test
    void closeWinsWhenAnInProgressPublishThrows() throws Exception {
        assertCloseDuringPublish(true);
    }

    @Test
    void concurrentCloseCallsSettleAllSendsAndLateAcknowledgementsCannotWin() throws Exception {
        List<CompletableFuture<PublishAck>> acknowledgements = new ArrayList<>();
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(
                        jetStream(
                                (topic, message) -> {
                                    CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
                                    acknowledgements.add(acknowledgement);
                                    return acknowledgement;
                                }),
                        TOPIC);
        List<CompletableFuture<Void>> sends = new ArrayList<>();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        AtomicBoolean blockFirst = new AtomicBoolean(true);
        for (int i = 0; i < 3; i++) {
            CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), samplePayload());
            sends.add(send);
            send.whenComplete(
                    (ignored, failure) -> {
                        if (blockFirst.compareAndSet(true, false)) {
                            callbackEntered.countDown();
                            await(releaseCallback);
                        }
                    });
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstClose = executor.submit(producer::close);
            assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
            List<Integer> unfinished = new ArrayList<>();
            for (int i = 0; i < sends.size(); i++) {
                if (!sends.get(i).isDone()) {
                    unfinished.add(i);
                }
            }
            assertEquals(2, unfinished.size());

            int lateAck = unfinished.get(0);
            acknowledgements.get(lateAck).complete(null);
            assertClosed(sends.get(lateAck));
            Future<?> secondClose = executor.submit(producer::close);
            secondClose.get(5, TimeUnit.SECONDS);
            for (CompletableFuture<Void> send : sends) {
                assertClosed(send);
            }
            releaseCallback.countDown();
            firstClose.get(5, TimeUnit.SECONDS);
        } finally {
            releaseCallback.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void concurrentSendsAndCloseLeaveNoSendPending() throws Exception {
        int senderCount = 12;
        ExecutorService executor = Executors.newFixedThreadPool(senderCount + 1);
        byte[] payload = samplePayload();
        byte[] original = payload.clone();
        try {
            for (int round = 0; round < 25; round++) {
                NatsBenchmarkProducer producer =
                        new NatsBenchmarkProducer(
                                jetStream((topic, message) -> new CompletableFuture<>()), TOPIC);
                CompletableFuture<Void> alreadyPending = producer.sendAsync(Optional.empty(), payload);
                CountDownLatch ready = new CountDownLatch(senderCount + 1);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<CompletableFuture<Void>>> calls = new ArrayList<>();
                for (int i = 0; i < senderCount; i++) {
                    calls.add(
                            executor.submit(
                                    () -> {
                                        ready.countDown();
                                        assertTrue(start.await(5, TimeUnit.SECONDS));
                                        return producer.sendAsync(Optional.empty(), payload);
                                    }));
                }
                Future<?> closing =
                        executor.submit(
                                () -> {
                                    ready.countDown();
                                    assertTrue(start.await(5, TimeUnit.SECONDS));
                                    producer.close();
                                    return null;
                                });
                assertTrue(ready.await(5, TimeUnit.SECONDS));
                start.countDown();
                closing.get(5, TimeUnit.SECONDS);
                assertClosed(alreadyPending);
                for (Future<CompletableFuture<Void>> call : calls) {
                    assertClosed(call.get(5, TimeUnit.SECONDS));
                }
                assertClosed(producer.sendAsync(Optional.empty(), payload));
            }
            assertArrayEquals(original, payload);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void racingAcknowledgementAndCloseSelectExactlyOneOutcome() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 100; round++) {
                CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
                NatsBenchmarkProducer producer =
                        new NatsBenchmarkProducer(jetStream((topic, message) -> acknowledgement), TOPIC);
                CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), samplePayload());
                AtomicInteger completions = new AtomicInteger();
                send.whenComplete((ignored, failure) -> completions.incrementAndGet());
                IOException acknowledgementFailure = new IOException("publish rejected");
                boolean succeed = round % 2 == 0;
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);
                Future<?> acknowledging =
                        executor.submit(
                                () -> {
                                    ready.countDown();
                                    assertTrue(start.await(5, TimeUnit.SECONDS));
                                    if (succeed) {
                                        acknowledgement.complete(null);
                                    } else {
                                        acknowledgement.completeExceptionally(acknowledgementFailure);
                                    }
                                    return null;
                                });
                Future<?> closing =
                        executor.submit(
                                () -> {
                                    ready.countDown();
                                    assertTrue(start.await(5, TimeUnit.SECONDS));
                                    producer.close();
                                    return null;
                                });
                assertTrue(ready.await(5, TimeUnit.SECONDS));
                start.countDown();
                acknowledging.get(5, TimeUnit.SECONDS);
                closing.get(5, TimeUnit.SECONDS);

                assertTrue(send.isDone());
                if (!send.isCompletedExceptionally()) {
                    assertTrue(succeed);
                    assertNull(send.join());
                } else {
                    Throwable failure = assertThrows(CompletionException.class, send::join).getCause();
                    assertTrue(
                            failure == acknowledgementFailure
                                    || (failure instanceof IllegalStateException
                                            && "NATS producer is closed".equals(failure.getMessage())));
                }
                assertEquals(1, completions.get());
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static void assertCloseDuringPublish(boolean throwSynchronously) throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
        IllegalStateException publishFailure = new IllegalStateException("synchronous publish failure");
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(
                        jetStream(
                                (topic, message) -> {
                                    entered.countDown();
                                    await(release);
                                    if (throwSynchronously) {
                                        throw publishFailure;
                                    }
                                    return acknowledgement;
                                }),
                        TOPIC);
        byte[] payload = samplePayload();
        byte[] original = payload.clone();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<CompletableFuture<Void>> sending =
                    executor.submit(() -> producer.sendAsync(Optional.empty(), payload));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            executor.submit(producer::close).get(5, TimeUnit.SECONDS);
            release.countDown();

            CompletableFuture<Void> send = sending.get(5, TimeUnit.SECONDS);
            Throwable failure = assertClosed(send);
            assertFalse(acknowledgement.isDone());
            acknowledgement.complete(null);
            assertSame(failure, assertClosed(send));
            assertArrayEquals(original, payload);
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static Throwable assertClosed(CompletableFuture<Void> send) {
        assertTrue(send.isDone(), "send did not settle");
        CompletionException failure = assertThrows(CompletionException.class, send::join);
        IllegalStateException cause = assertInstanceOf(IllegalStateException.class, failure.getCause());
        assertEquals("NATS producer is closed", cause.getMessage());
        return cause;
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for concurrent test", e);
        }
    }

    private static byte[] samplePayload() {
        byte[] payload = new byte[64];
        Arrays.fill(payload, (byte) 0x5a);
        return payload;
    }

    private static JetStream jetStream(
            BiFunction<String, byte[], CompletableFuture<PublishAck>> publish) {
        return (JetStream)
                Proxy.newProxyInstance(
                        JetStream.class.getClassLoader(),
                        new Class<?>[] {JetStream.class},
                        (proxy, method, args) -> {
                            if (!method.getName().equals("publishAsync")
                                    || method.getParameterCount() != 2
                                    || method.getParameterTypes()[0] != String.class
                                    || method.getParameterTypes()[1] != byte[].class) {
                                throw new AssertionError("Unexpected JetStream call: " + method);
                            }
                            return publish.apply((String) args[0], (byte[]) args[1]);
                        });
    }
}
