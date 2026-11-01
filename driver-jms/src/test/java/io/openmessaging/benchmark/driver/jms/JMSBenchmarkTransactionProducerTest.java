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
package io.openmessaging.benchmark.driver.jms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.junit.jupiter.api.Test;

class JMSBenchmarkTransactionProducerTest {
    @Test
    void missingProviderFailureDoesNotLeaveTransactionPending() throws Exception {
        Scenario scenario = new Scenario();
        JMSBenchmarkTransactionProducer producer = scenario.producer(true);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        scenario.listeners.get(0).onException(null, null);

        assertInstanceOf(
                JMSException.class, assertThrows(CompletionException.class, send::join).getCause());
        assertEquals(0, scenario.commits.get());
        producer.close();
    }

    @Test
    void closeSettlesPendingSendsAndIgnoresLateAcknowledgements() throws Exception {
        Scenario scenario = new Scenario();
        JMSBenchmarkTransactionProducer producer = scenario.producer(true);
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        assertFalse(first.isDone());
        assertFalse(second.isDone());

        producer.close();

        assertInstanceOf(
                IllegalStateException.class,
                assertThrows(CompletionException.class, first::join).getCause());
        assertInstanceOf(
                IllegalStateException.class,
                assertThrows(CompletionException.class, second::join).getCause());
        assertEquals(2, scenario.producerCloses.get());
        assertEquals(2, scenario.sessionCloses.get());
        assertEquals(0, scenario.commits.get());
        assertEquals(0, scenario.connectionCloses.get());

        scenario.listeners.get(0).onCompletion(null);
        scenario.listeners.get(1).onException(null, new JMSException("late failure"));
        drainCallbacks();
        assertEquals(2, scenario.producerCloses.get());
        assertEquals(2, scenario.sessionCloses.get());
        assertEquals(0, scenario.commits.get());
        CompletableFuture<Void> rejected = producer.sendAsync(Optional.empty(), new byte[] {3});
        assertInstanceOf(
                IllegalStateException.class,
                assertThrows(CompletionException.class, rejected::join).getCause());
        assertEquals(2, scenario.sessionsCreated.get());
        producer.close();
    }

    @Test
    void cancellationClosesResourcesAndLateAcknowledgementCannotCommit() throws Exception {
        Scenario scenario = new Scenario();
        JMSBenchmarkTransactionProducer producer = scenario.producer(true);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertTrue(send.cancel(true));
        assertTrue(send.isCancelled());
        scenario.listeners.get(0).onCompletion(null);
        drainCallbacks();
        assertEquals(1, scenario.producerCloses.get());
        assertEquals(1, scenario.sessionCloses.get());
        assertEquals(0, scenario.commits.get());
        producer.close();
        assertEquals(1, scenario.producerCloses.get());
        assertEquals(1, scenario.sessionCloses.get());
    }

    @Test
    void sendAndCleanupFailuresPreservePrimaryAndSuppressedExceptions() throws Exception {
        Scenario scenario = new Scenario();
        scenario.sendFailure = new JMSException("send failed");
        scenario.producerCloseFailure = new JMSException("producer close failed");
        scenario.sessionCloseFailure = new JMSException("session close failed");
        JMSBenchmarkTransactionProducer producer = scenario.producer(false);

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () -> producer.sendAsync(Optional.empty(), new byte[] {1}).join());
        assertSame(scenario.sendFailure, failure.getCause());
        assertSame(scenario.producerCloseFailure, failure.getCause().getSuppressed()[0]);
        assertSame(scenario.sessionCloseFailure, failure.getCause().getSuppressed()[1]);
        assertEquals(1, scenario.producerCloses.get());
        assertEquals(1, scenario.sessionCloses.get());
        producer.close();
    }

    @Test
    void closeAttemptsBothResourcesAndReportsTheirFailures() throws Exception {
        Scenario scenario = new Scenario();
        scenario.producerCloseFailure = new JMSException("producer close failed");
        scenario.sessionCloseFailure = new JMSException("session close failed");
        JMSBenchmarkTransactionProducer producer = scenario.producer(true);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertSame(scenario.producerCloseFailure, assertThrows(JMSException.class, producer::close));
        Throwable sendFailure = assertThrows(CompletionException.class, send::join).getCause();
        assertInstanceOf(IllegalStateException.class, sendFailure);
        assertSame(scenario.producerCloseFailure, sendFailure.getSuppressed()[0]);
        assertSame(scenario.sessionCloseFailure, sendFailure.getSuppressed()[1]);
        assertEquals(1, scenario.producerCloses.get());
        assertEquals(1, scenario.sessionCloses.get());
    }

    @Test
    void commitFailureClosesBothResourcesAndIsReturnedToTheSender() throws Exception {
        Scenario scenario = new Scenario();
        scenario.commitFailure = new JMSException("commit failed");
        JMSBenchmarkTransactionProducer producer = scenario.producer(true);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        scenario.listeners.get(0).onCompletion(null);

        assertSame(
                scenario.commitFailure, assertThrows(CompletionException.class, send::join).getCause());
        assertEquals(1, scenario.commits.get());
        assertEquals(1, scenario.producerCloses.get());
        assertEquals(1, scenario.sessionCloses.get());
        producer.close();
    }

    @Test
    void sessionAcquiredAfterCloseIsStillClosed() throws Exception {
        Scenario scenario = new Scenario();
        scenario.sessionEntered = new CountDownLatch(1);
        scenario.releaseSession = new CountDownLatch(1);
        JMSBenchmarkTransactionProducer producer = scenario.producer(true);
        AtomicReference<CompletableFuture<Void>> result = new AtomicReference<>();
        Thread sender =
                Thread.ofVirtual()
                        .start(() -> result.set(producer.sendAsync(Optional.empty(), new byte[] {1})));
        try {
            assertTrue(scenario.sessionEntered.await(2, TimeUnit.SECONDS));
            producer.close();
        } finally {
            scenario.releaseSession.countDown();
        }
        sender.join(2000);
        assertFalse(sender.isAlive());
        assertInstanceOf(
                IllegalStateException.class,
                assertThrows(CompletionException.class, () -> result.get().join()).getCause());
        assertEquals(1, scenario.sessionCloses.get());
        assertEquals(0, scenario.producerCloses.get());
        assertEquals(0, scenario.topicsCreated.get());
    }

    @Test
    void producerAcquiredAfterCloseIsStillClosed() throws Exception {
        Scenario scenario = new Scenario();
        scenario.producerEntered = new CountDownLatch(1);
        scenario.releaseProducer = new CountDownLatch(1);
        JMSBenchmarkTransactionProducer producer = scenario.producer(true);
        AtomicReference<CompletableFuture<Void>> result = new AtomicReference<>();
        Thread sender =
                Thread.ofVirtual()
                        .start(() -> result.set(producer.sendAsync(Optional.empty(), new byte[] {1})));
        try {
            assertTrue(scenario.producerEntered.await(2, TimeUnit.SECONDS));
            producer.close();
        } finally {
            scenario.releaseProducer.countDown();
        }
        sender.join(2000);
        assertFalse(sender.isAlive());
        assertInstanceOf(
                IllegalStateException.class,
                assertThrows(CompletionException.class, () -> result.get().join()).getCause());
        assertEquals(1, scenario.sessionCloses.get());
        assertEquals(1, scenario.producerCloses.get());
        assertEquals(0, scenario.listeners.size());
    }

    @Test
    void closeWaitsForAnAcknowledgementAlreadyCommitting() throws Exception {
        Scenario scenario = new Scenario();
        scenario.commitEntered = new CountDownLatch(1);
        scenario.releaseCommit = new CountDownLatch(1);
        JMSBenchmarkTransactionProducer producer = scenario.producer(true);
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        CompletableFuture<Void> closeFromCallback =
                send.whenComplete(
                        (ignored, error) -> {
                            try {
                                producer.close();
                            } catch (Exception closeError) {
                                callbackFailure.set(closeError);
                            }
                        });
        scenario.listeners.get(0).onCompletion(null);
        assertTrue(scenario.commitEntered.await(2, TimeUnit.SECONDS));
        CountDownLatch closeReturned = new CountDownLatch(1);
        Thread closer =
                Thread.ofVirtual()
                        .start(
                                () -> {
                                    try {
                                        producer.close();
                                    } catch (Exception error) {
                                        throw new AssertionError(error);
                                    } finally {
                                        closeReturned.countDown();
                                    }
                                });
        try {
            assertFalse(closeReturned.await(50, TimeUnit.MILLISECONDS));
        } finally {
            scenario.releaseCommit.countDown();
        }
        assertTrue(closeReturned.await(2, TimeUnit.SECONDS));
        closer.join();
        send.join();
        closeFromCallback.join();
        assertNull(callbackFailure.get());
        assertEquals(1, scenario.commits.get());
        assertEquals(1, scenario.producerCloses.get());
        assertEquals(1, scenario.sessionCloses.get());
    }

    @Test
    void cancellationCloseAndAcknowledgementRacesSettleExactlyOnce() throws Exception {
        for (int i = 0; i < 100; i++) {
            Scenario scenario = new Scenario();
            JMSBenchmarkTransactionProducer producer = scenario.producer(true);
            CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
            CountDownLatch start = new CountDownLatch(1);
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            List<Thread> threads = new ArrayList<>();
            threads.add(
                    Thread.ofVirtual()
                            .start(
                                    () -> {
                                        await(start);
                                        send.cancel(true);
                                    }));
            threads.add(
                    Thread.ofVirtual()
                            .start(
                                    () -> {
                                        await(start);
                                        try {
                                            producer.close();
                                        } catch (Exception error) {
                                            errors.add(error);
                                        }
                                    }));
            threads.add(
                    Thread.ofVirtual()
                            .start(
                                    () -> {
                                        await(start);
                                        scenario.listeners.get(0).onCompletion(null);
                                    }));
            start.countDown();
            for (Thread thread : threads) {
                thread.join(2000);
                assertFalse(thread.isAlive());
            }
            drainCallbacks();
            assertTrue(errors.isEmpty(), errors.toString());
            assertTrue(send.isDone());
            assertEquals(1, scenario.producerCloses.get());
            assertEquals(1, scenario.sessionCloses.get());
            assertTrue(scenario.commits.get() <= 1);
        }
    }

    private static void drainCallbacks() {
        assertTrue(ForkJoinPool.commonPool().awaitQuiescence(3, TimeUnit.SECONDS));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new AssertionError(error);
        }
    }

    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return type.cast(
                Proxy.newProxyInstance(
                        type.getClassLoader(),
                        new Class<?>[] {type},
                        (instance, method, args) -> invocation.invoke(method.getName(), args)));
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(String method, Object[] args) throws Throwable;
    }

    private static final class Scenario {
        private final AtomicInteger sessionsCreated = new AtomicInteger();
        private final AtomicInteger topicsCreated = new AtomicInteger();
        private final AtomicInteger connectionCloses = new AtomicInteger();
        private final AtomicInteger producerCloses = new AtomicInteger();
        private final AtomicInteger sessionCloses = new AtomicInteger();
        private final AtomicInteger commits = new AtomicInteger();
        private final List<CompletionListener> listeners = new CopyOnWriteArrayList<>();
        private JMSException sendFailure;
        private JMSException commitFailure;
        private JMSException producerCloseFailure;
        private JMSException sessionCloseFailure;
        private CountDownLatch sessionEntered;
        private CountDownLatch releaseSession;
        private CountDownLatch producerEntered;
        private CountDownLatch releaseProducer;
        private CountDownLatch commitEntered;
        private CountDownLatch releaseCommit;

        private JMSBenchmarkTransactionProducer producer(boolean async) {
            Connection connection =
                    proxy(
                            Connection.class,
                            (method, args) -> {
                                if (method.equals("createSession")) {
                                    sessionsCreated.incrementAndGet();
                                    if (sessionEntered != null) {
                                        sessionEntered.countDown();
                                        releaseSession.await();
                                    }
                                    return session();
                                }
                                if (method.equals("close")) {
                                    connectionCloses.incrementAndGet();
                                }
                                return null;
                            });
            return JMSBenchmarkTransactionProducer.create(connection, "topic", async, List.of());
        }

        private Session session() {
            Topic topic = proxy(Topic.class, (method, args) -> null);
            BytesMessage message = proxy(BytesMessage.class, (method, args) -> null);
            MessageProducer messageProducer =
                    proxy(
                            MessageProducer.class,
                            (method, args) -> {
                                if (method.equals("send")) {
                                    if (sendFailure != null) {
                                        throw sendFailure;
                                    }
                                    if (args.length == 2 && args[1] instanceof CompletionListener listener) {
                                        listeners.add(listener);
                                    }
                                } else if (method.equals("close")) {
                                    producerCloses.incrementAndGet();
                                    if (producerCloseFailure != null) {
                                        throw producerCloseFailure;
                                    }
                                }
                                return null;
                            });
            return proxy(
                    Session.class,
                    (method, args) ->
                            switch (method) {
                                case "createTopic" -> {
                                    topicsCreated.incrementAndGet();
                                    yield topic;
                                }
                                case "createProducer" -> {
                                    if (producerEntered != null) {
                                        producerEntered.countDown();
                                        releaseProducer.await();
                                    }
                                    yield messageProducer;
                                }
                                case "createBytesMessage" -> message;
                                case "commit" -> {
                                    commits.incrementAndGet();
                                    if (commitEntered != null) {
                                        commitEntered.countDown();
                                        releaseCommit.await();
                                    }
                                    if (commitFailure != null) {
                                        throw commitFailure;
                                    }
                                    yield null;
                                }
                                case "close" -> {
                                    sessionCloses.incrementAndGet();
                                    if (sessionCloseFailure != null) {
                                        throw sessionCloseFailure;
                                    }
                                    yield null;
                                }
                                default -> null;
                            });
        }
    }
}
