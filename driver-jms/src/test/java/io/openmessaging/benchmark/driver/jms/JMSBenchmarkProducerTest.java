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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.junit.jupiter.api.Test;

class JMSBenchmarkProducerTest {
    @Test
    void closeSettlesPendingSendsAndRejectsLateAcknowledgements() throws Exception {
        Fixture fixture = new Fixture();
        JMSBenchmarkProducer producer = fixture.producer();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        fixture.listeners.get(0).onCompletion(null);
        assertNull(first.join());

        producer.close();

        assertClosed(second);
        fixture.listeners.get(1).onCompletion(null);
        assertClosed(second);
        assertClosed(producer.sendAsync(Optional.empty(), new byte[] {3}));
        producer.close();
        assertEquals(1, fixture.sessionCloses.get());
    }

    @Test
    void acknowledgementDuringSessionCloseIsPreserved() throws Exception {
        Fixture fixture = new Fixture();
        JMSBenchmarkProducer producer = fixture.producer();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        fixture.onClose = () -> fixture.listeners.get(0).onCompletion(null);

        producer.close();

        assertNull(first.join());
        assertClosed(second);
    }

    @Test
    void closeFailureStillSettlesOutstandingSends() throws Exception {
        Fixture fixture = new Fixture();
        fixture.closeFailure = new JMSException("close failed");
        JMSBenchmarkProducer producer = fixture.producer();
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertSame(fixture.closeFailure, assertThrows(JMSException.class, producer::close));
        assertClosed(send);
        assertSame(
                fixture.closeFailure,
                assertThrows(CompletionException.class, send::join).getCause().getCause());
        assertEquals(1, fixture.sessionCloses.get());
    }

    @Test
    void synchronousRuntimeFailureCompletesExceptionally() throws Exception {
        Fixture fixture = new Fixture();
        fixture.sendFailure = new IllegalStateException("send failed");
        JMSBenchmarkProducer producer = fixture.producer();

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () -> producer.sendAsync(Optional.empty(), new byte[] {1}).join());

        assertSame(fixture.sendFailure, failure.getCause());
        producer.close();
    }

    @Test
    void missingProviderFailureDoesNotCompleteSuccessfully() throws Exception {
        Fixture fixture = new Fixture();
        JMSBenchmarkProducer producer = fixture.producer();
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        fixture.listeners.get(0).onException(null, null);

        assertInstanceOf(
                JMSException.class, assertThrows(CompletionException.class, send::join).getCause());
        producer.close();
    }

    @Test
    void closeFromCompletionCallbackIsRejectedUntilCallbackReturns() throws Exception {
        Fixture fixture = new Fixture();
        JMSBenchmarkProducer producer = fixture.producer();
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        AtomicReference<Exception> closeFailure = new AtomicReference<>();
        first.whenComplete(
                (unused, failure) -> {
                    try {
                        producer.close();
                    } catch (Exception e) {
                        closeFailure.set(e);
                    }
                });

        fixture.listeners.get(0).onCompletion(null);

        assertInstanceOf(IllegalStateException.class, closeFailure.get());
        assertEquals(0, fixture.sessionCloses.get());
        assertFalse(second.isDone());
        producer.close();
        assertClosed(second);
        assertEquals(1, fixture.sessionCloses.get());
    }

    @Test
    void concurrentCloseSettlesSendsWhileSessionCloseIsBlocked() throws Exception {
        Fixture fixture = new Fixture();
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        fixture.onClose =
                () -> {
                    closing.countDown();
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("session close was not released");
                    }
                };
        JMSBenchmarkProducer producer = fixture.producer();
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        Thread firstClose =
                Thread.ofVirtual()
                        .start(
                                () -> {
                                    try {
                                        producer.close();
                                    } catch (Throwable failure) {
                                        firstFailure.set(failure);
                                    }
                                });
        try {
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            producer.close();
            assertClosed(send);
        } finally {
            release.countDown();
            firstClose.join(5000);
        }
        assertFalse(firstClose.isAlive());
        assertNull(firstFailure.get());
        assertEquals(1, fixture.sessionCloses.get());
    }

    @Test
    void closeWaitsForAnInProgressSendBeforeClosingSession() throws Exception {
        Fixture fixture = new Fixture();
        CountDownLatch sending = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        fixture.onSend =
                () -> {
                    sending.countDown();
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("send was not released");
                    }
                };
        JMSBenchmarkProducer producer = fixture.producer();
        AtomicReference<CompletableFuture<Void>> sent = new AtomicReference<>();
        Thread sender =
                Thread.ofVirtual()
                        .start(() -> sent.set(producer.sendAsync(Optional.empty(), new byte[] {1})));
        try {
            assertTrue(sending.await(5, TimeUnit.SECONDS));
            CompletableFuture<Void> close =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    producer.close();
                                } catch (Exception failure) {
                                    throw new CompletionException(failure);
                                }
                            });
            assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            assertEquals(0, fixture.sessionCloses.get());
            release.countDown();
            sender.join(5000);
            close.get(5, TimeUnit.SECONDS);
            assertFalse(sender.isAlive());
            assertClosed(sent.get());
            assertEquals(1, fixture.sessionCloses.get());
        } finally {
            release.countDown();
            sender.join(5000);
        }
    }

    private static void assertClosed(CompletableFuture<Void> future) {
        assertInstanceOf(
                IllegalStateException.class,
                assertThrows(CompletionException.class, future::join).getCause());
    }

    private static final class Fixture {
        private final List<CompletionListener> listeners = new ArrayList<>();
        private final AtomicInteger sessionCloses = new AtomicInteger();
        private JMSException closeFailure;
        private RuntimeException sendFailure;
        private Action onClose;
        private Action onSend;

        private JMSBenchmarkProducer producer() throws Exception {
            BytesMessage message = proxy(BytesMessage.class, (method, args) -> null);
            MessageProducer producer =
                    proxy(
                            MessageProducer.class,
                            (method, args) -> {
                                if (method.equals("send")) {
                                    if (sendFailure != null) {
                                        throw sendFailure;
                                    }
                                    if (onSend != null) {
                                        onSend.run();
                                    }
                                    listeners.add((CompletionListener) args[1]);
                                }
                                return null;
                            });
            Session session =
                    proxy(
                            Session.class,
                            (method, args) -> {
                                switch (method) {
                                    case "createProducer":
                                        return producer;
                                    case "createBytesMessage":
                                        return message;
                                    case "close":
                                        sessionCloses.incrementAndGet();
                                        if (onClose != null) {
                                            onClose.run();
                                        }
                                        if (closeFailure != null) {
                                            throw closeFailure;
                                        }
                                        return null;
                                    default:
                                        throw new AssertionError("Unexpected session method: " + method);
                                }
                            });
            return JMSBenchmarkProducer.create(
                    session, proxy(Destination.class, (m, a) -> null), true, List.of());
        }
    }

    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return type.cast(
                Proxy.newProxyInstance(
                        type.getClassLoader(),
                        new Class<?>[] {type},
                        (object, method, args) -> invocation.invoke(method.getName(), args)));
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(String method, Object[] args) throws Throwable;
    }

    @FunctionalInterface
    private interface Action {
        void run() throws Exception;
    }
}
