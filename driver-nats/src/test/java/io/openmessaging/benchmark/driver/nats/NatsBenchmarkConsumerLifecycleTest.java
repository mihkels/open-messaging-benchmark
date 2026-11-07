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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class NatsBenchmarkConsumerLifecycleTest {
    @Test
    void deliveryInvokesCallbackWithPublishTimestampAndAcknowledgesMessage() throws Exception {
        Fixture fixture = new Fixture();
        AtomicReference<byte[]> received = new AtomicReference<>();
        AtomicReference<Long> timestamp = new AtomicReference<>();
        AtomicInteger acknowledgements = new AtomicInteger();
        ConsumerCallback callback =
                new ConsumerCallback() {
                    @Override
                    public void messageReceived(byte[] payload, long publishTimestamp) {
                        received.set(payload);
                        timestamp.set(publishTimestamp);
                    }

                    @Override
                    public void messageReceived(ByteBuffer payload, long publishTimestamp) {
                        throw new AssertionError("byte array callback expected");
                    }
                };
        fixture.driver.createConsumer("topic", "subscription", callback).join();

        byte[] body = ByteBuffer.allocate(Long.BYTES + 1).putLong(1234L).put((byte) 9).array();
        io.nats.client.Message message =
                proxy(
                        io.nats.client.Message.class,
                        (method, args) -> {
                            if (method.equals("getData")) {
                                return body;
                            }
                            if (method.equals("ack")) {
                                acknowledgements.incrementAndGet();
                            }
                            return null;
                        });
        fixture.messageHandler.onMessage(message);

        assertSame(body, received.get());
        assertEquals(1234L, timestamp.get());
        assertEquals(1, acknowledgements.get());
    }

    @Test
    void consumerClosesOnlyItsDispatcher() throws Exception {
        Fixture fixture = new Fixture();
        BenchmarkConsumer consumer =
                fixture.driver.createConsumer("topic", "subscription", null).join();

        consumer.close();
        consumer.close();

        assertEquals(1, fixture.dispatcherCloses.get());
        assertEquals(0, fixture.connectionCloses.get());
    }

    @Test
    void subscriptionFailureClosesTheDispatcherAndPreservesCleanupFailure() throws Exception {
        Fixture fixture = new Fixture();
        IOException cause = new IOException("subscription failed");
        IllegalStateException closeFailure = new IllegalStateException("dispatcher close failed");
        fixture.subscribeFailure = cause;
        fixture.closeFailure = closeFailure;

        CompletableFuture<BenchmarkConsumer> creation =
                fixture.driver.createConsumer("topic", "subscription", null);
        CompletionException failure = assertThrows(CompletionException.class, creation::join);

        assertSame(cause, failure.getCause());
        assertEquals(1, cause.getSuppressed().length);
        assertSame(closeFailure, cause.getSuppressed()[0]);
        assertEquals(1, fixture.dispatcherCloses.get());
        assertEquals(0, fixture.connectionCloses.get());
    }

    @Test
    void missingSubscriptionFailsAndClosesTheDispatcher() throws Exception {
        Fixture fixture = new Fixture();
        fixture.returnNullSubscription = true;

        CompletableFuture<BenchmarkConsumer> creation =
                fixture.driver.createConsumer("topic", "subscription", null);
        CompletionException failure = assertThrows(CompletionException.class, creation::join);

        assertEquals(NullPointerException.class, failure.getCause().getClass());
        assertEquals(1, fixture.dispatcherCloses.get());
    }

    private static final class Fixture {
        private final NatsBenchmarkDriver driver = new NatsBenchmarkDriver();
        private final AtomicInteger dispatcherCloses = new AtomicInteger();
        private final AtomicInteger connectionCloses = new AtomicInteger();
        private IOException subscribeFailure;
        private RuntimeException closeFailure;
        private boolean returnNullSubscription;
        private io.nats.client.MessageHandler messageHandler;

        private Fixture() throws ReflectiveOperationException {
            Dispatcher dispatcher = proxy(Dispatcher.class, (method, args) -> null);
            JetStreamSubscription subscription =
                    proxy(JetStreamSubscription.class, (method, args) -> null);
            Connection connection =
                    proxy(
                            Connection.class,
                            (method, args) -> {
                                switch (method) {
                                    case "createDispatcher":
                                        return dispatcher;
                                    case "closeDispatcher":
                                        assertSame(dispatcher, args[0]);
                                        dispatcherCloses.incrementAndGet();
                                        if (closeFailure != null) {
                                            throw closeFailure;
                                        }
                                        return null;
                                    case "close":
                                        connectionCloses.incrementAndGet();
                                        return null;
                                    default:
                                        throw new AssertionError("Unexpected connection method: " + method);
                                }
                            });
            JetStream stream =
                    proxy(
                            JetStream.class,
                            (method, args) -> {
                                if (subscribeFailure != null) {
                                    throw subscribeFailure;
                                }
                                if (method.equals("subscribe")) {
                                    messageHandler = (io.nats.client.MessageHandler) args[2];
                                }
                                return returnNullSubscription ? null : subscription;
                            });
            JetStreamManagement management = proxy(JetStreamManagement.class, (method, args) -> null);
            setField(driver, "connection", connection);
            setField(driver, "jetStream", stream);
            setField(driver, "jetStreamManagement", management);
        }
    }

    private static void setField(NatsBenchmarkDriver driver, String name, Object value)
            throws ReflectiveOperationException {
        Field field = NatsBenchmarkDriver.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(driver, value);
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
}
