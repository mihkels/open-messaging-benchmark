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
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class NatsBenchmarkConsumerLifecycleTest {
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

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () -> fixture.driver.createConsumer("topic", "subscription", null).join());

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

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () -> fixture.driver.createConsumer("topic", "subscription", null).join());

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
