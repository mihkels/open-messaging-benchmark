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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.jms.config.JMSConfig;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import org.junit.jupiter.api.Test;

public class JMSBenchmarkDriverTest {
    @Test
    void closesSharedConnectionFactoryAndDelegateEvenWhenClosuresFail() throws Exception {
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();
        AtomicInteger connectionCloses = new AtomicInteger();
        AtomicInteger factoryCloses = new AtomicInteger();
        AtomicInteger delegateCloses = new AtomicInteger();
        JMSException connectionFailure = new JMSException("connection");
        IllegalStateException factoryFailure = new IllegalStateException("factory");
        IOException delegateFailure = new IOException("delegate");
        Connection connection =
                proxy(
                        Connection.class,
                        (method, args) -> {
                            if (method.equals("close")) {
                                connectionCloses.incrementAndGet();
                                throw connectionFailure;
                            }
                            return null;
                        });
        ConnectionFactory factory =
                proxy(
                        ConnectionFactory.class,
                        new Class<?>[] {ConnectionFactory.class, AutoCloseable.class},
                        (method, args) -> {
                            if (method.equals("close")) {
                                factoryCloses.incrementAndGet();
                                throw factoryFailure;
                            }
                            return null;
                        });
        TestDelegate delegate = new TestDelegate();
        delegate.onClose =
                () -> {
                    delegateCloses.incrementAndGet();
                    throw delegateFailure;
                };
        set(driver, "connection", connection);
        set(driver, "connectionFactory", factory);
        set(driver, "delegateForAdminOperations", delegate);

        assertSame(connectionFailure, assertThrows(JMSException.class, driver::close));
        assertSame(factoryFailure, connectionFailure.getSuppressed()[0]);
        assertSame(delegateFailure, connectionFailure.getSuppressed()[1]);
        driver.close();
        assertEquals(1, connectionCloses.get());
        assertEquals(1, factoryCloses.get());
        assertEquals(1, delegateCloses.get());
    }

    @Test
    void producerCreationFailureClosesOnlyItsPartialSession() throws Exception {
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();
        JMSException cause = new JMSException("topic");
        JMSException closeFailure = new JMSException("session close");
        AtomicInteger sessionCloses = new AtomicInteger();
        AtomicInteger connectionCloses = new AtomicInteger();
        Session session =
                proxy(
                        Session.class,
                        (method, args) -> {
                            if (method.equals("createTopic")) {
                                throw cause;
                            }
                            if (method.equals("close")) {
                                sessionCloses.incrementAndGet();
                                throw closeFailure;
                            }
                            return null;
                        });
        Connection connection =
                proxy(
                        Connection.class,
                        (method, args) -> {
                            if (method.equals("createSession")) {
                                return session;
                            }
                            if (method.equals("close")) {
                                connectionCloses.incrementAndGet();
                            }
                            return null;
                        });
        set(driver, "config", new JMSConfig());
        set(driver, "connection", connection);

        CompletionException failure =
                assertThrows(CompletionException.class, () -> driver.createProducer("topic").join());
        assertSame(cause, failure.getCause());
        assertSame(closeFailure, cause.getSuppressed()[0]);
        assertEquals(1, sessionCloses.get());
        assertEquals(0, connectionCloses.get());
        driver.close();
        assertEquals(1, connectionCloses.get());
    }

    @Test
    void consumerInitializationFailureClosesEveryPartialResourceOnce() throws Exception {
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();
        JMSException cause = new JMSException("listener");
        AtomicInteger consumerCloses = new AtomicInteger();
        AtomicInteger sessionCloses = new AtomicInteger();
        AtomicInteger connectionCloses = new AtomicInteger();
        MessageConsumer consumer =
                proxy(
                        MessageConsumer.class,
                        (method, args) -> {
                            if (method.equals("setMessageListener")) {
                                throw cause;
                            }
                            if (method.equals("close")) {
                                consumerCloses.incrementAndGet();
                            }
                            return null;
                        });
        Topic topic = proxy(Topic.class, (method, args) -> null);
        Session session =
                proxy(
                        Session.class,
                        (method, args) ->
                                switch (method) {
                                    case "createTopic" -> topic;
                                    case "createConsumer" -> consumer;
                                    case "close" -> {
                                        sessionCloses.incrementAndGet();
                                        yield null;
                                    }
                                    default -> null;
                                });
        Connection connection =
                proxy(
                        Connection.class,
                        (method, args) ->
                                switch (method) {
                                    case "createSession" -> session;
                                    case "close" -> {
                                        connectionCloses.incrementAndGet();
                                        yield null;
                                    }
                                    default -> null;
                                });
        ConnectionFactory factory =
                proxy(
                        ConnectionFactory.class,
                        (method, args) -> method.equals("createConnection") ? connection : null);
        set(driver, "config", new JMSConfig());
        set(driver, "connectionFactory", factory);

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () -> driver.createConsumer("topic", "subscription", null).join());
        assertSame(cause, failure.getCause());
        assertEquals(1, consumerCloses.get());
        assertEquals(1, sessionCloses.get());
        assertEquals(1, connectionCloses.get());
        driver.close();
        assertEquals(1, connectionCloses.get());
    }

    @Test
    void initializationFailureClosesConnectionFactoryAndAdminDelegate() throws Exception {
        JMSException cause = new JMSException("start");
        AtomicInteger connectionCloses = new AtomicInteger();
        TestFactory.closes.set(0);
        TestDelegate.closes.set(0);
        TestFactory.nextConnection =
                proxy(
                        Connection.class,
                        (method, args) -> {
                            if (method.equals("start")) {
                                throw cause;
                            }
                            if (method.equals("close")) {
                                connectionCloses.incrementAndGet();
                            }
                            return null;
                        });
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();
        Path config =
                Path.of(JMSBenchmarkDriverTest.class.getResource("/jms-creation-test.yaml").toURI());

        IOException failure =
                assertThrows(IOException.class, () -> driver.initialize(config, new SimpleMeterRegistry()));
        assertSame(cause, failure.getCause());
        assertEquals(1, connectionCloses.get());
        assertEquals(1, TestFactory.closes.get());
        assertEquals(1, TestDelegate.closes.get());
        driver.close();
        assertEquals(1, TestFactory.closes.get());
    }

    private static void set(Object instance, String name, Object value) throws Exception {
        Field field = instance.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(instance, value);
    }

    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return proxy(type, new Class<?>[] {type}, invocation);
    }

    private static <T> T proxy(Class<T> type, Class<?>[] interfaces, Invocation invocation) {
        return type.cast(
                Proxy.newProxyInstance(
                        type.getClassLoader(),
                        interfaces,
                        (instance, method, args) -> invocation.invoke(method.getName(), args)));
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(String method, Object[] args) throws Throwable;
    }

    public static final class TestFactory implements ConnectionFactory, AutoCloseable {
        private static final AtomicInteger closes = new AtomicInteger();
        private static Connection nextConnection;

        public TestFactory(String ignored) {}

        @Override
        public Connection createConnection() {
            return nextConnection;
        }

        @Override
        public Connection createConnection(String user, String password) {
            return nextConnection;
        }

        @Override
        public JMSContext createContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public JMSContext createContext(String user, String password) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JMSContext createContext(String user, String password, int mode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JMSContext createContext(int mode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    public static final class TestDelegate implements BenchmarkDriver {
        private static final AtomicInteger closes = new AtomicInteger();
        private CloseAction onClose = closes::incrementAndGet;

        public TestDelegate() {}

        @Override
        public void initialize(Path configurationFile, MeterRegistry statsLogger) {}

        @Override
        public String getTopicNamePrefix() {
            return "test";
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
        public CompletableFuture<BenchmarkConsumer> createConsumer(
                String topic, String subscriptionName, ConsumerCallback callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws Exception {
            onClose.close();
        }
    }

    @FunctionalInterface
    private interface CloseAction {
        void close() throws Exception;
    }
}
