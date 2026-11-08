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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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

        CompletableFuture<BenchmarkProducer> producer = driver.createProducer("topic");
        CompletionException failure = assertThrows(CompletionException.class, producer::join);
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

        CompletableFuture<BenchmarkConsumer> creation =
                driver.createConsumer("topic", "subscription", null);
        CompletionException failure = assertThrows(CompletionException.class, creation::join);
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

    @Test
    void initializesWithoutAdminDelegateAndUsesOwnTopicSettings() throws Exception {
        AtomicInteger starts = new AtomicInteger();
        TestFactory.nextConnection =
                proxy(
                        Connection.class,
                        (method, args) -> {
                            if (method.equals("start")) {
                                starts.incrementAndGet();
                            }
                            return null;
                        });
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();

        driver.initialize(resource("/jms-no-delegate-test.yaml"), new SimpleMeterRegistry());

        assertEquals(1, starts.get());
        assertEquals("jms-test", driver.getTopicNamePrefix());
        assertNull(driver.createTopic("topic", 3).join());
        driver.close();
    }

    @Test
    void adminDelegateHandlesTopicPrefixAndCreation() throws Exception {
        TestFactory.nextConnection = proxy(Connection.class, (method, args) -> null);
        TestDelegate.closes.set(0);
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();

        driver.initialize(resource("/jms-creation-test.yaml"), new SimpleMeterRegistry());

        assertEquals("test", driver.getTopicNamePrefix());
        assertNull(driver.createTopic("topic", 1).join());
        driver.close();
        assertEquals(1, TestDelegate.closes.get());
    }

    @Test
    void connectionFactoryCanBeBuiltFromJsonProperties() throws Exception {
        PropertiesFactory.lastProperties = null;
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();

        driver.initialize(resource("/jms-properties-factory-test.yaml"), new SimpleMeterRegistry());

        assertEquals("localhost:9092", PropertiesFactory.lastProperties.get("bootstrap.servers"));
        driver.close();
    }

    @Test
    void connectionFactoryWithoutSupportedConstructorFailsInitialization() {
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();

        IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                driver.initialize(
                                        resource("/jms-unsupported-factory-test.yaml"), new SimpleMeterRegistry()));

        assertInstanceOf(RuntimeException.class, failure.getCause());
    }

    @Test
    void connectionFactoryOfAnotherTypeIsRejectedBeforeItIsConstructed() {
        NotAFactory.instances.set(0);
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();

        IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                driver.initialize(
                                        resource("/jms-not-a-factory-test.yaml"), new SimpleMeterRegistry()));

        IllegalArgumentException cause =
                assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        assertEquals(
                "Class "
                        + NotAFactory.class.getName()
                        + " configured as a javax.jms.ConnectionFactory does not implement it",
                cause.getMessage());
        assertEquals(0, NotAFactory.instances.get());
    }

    @Test
    void adminDelegateOfAnotherTypeIsRejectedBeforeItIsConstructed() {
        NotAFactory.instances.set(0);
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();

        IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                driver.initialize(
                                        resource("/jms-not-a-delegate-test.yaml"), new SimpleMeterRegistry()));

        IllegalArgumentException cause =
                assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        assertEquals(
                "Class "
                        + NotAFactory.class.getName()
                        + " configured as a "
                        + BenchmarkDriver.class.getName()
                        + " does not implement it",
                cause.getMessage());
        assertEquals(0, NotAFactory.instances.get());
    }

    @Test
    void initializationCleanupFailureIsSuppressedIntoTheCause() {
        JMSException cause = new JMSException("start");
        JMSException closeFailure = new JMSException("close");
        TestFactory.nextConnection =
                proxy(
                        Connection.class,
                        (method, args) ->
                                switch (method) {
                                    case "start" -> throw cause;
                                    case "close" -> throw closeFailure;
                                    default -> null;
                                });
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();

        IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                driver.initialize(resource("/jms-creation-test.yaml"), new SimpleMeterRegistry()));

        assertSame(cause, failure.getCause());
        assertSame(closeFailure, cause.getSuppressed()[0]);
    }

    @Test
    void initializationCleanupRethrowingTheCauseIsNotSuppressedIntoItself() {
        JMSException cause = new JMSException("broken connection");
        TestFactory.nextConnection =
                proxy(
                        Connection.class,
                        (method, args) ->
                                switch (method) {
                                    case "start", "close" -> throw cause;
                                    default -> null;
                                });
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();

        IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                driver.initialize(resource("/jms-creation-test.yaml"), new SimpleMeterRegistry()));

        assertSame(cause, failure.getCause());
        assertEquals(0, cause.getSuppressed().length);
    }

    @Test
    void interruptedAdminDelegateInitializationIsRethrown() {
        InterruptedException interruption = new InterruptedException("delegate");
        TestDelegate.initFailure = interruption;
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();
        try {
            assertSame(
                    interruption,
                    assertThrows(
                            InterruptedException.class,
                            () ->
                                    driver.initialize(
                                            resource("/jms-creation-test.yaml"), new SimpleMeterRegistry())));
            assertTrue(Thread.interrupted());
        } finally {
            TestDelegate.initFailure = null;
        }
    }

    @Test
    void interruptedCloseRestoresInterruptFlag() throws Exception {
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();
        InterruptedException interruption = new InterruptedException("close");
        TestDelegate delegate = new TestDelegate();
        delegate.onClose =
                () -> {
                    throw interruption;
                };
        set(driver, "delegateForAdminOperations", delegate);

        assertSame(interruption, assertThrows(InterruptedException.class, driver::close));
        assertTrue(Thread.interrupted());
    }

    @Test
    void sameCloseFailureFromSeveralResourcesIsReportedOnce() throws Exception {
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();
        JMSException failure = new JMSException("shared");
        set(
                driver,
                "connection",
                proxy(
                        Connection.class,
                        (method, args) -> {
                            if (method.equals("close")) {
                                throw failure;
                            }
                            return null;
                        }));
        set(
                driver,
                "connectionFactory",
                proxy(
                        ConnectionFactory.class,
                        new Class<?>[] {ConnectionFactory.class, AutoCloseable.class},
                        (method, args) -> {
                            if (method.equals("close")) {
                                throw failure;
                            }
                            return null;
                        }));

        assertSame(failure, assertThrows(JMSException.class, driver::close));
        assertEquals(0, failure.getSuppressed().length);
    }

    @Test
    void transactionalConfigurationCreatesTransactionalProducer() throws Exception {
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();
        JMSConfig config = new JMSConfig();
        config.sendWithTransactions = true;
        set(driver, "config", config);
        set(driver, "connection", proxy(Connection.class, (method, args) -> null));

        BenchmarkProducer producer = driver.createProducer("topic").join();

        assertInstanceOf(JMSBenchmarkTransactionProducer.class, producer);
        producer.close();
    }

    @Test
    void consumerUsesSharedDurableSubscriptionAndSelectorWithJms20() throws Exception {
        JMSConfig config = new JMSConfig();
        config.use20api = true;
        config.messageSelector = "region = 'eu'";
        List<String> calls = new ArrayList<>();
        JMSBenchmarkDriver driver = consumerDriver(config, calls);

        driver.createConsumer("topic", "subscription", null).join().close();

        assertEquals(List.of("createSharedDurableConsumer:subscription:region = 'eu'"), calls);
    }

    @Test
    void emptySelectorIsTreatedAsNoSelector() throws Exception {
        JMSConfig config = new JMSConfig();
        config.messageSelector = "";
        List<String> calls = new ArrayList<>();
        JMSBenchmarkDriver driver = consumerDriver(config, calls);

        driver.createConsumer("topic", "subscription", null).join().close();

        assertEquals(List.of("createConsumer:null"), calls);
    }

    private static JMSBenchmarkDriver consumerDriver(JMSConfig config, List<String> calls)
            throws Exception {
        MessageConsumer consumer = proxy(MessageConsumer.class, (method, args) -> null);
        Topic topic = proxy(Topic.class, (method, args) -> null);
        Session session =
                proxy(
                        Session.class,
                        (method, args) ->
                                switch (method) {
                                    case "createTopic" -> topic;
                                    case "createSharedDurableConsumer" -> {
                                        calls.add(method + ":" + args[1] + ":" + args[2]);
                                        yield consumer;
                                    }
                                    case "createConsumer" -> {
                                        calls.add(method + ":" + args[1]);
                                        yield consumer;
                                    }
                                    default -> null;
                                });
        Connection connection =
                proxy(Connection.class, (method, args) -> method.equals("createSession") ? session : null);
        ConnectionFactory factory =
                proxy(
                        ConnectionFactory.class,
                        (method, args) -> method.equals("createConnection") ? connection : null);
        JMSBenchmarkDriver driver = new JMSBenchmarkDriver();
        set(driver, "config", config);
        set(driver, "connectionFactory", factory);
        return driver;
    }

    private static Path resource(String name) throws Exception {
        return Path.of(JMSBenchmarkDriverTest.class.getResource(name).toURI());
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

    public static class TestFactory implements ConnectionFactory, AutoCloseable {
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

    public static final class PropertiesFactory extends TestFactory {
        private static Properties lastProperties;

        public PropertiesFactory(Properties properties) {
            super("properties");
            lastProperties = properties;
        }

        @Override
        public Connection createConnection() {
            return proxy(Connection.class, (method, args) -> null);
        }
    }

    /** Has both constructors the driver tries, but is neither a factory nor a driver. */
    public static final class NotAFactory {
        private static final AtomicInteger instances = new AtomicInteger();

        public NotAFactory() {
            instances.incrementAndGet();
        }

        public NotAFactory(String ignored) {
            this();
        }
    }

    public static final class NoArgFactory extends TestFactory {
        public NoArgFactory() {
            super("none");
        }
    }

    public static final class TestDelegate implements BenchmarkDriver {
        private static final AtomicInteger closes = new AtomicInteger();
        private static volatile InterruptedException initFailure;
        private CloseAction onClose = closes::incrementAndGet;

        public TestDelegate() {
            // no-op: instantiated reflectively by the driver
        }

        @Override
        public void initialize(Path configurationFile, MeterRegistry statsLogger)
                throws InterruptedException {
            if (initFailure != null) {
                throw initFailure;
            }
        }

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
