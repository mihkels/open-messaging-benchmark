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

import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConfiguredClasses;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.DriverConfigs;
import io.openmessaging.benchmark.driver.Failures;
import io.openmessaging.benchmark.driver.jms.config.JMSConfig;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

public class JMSBenchmarkDriver implements BenchmarkDriver {

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private JMSConfig config;
    private BenchmarkDriver delegateForAdminOperations;

    @Override
    public void initialize(Path configurationFile, MeterRegistry statsLogger)
            throws IOException, InterruptedException {
        this.config = readConfig(configurationFile);
        try {
            if (config.delegateForAdminOperationsClassName != null
                    && !config.delegateForAdminOperationsClassName.isEmpty()) {
                log.info(
                        "Initializing Driver for Admin operations {}",
                        config.delegateForAdminOperationsClassName);
                delegateForAdminOperations =
                        ConfiguredClasses.load(
                                        config.delegateForAdminOperationsClassName,
                                        BenchmarkDriver.class,
                                        JMSBenchmarkDriver.class.getClassLoader())
                                .getConstructor()
                                .newInstance();
                delegateForAdminOperations.initialize(configurationFile, statsLogger);
            }

            connectionFactory = buildConnectionFactory();
            connection = connectionFactory.createConnection();
            connection.start();
        } catch (Exception t) {
            try {
                close();
            } catch (Exception closeFailure) {
                if (closeFailure != t) {
                    t.addSuppressed(closeFailure);
                }
            }
            if (t instanceof InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            throw new IOException("Cannot initialize JMS benchmark driver", t);
        }
    }

    private ConnectionFactory buildConnectionFactory() throws ReflectiveOperationException {
        Class<? extends ConnectionFactory> clazz =
                ConfiguredClasses.load(
                        config.connectionFactoryClassName,
                        ConnectionFactory.class,
                        Thread.currentThread().getContextClassLoader());

        // constructor with a String (like DataStax Pulsar JMS)
        try {
            Constructor<? extends ConnectionFactory> constructor = clazz.getConstructor(String.class);
            return constructor.newInstance(config.connectionFactoryConfigurationParam);
        } catch (NoSuchMethodException _) {
            // No String constructor: try the Properties one
        }

        // constructor with Properties (like Confluent Kafka)
        try {
            Constructor<? extends ConnectionFactory> constructor = clazz.getConstructor(Properties.class);
            Properties props = new Properties();
            ObjectMapper mapper = new ObjectMapper();
            Map map =
                    mapper.readValue(new StringReader(config.connectionFactoryConfigurationParam), Map.class);
            props.putAll(map);
            return constructor.newInstance(props);
        } catch (NoSuchMethodException _) {
            // No Properties constructor either: reported below
        }

        throw new IllegalArgumentException(
                "Connection factory "
                        + clazz.getName()
                        + " has neither a String nor a Properties constructor");
    }

    @Override
    public String getTopicNamePrefix() {
        if (delegateForAdminOperations != null) {
            return delegateForAdminOperations.getTopicNamePrefix();
        }
        return config.topicNamePrefix;
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        if (delegateForAdminOperations != null) {
            return delegateForAdminOperations.createTopic(topic, partitions);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        Session session = null;
        try {
            if (config.sendWithTransactions) {
                return CompletableFuture.completedFuture(
                        JMSBenchmarkTransactionProducer.create(
                                connection, topic, config.use20api, config.properties));
            } else {
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createTopic(topic);
                return CompletableFuture.completedFuture(
                        JMSBenchmarkProducer.create(session, destination, config.use20api, config.properties));
            }
        } catch (Exception err) {
            Failures.closeAfterFailure(err, session);
            return CompletableFuture.failedFuture(err);
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        Connection consumerConnection = null;
        Session session = null;
        MessageConsumer durableConsumer = null;
        try {
            String selector =
                    config.messageSelector != null && !config.messageSelector.isEmpty()
                            ? config.messageSelector
                            : null;
            consumerConnection = connectionFactory.createConnection();
            session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic destination = session.createTopic(topic);
            if (config.use20api) {
                durableConsumer =
                        session.createSharedDurableConsumer(destination, subscriptionName, selector);
            } else {
                // in JMS 1.0 we should use session.createDurableSubscriber()
                // but it is not supported in Confluent Kafka JMS client
                durableConsumer = session.createConsumer(destination, selector);
            }
            return CompletableFuture.completedFuture(
                    JMSBenchmarkConsumer.create(
                            consumerConnection, durableConsumer, consumerCallback, config.use20api));
        } catch (Exception err) {
            Failures.closeAfterFailure(err, durableConsumer, session, consumerConnection);
            return CompletableFuture.failedFuture(err);
        }
    }

    @Override
    public synchronized void close() throws Exception {
        log.info("Shutting down JMS benchmark driver");
        Connection producerConnection = connection;
        ConnectionFactory factory = connectionFactory;
        BenchmarkDriver delegate = delegateForAdminOperations;
        connection = null;
        connectionFactory = null;
        delegateForAdminOperations = null;

        Failures failures = new Failures();
        failures.closeRestoringInterruptNow(producerConnection);
        if (factory instanceof AutoCloseable closeable) {
            failures.closeRestoringInterruptNow(closeable);
        }
        failures.closeRestoringInterruptNow(delegate);
        failures.throwIfAny();
        log.info("JMS benchmark driver successfully shut down");
    }

    private static JMSConfig readConfig(Path configurationFile) throws IOException {
        return DriverConfigs.read(configurationFile, JMSConfig.class);
    }

    private static final Logger log = LoggerFactory.getLogger(JMSBenchmarkDriver.class);
}
