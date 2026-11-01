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

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.jms.config.JMSConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JMSBenchmarkTransactionProducer implements BenchmarkProducer {

    private static final Logger log = LoggerFactory.getLogger(JMSBenchmarkTransactionProducer.class);

    private final String destination;
    private final boolean useAsyncSend;
    private final Connection connection;
    private final List<JMSConfig.AddProperty> properties;
    private final Object lifecycleLock = new Object();
    private final Set<SendOperation> pending = new HashSet<>();
    private final CountDownLatch closeComplete = new CountDownLatch(1);
    private boolean closed;
    private Thread closingThread;
    private Exception closeFailure;

    private JMSBenchmarkTransactionProducer(
            Connection connection,
            String destination,
            boolean useAsyncSend,
            List<JMSConfig.AddProperty> properties) {
        this.destination = destination;
        this.useAsyncSend = useAsyncSend;
        this.connection = connection;
        this.properties = properties != null ? properties : Collections.emptyList();
    }

    /**
     * Creates a producer which owns a session and message producer for each send.
     *
     * @param connection the driver-owned connection
     * @param destination the destination topic name
     * @param useAsyncSend whether to use asynchronous sending
     * @param properties additional properties to set on messages
     * @return a new transactional producer
     */
    public static JMSBenchmarkTransactionProducer create(
            Connection connection,
            String destination,
            boolean useAsyncSend,
            List<JMSConfig.AddProperty> properties) {
        return new JMSBenchmarkTransactionProducer(connection, destination, useAsyncSend, properties);
    }

    @Override
    public void close() throws Exception {
        List<SendOperation> operations = null;
        synchronized (lifecycleLock) {
            if (closed && closingThread == Thread.currentThread()) {
                return;
            }
            if (!closed) {
                closed = true;
                closingThread = Thread.currentThread();
                operations = new ArrayList<>(pending);
            }
        }

        if (operations == null) {
            try {
                closeComplete.await();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw error;
            }
            if (closeFailure != null) {
                throw closeFailure;
            }
            return;
        }

        try {
            for (SendOperation operation : operations) {
                operation.finish(new IllegalStateException("JMS transactional producer is closed"), false);
            }

            Exception failure = null;
            boolean interrupted = false;
            for (SendOperation operation : operations) {
                while (!operation.isSettled()) {
                    try {
                        operation.awaitSettlement();
                    } catch (InterruptedException error) {
                        interrupted = true;
                    }
                }
                for (Exception cleanupFailure : operation.cleanupFailures()) {
                    if (failure == null) {
                        failure = cleanupFailure;
                    } else if (failure != cleanupFailure) {
                        failure.addSuppressed(cleanupFailure);
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
                InterruptedException error =
                        new InterruptedException("Interrupted while closing JMS producer");
                if (failure != null) {
                    error.addSuppressed(failure);
                }
                failure = error;
            }
            closeFailure = failure;
            if (failure != null) {
                throw failure;
            }
        } finally {
            synchronized (lifecycleLock) {
                closingThread = null;
            }
            closeComplete.countDown();
        }
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        SendOperation operation = new SendOperation();
        synchronized (lifecycleLock) {
            if (closed) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("JMS transactional producer is closed"));
            }
            pending.add(operation);
        }

        try {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            if (!operation.attachSession(session)) {
                return operation.result;
            }
            Topic topic = session.createTopic(destination);
            if (!operation.isActive()) {
                return operation.result;
            }
            MessageProducer producer = session.createProducer(topic);
            if (!operation.attachProducer(producer)) {
                return operation.result;
            }
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(payload);
            if (key.isPresent()) {
                bytesMessage.setStringProperty("JMSXGroupID", key.get());
            }
            for (JMSConfig.AddProperty prop : properties) {
                bytesMessage.setStringProperty(prop.name, prop.value);
            }
            bytesMessage.setLongProperty("E2EStartMillis", System.currentTimeMillis());
            if (!operation.isActive()) {
                return operation.result;
            }

            if (useAsyncSend) {
                CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
                acknowledgement
                        .whenCompleteAsync((ignored, error) -> operation.finish(error, error == null))
                        .whenComplete(
                                (ignored, error) -> {
                                    if (error != null) {
                                        operation.finish(error, false);
                                    }
                                });
                producer.send(
                        bytesMessage,
                        new CompletionListener() {
                            @Override
                            public void onCompletion(Message message) {
                                acknowledgement.complete(null);
                            }

                            @Override
                            public void onException(Message message, Exception exception) {
                                acknowledgement.completeExceptionally(
                                        exception != null
                                                ? exception
                                                : new JMSException("JMS provider reported a failure without a cause"));
                            }
                        });
            } else {
                producer.send(bytesMessage);
                operation.finish(null, true);
            }
        } catch (JMSException | RuntimeException error) {
            operation.finish(error, false);
        }
        return operation.result;
    }

    private final class SendOperation {
        private final Object lock = new Object();
        private final CountDownLatch resourcesReleased = new CountDownLatch(1);
        private final List<Exception> cleanupFailures = new ArrayList<>();
        private final SendFuture result = new SendFuture();
        private Session session;
        private MessageProducer producer;
        private Throwable failure;
        private boolean finished;
        private boolean resourcesClosed;

        private boolean attachSession(Session created) {
            synchronized (lock) {
                if (!finished) {
                    session = created;
                    return true;
                }
            }
            closeSession(created);
            return false;
        }

        private boolean attachProducer(MessageProducer created) {
            synchronized (lock) {
                if (!finished) {
                    producer = created;
                    return true;
                }
            }
            closeProducer(created);
            return false;
        }

        private boolean isActive() {
            synchronized (lock) {
                return !finished;
            }
        }

        private boolean finish(Throwable error, boolean commit) {
            Session sessionToClose;
            MessageProducer producerToClose;
            synchronized (lock) {
                if (finished) {
                    return error instanceof CancellationException && failure instanceof CancellationException;
                }
                finished = true;
                failure = unwrap(error);
                sessionToClose = session;
                producerToClose = producer;
                session = null;
                producer = null;
            }

            if (commit && sessionToClose != null) {
                try {
                    sessionToClose.commit();
                } catch (JMSException | RuntimeException commitFailure) {
                    recordFailure(commitFailure, false);
                }
            }
            closeProducer(producerToClose);
            closeSession(sessionToClose);

            Throwable outcome;
            synchronized (lock) {
                resourcesClosed = true;
                outcome = failure;
            }
            resourcesReleased.countDown();
            try {
                if (outcome == null) {
                    result.succeed();
                } else {
                    result.fail(outcome);
                }
            } finally {
                synchronized (lifecycleLock) {
                    pending.remove(this);
                }
            }
            return true;
        }

        private void closeProducer(MessageProducer resource) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (JMSException | RuntimeException error) {
                    recordFailure(error, true);
                }
            }
        }

        private void closeSession(Session resource) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (JMSException | RuntimeException error) {
                    recordFailure(error, true);
                }
            }
        }

        private void recordFailure(Exception error, boolean cleanup) {
            synchronized (lock) {
                if (failure == null) {
                    failure = error;
                } else if (failure != error) {
                    failure.addSuppressed(error);
                }
                if (cleanup) {
                    cleanupFailures.add(error);
                }
            }
            if (cleanup) {
                log.warn("Failed to close a JMS transactional send resource", error);
            }
        }

        private boolean isSettled() {
            synchronized (lock) {
                return resourcesClosed && result.isDone();
            }
        }

        private void awaitSettlement() throws InterruptedException {
            resourcesReleased.await();
            try {
                result.join();
            } catch (CompletionException | CancellationException ignored) {
                // The exceptional result is already available to the sender.
            }
        }

        private List<Exception> cleanupFailures() {
            synchronized (lock) {
                return List.copyOf(cleanupFailures);
            }
        }

        private final class SendFuture extends CompletableFuture<Void> {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return finish(new CancellationException("JMS send was cancelled"), false);
            }

            private void succeed() {
                super.complete(null);
            }

            private void fail(Throwable error) {
                super.completeExceptionally(error);
            }
        }
    }

    private static Throwable unwrap(Throwable error) {
        if (error instanceof CompletionException && error.getCause() != null) {
            return error.getCause();
        }
        return error;
    }
}
