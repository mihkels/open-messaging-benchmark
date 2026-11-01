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
package io.openmessaging.benchmark.driver.artemis;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.PendingSendTracker;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;

public final class ArtemisBenchmarkProducer implements BenchmarkProducer {

    private final ClientSession session;
    private final ClientProducer producer;
    private final Object lifecycleLock = new Object();
    private final PendingSendTracker pending = new PendingSendTracker("Artemis producer");

    // Private constructor - cannot throw exceptions
    private ArtemisBenchmarkProducer(ClientSession session, ClientProducer producer) {
        this.session = session;
        this.producer = producer;
    }

    /**
     * Factory method to create ArtemisBenchmarkProducer safely. This method handles all the
     * exception-throwing initialization logic.
     *
     * @param address the address to send messages to
     * @param sessionFactory the client session factory
     * @return a new ArtemisBenchmarkProducer instance
     * @throws ActiveMQException if initialization fails
     */
    public static ArtemisBenchmarkProducer create(String address, ClientSessionFactory sessionFactory)
            throws ActiveMQException {

        ClientSession tempSession = null;
        ClientProducer tempProducer = null;

        try {
            tempSession = sessionFactory.createSession();
            tempProducer = tempSession.createProducer(address);
            tempSession.start();

            // Create the producer instance only after all operations succeed
            return new ArtemisBenchmarkProducer(tempSession, tempProducer);

        } catch (ActiveMQException | RuntimeException e) {
            if (tempProducer != null) {
                try {
                    tempProducer.close();
                } catch (ActiveMQException | RuntimeException closeException) {
                    if (closeException != e) {
                        e.addSuppressed(closeException);
                    }
                }
            }
            if (tempSession != null) {
                try {
                    tempSession.close();
                } catch (ActiveMQException | RuntimeException closeException) {
                    if (closeException != e) {
                        e.addSuppressed(closeException);
                    }
                }
            }
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        boolean closeResources = pending.beginClose();
        Exception closeFailure = null;
        try {
            if (closeResources) {
                ClientSession sessionToClose;
                ClientProducer producerToClose;
                synchronized (lifecycleLock) {
                    // Wait for a send that started before close to finish using the session.
                    sessionToClose = session;
                    producerToClose = producer;
                }
                try (sessionToClose) {
                    producerToClose.close();
                }
            }
        } catch (Exception e) {
            closeFailure = e;
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        } finally {
            pending.failRemaining(closeFailure);
        }
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        CompletableFuture<Void> future;
        synchronized (lifecycleLock) {
            future = pending.register();
            if (future.isDone()) {
                return future;
            }
            try {
                ClientMessage msg = session.createMessage(true /* durable */);
                msg.setTimestamp(System.currentTimeMillis());
                msg.getBodyBuffer().writeBytes(payload);

                producer.send(
                        msg,
                        new SendAcknowledgementHandler() {
                            @Override
                            public void sendAcknowledged(Message message) {
                                pending.completeFromClient(future, null);
                            }

                            @Override
                            public void sendFailed(Message message, Exception e) {
                                Throwable failure =
                                        e != null
                                                ? e
                                                : new IllegalStateException(
                                                        "Artemis provider reported a failure without a cause");
                                pending.completeFromClient(future, failure);
                            }
                        });
            } catch (ActiveMQException | RuntimeException e) {
                future.completeExceptionally(e);
            }
        }
        return future;
    }
}
