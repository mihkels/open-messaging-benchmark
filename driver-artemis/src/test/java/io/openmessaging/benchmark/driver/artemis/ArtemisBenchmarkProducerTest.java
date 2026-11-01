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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.junit.jupiter.api.Test;

class ArtemisBenchmarkProducerTest {

    @Test
    void closeSettlesOutstandingSendsAndRejectsLateAcknowledgements() throws Exception {
        Fixture fixture = new Fixture();
        AtomicReference<SendAcknowledgementHandler> handler = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            handler.set(invocation.getArgument(1));
                            return null;
                        })
                .when(fixture.clientProducer)
                .send(any(Message.class), any(SendAcknowledgementHandler.class));
        ArtemisBenchmarkProducer producer = fixture.create();

        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        assertThat(send).isNotDone();
        producer.close();

        assertThat(send).isCompletedExceptionally();
        handler.get().sendAcknowledged(fixture.message);
        assertThat(send).isCompletedExceptionally();
        assertThat(producer.sendAsync(Optional.empty(), new byte[] {2})).isCompletedExceptionally();
        verify(fixture.clientProducer, times(1))
                .send(any(Message.class), any(SendAcknowledgementHandler.class));
        verify(fixture.clientProducer).close();
        verify(fixture.session).close();
    }

    @Test
    void closePreservesAcknowledgementsReceivedWhileClosing() throws Exception {
        Fixture fixture = new Fixture();
        AtomicReference<SendAcknowledgementHandler> handler = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            handler.set(invocation.getArgument(1));
                            return null;
                        })
                .when(fixture.clientProducer)
                .send(any(Message.class), any(SendAcknowledgementHandler.class));
        doAnswer(
                        invocation -> {
                            handler.get().sendAcknowledged(fixture.message);
                            return null;
                        })
                .when(fixture.clientProducer)
                .close();
        ArtemisBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        producer.close();

        assertThat(send).isCompleted();
        assertThat(send).isNotCompletedExceptionally();
    }

    @Test
    void concurrentCloseSettlesSendsWhileResourcesAreStillClosing() throws Exception {
        Fixture fixture = new Fixture();
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            closing.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("producer close was not released");
                            }
                            return null;
                        })
                .when(fixture.clientProducer)
                .close();
        ArtemisBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            var firstClose =
                    executor.submit(
                            () -> {
                                producer.close();
                                return null;
                            });
            assertThat(closing.await(5, TimeUnit.SECONDS)).isTrue();

            producer.close();

            assertThat(send).isCompletedExceptionally();
            release.countDown();
            firstClose.get(5, TimeUnit.SECONDS);
            verify(fixture.clientProducer, times(1)).close();
            verify(fixture.session, times(1)).close();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void synchronousSendFailureCompletesExceptionally() throws Exception {
        Fixture fixture = new Fixture();
        ActiveMQException cause = new ActiveMQException("send failed");
        doThrow(cause)
                .when(fixture.clientProducer)
                .send(any(Message.class), any(SendAcknowledgementHandler.class));
        ArtemisBenchmarkProducer producer = fixture.create();

        assertThatThrownBy(() -> producer.sendAsync(Optional.empty(), new byte[] {1}).join())
                .hasCause(cause);
    }

    @Test
    void nullFailureFromClientStillCompletesSendExceptionally() throws Exception {
        Fixture fixture = new Fixture();
        AtomicReference<SendAcknowledgementHandler> handler = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            handler.set(invocation.getArgument(1));
                            return null;
                        })
                .when(fixture.clientProducer)
                .send(any(Message.class), any(SendAcknowledgementHandler.class));
        ArtemisBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        handler.get().sendFailed(fixture.message, null);

        assertThatThrownBy(send::join).hasCauseInstanceOf(IllegalStateException.class);
        producer.close();
    }

    @Test
    void closeFromAcknowledgementCallbackCanBeRetriedLater() throws Exception {
        Fixture fixture = new Fixture();
        AtomicReference<SendAcknowledgementHandler> handler = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            handler.set(invocation.getArgument(1));
                            return null;
                        })
                .when(fixture.clientProducer)
                .send(any(Message.class), any(SendAcknowledgementHandler.class));
        ArtemisBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> callback =
                send.thenRun(
                        () -> assertThatThrownBy(producer::close).isInstanceOf(IllegalStateException.class));

        handler.get().sendAcknowledged(fixture.message);
        callback.join();
        verify(fixture.clientProducer, never()).close();
        producer.close();
        verify(fixture.clientProducer).close();
    }

    @Test
    void closeStillClosesSessionAndSettlesSendsWhenProducerCloseFails() throws Exception {
        Fixture fixture = new Fixture();
        ActiveMQException cause = new ActiveMQException("producer close failed");
        ActiveMQException sessionFailure = new ActiveMQException("session close failed");
        doThrow(cause).when(fixture.clientProducer).close();
        doThrow(sessionFailure).when(fixture.session).close();
        ArtemisBenchmarkProducer producer = fixture.create();
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertThatThrownBy(producer::close)
                .isSameAs(cause)
                .satisfies(error -> assertThat(error.getSuppressed()).containsExactly(sessionFailure));
        assertThatThrownBy(send::join).hasRootCause(cause);
        verify(fixture.session).close();
    }

    @Test
    void failedCreationPreservesCleanupFailures() throws Exception {
        Fixture fixture = new Fixture();
        ActiveMQException cause = new ActiveMQException("session start failed");
        ActiveMQException producerFailure = new ActiveMQException("producer close failed");
        ActiveMQException sessionFailure = new ActiveMQException("session close failed");
        doThrow(cause).when(fixture.session).start();
        doThrow(producerFailure).when(fixture.clientProducer).close();
        doThrow(sessionFailure).when(fixture.session).close();

        assertThatThrownBy(fixture::create)
                .isSameAs(cause)
                .satisfies(
                        error ->
                                assertThat(error.getSuppressed()).containsExactly(producerFailure, sessionFailure));
        verify(fixture.clientProducer).close();
        verify(fixture.session).close();
    }

    private static final class Fixture {
        private final ClientSessionFactory factory = mock(ClientSessionFactory.class);
        private final ClientSession session = mock(ClientSession.class);
        private final ClientProducer clientProducer = mock(ClientProducer.class);
        private final ClientMessage message = mock(ClientMessage.class);

        private Fixture() throws ActiveMQException {
            when(factory.createSession()).thenReturn(session);
            when(session.createProducer("topic")).thenReturn(clientProducer);
            when(session.createMessage(true)).thenReturn(message);
            ActiveMQBuffer buffer = mock(ActiveMQBuffer.class);
            when(message.getBodyBuffer()).thenReturn(buffer);
        }

        private ArtemisBenchmarkProducer create() throws ActiveMQException {
            return ArtemisBenchmarkProducer.create("topic", factory);
        }
    }
}
