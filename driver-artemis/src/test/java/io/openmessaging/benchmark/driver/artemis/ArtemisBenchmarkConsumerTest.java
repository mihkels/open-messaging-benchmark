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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ArtemisBenchmarkConsumerTest {

    @Test
    void secondConsumerOnASubscriptionReusesTheExistingQueue() throws Exception {
        Fixture fixture = new Fixture();
        fixture.create();
        doThrow(new ActiveMQQueueExistsException("AMQ229019"))
                .when(fixture.session)
                .createQueue(any(QueueConfiguration.class));

        fixture.create();

        verify(fixture.session, times(2)).createConsumer("subscription");
        verify(fixture.session, times(2)).start();
        verify(fixture.session, never()).close();
    }

    @Test
    void closeStillClosesSessionWhenConsumerCloseFails() throws Exception {
        Fixture fixture = new Fixture();
        ActiveMQException cause = new ActiveMQException("consumer close failed");
        ActiveMQException sessionFailure = new ActiveMQException("session close failed");
        doThrow(cause).when(fixture.consumer).close();
        doThrow(sessionFailure).when(fixture.session).close();
        ArtemisBenchmarkConsumer consumer = fixture.create();

        assertThatThrownBy(consumer::close)
                .isSameAs(cause)
                .satisfies(error -> assertThat(error.getSuppressed()).containsExactly(sessionFailure));
        verify(fixture.session).close();
    }

    @Test
    void failedCreationPreservesCleanupFailures() throws Exception {
        Fixture fixture = new Fixture();
        ActiveMQException cause = new ActiveMQException("session start failed");
        ActiveMQException consumerFailure = new ActiveMQException("consumer close failed");
        ActiveMQException sessionFailure = new ActiveMQException("session close failed");
        doThrow(cause).when(fixture.session).start();
        doThrow(consumerFailure).when(fixture.consumer).close();
        doThrow(sessionFailure).when(fixture.session).close();

        assertThatThrownBy(fixture::create)
                .isSameAs(cause)
                .satisfies(error ->
                        assertThat(error.getSuppressed()).containsExactly(consumerFailure, sessionFailure));
        verify(fixture.consumer).close();
        verify(fixture.session).close();
    }

    @Test
    void failedCreationClosesOnlyTheResourcesThatExist() throws Exception {
        Fixture fixture = new Fixture();
        ActiveMQException cause = new ActiveMQException("consumer creation failed");
        when(fixture.session.createConsumer("subscription")).thenThrow(cause);
        doThrow(cause).when(fixture.session).close();

        assertThatThrownBy(fixture::create)
                .isSameAs(cause)
                .satisfies(error -> assertThat(error.getSuppressed()).isEmpty());
        verify(fixture.session).close();
        verify(fixture.consumer, never()).close();

        IllegalStateException sessionFailure = new IllegalStateException("no session");
        when(fixture.factory.createSession()).thenThrow(sessionFailure);
        assertThatThrownBy(fixture::create).isSameAs(sessionFailure);
    }

    @Test
    void failedCreationIgnoresCleanupFailureThatIsTheCause() throws Exception {
        Fixture fixture = new Fixture();
        ActiveMQException cause = new ActiveMQException("session start failed");
        doThrow(cause).when(fixture.session).start();
        doThrow(cause).when(fixture.consumer).close();

        assertThatThrownBy(fixture::create)
                .isSameAs(cause)
                .satisfies(error -> assertThat(error.getSuppressed()).isEmpty());
    }

    @Test
    void messageHandlerDeliversPayloadAndToleratesAcknowledgeFailure() throws Exception {
        Fixture fixture = new Fixture();
        ConsumerCallback callback = mock(ConsumerCallback.class);
        ArgumentCaptor<MessageHandler> handler = ArgumentCaptor.forClass(MessageHandler.class);
        ArtemisBenchmarkConsumer.create("topic", "subscription", fixture.factory, callback);
        verify(fixture.consumer).setMessageHandler(handler.capture());

        ClientMessage message = mock(ClientMessage.class);
        ActiveMQBuffer buffer = mock(ActiveMQBuffer.class);
        when(message.getBodyBuffer()).thenReturn(buffer);
        when(buffer.readableBytes()).thenReturn(3);
        when(message.getTimestamp()).thenReturn(42L);
        handler.getValue().onMessage(message);
        doThrow(new ActiveMQException("ack failed")).when(message).acknowledge();
        handler.getValue().onMessage(message);

        verify(buffer, times(2)).readBytes(any(byte[].class));
        verify(callback, times(2)).messageReceived(any(byte[].class), eq(42L));
        verify(message, times(2)).acknowledge();
    }

    @Test
    void failedCreationClosesResourcesWhoseCloseSucceeds() throws Exception {
        Fixture fixture = new Fixture();
        ActiveMQException cause = new ActiveMQException("session start failed");
        doThrow(cause).when(fixture.session).start();

        assertThatThrownBy(fixture::create)
                .isSameAs(cause)
                .satisfies(error -> assertThat(error.getSuppressed()).isEmpty());
        verify(fixture.consumer).close();
        verify(fixture.session).close();
    }

    private static final class Fixture {
        private final ClientSessionFactory factory = mock(ClientSessionFactory.class);
        private final ClientSession session = mock(ClientSession.class);
        private final ClientConsumer consumer = mock(ClientConsumer.class);

        private Fixture() throws ActiveMQException {
            when(factory.createSession()).thenReturn(session);
            when(session.createConsumer("subscription")).thenReturn(consumer);
        }

        private ArtemisBenchmarkConsumer create() throws ActiveMQException {
            return ArtemisBenchmarkConsumer.create(
                    "topic", "subscription", factory, mock(ConsumerCallback.class));
        }
    }
}
