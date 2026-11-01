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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.junit.jupiter.api.Test;

class ArtemisBenchmarkConsumerTest {

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
                .satisfies(
                        error ->
                                assertThat(error.getSuppressed()).containsExactly(consumerFailure, sessionFailure));
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
