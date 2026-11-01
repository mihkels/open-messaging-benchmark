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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArtemisBenchmarkDriverTest {
    @Test
    void topicAndProducerFailuresCompleteExceptionally() throws Exception {
        ArtemisBenchmarkDriver driver = new ArtemisBenchmarkDriver();
        ClientSession session = mock(ClientSession.class);
        ClientSessionFactory factory = mock(ClientSessionFactory.class);
        setField(driver, "session", session);
        setField(driver, "sessionFactory", factory);
        IllegalStateException topicFailure = new IllegalStateException("address failed");
        IllegalStateException producerFailure = new IllegalStateException("session failed");
        doThrow(topicFailure)
                .when(session)
                .createAddress(any(SimpleString.class), eq(RoutingType.MULTICAST), eq(true));
        when(factory.createSession()).thenThrow(producerFailure);

        assertThat(driver.createTopic("topic", 2)).isCompletedExceptionally();
        assertThatThrownBy(() -> driver.createTopic("topic", 1).join()).hasCause(topicFailure);
        assertThatThrownBy(() -> driver.createProducer("topic").join()).hasCause(producerFailure);
    }

    @Test
    void consumerRuntimeFailureCompletesExceptionally() throws Exception {
        ArtemisBenchmarkDriver driver = new ArtemisBenchmarkDriver();
        ClientSessionFactory factory = mock(ClientSessionFactory.class);
        setField(driver, "sessionFactory", factory);
        IllegalStateException cause = new IllegalStateException("consumer session failed");
        when(factory.createSession()).thenThrow(cause);

        assertThatThrownBy(
                        () ->
                                driver.createConsumer("topic", "subscription", mock(ConsumerCallback.class)).join())
                .hasCause(cause);
    }

    @Test
    void cancelledConsumerCreationClosesTheLateHandle() throws Exception {
        ArtemisBenchmarkDriver driver = new ArtemisBenchmarkDriver();
        ClientSessionFactory factory = mock(ClientSessionFactory.class);
        ClientSession session = mock(ClientSession.class);
        ClientConsumer consumer = mock(ClientConsumer.class);
        setField(driver, "sessionFactory", factory);
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        when(factory.createSession())
                .thenAnswer(
                        invocation -> {
                            creating.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("consumer creation was not released");
                            }
                            return session;
                        });
        when(session.createConsumer("topic-subscription")).thenReturn(consumer);
        doAnswer(
                        invocation -> {
                            closed.countDown();
                            return null;
                        })
                .when(session)
                .close();

        var creation = driver.createConsumer("topic", "subscription", mock(ConsumerCallback.class));
        try {
            assertThat(creating.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(creation.cancel(false)).isTrue();
        } finally {
            release.countDown();
        }
        assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
        verify(consumer, times(1)).close();
        verify(session, times(1)).close();
        assertThat(creation).isCancelled();
    }

    @Test
    void closeAttemptsEverySharedResourceAndPreservesFailures() throws Exception {
        ArtemisBenchmarkDriver driver = new ArtemisBenchmarkDriver();
        ClientSession session = mock(ClientSession.class);
        ClientSessionFactory factory = mock(ClientSessionFactory.class);
        ServerLocator locator = mock(ServerLocator.class);
        setField(driver, "session", session);
        setField(driver, "sessionFactory", factory);
        setField(driver, "serverLocator", locator);
        ActiveMQException sessionFailure = new ActiveMQException("session close failed");
        IllegalStateException factoryFailure = new IllegalStateException("factory close failed");
        IllegalStateException locatorFailure = new IllegalStateException("locator close failed");
        doThrow(sessionFailure).when(session).close();
        doThrow(factoryFailure).when(factory).close();
        doThrow(locatorFailure).when(locator).close();

        assertThatThrownBy(driver::close)
                .isSameAs(sessionFailure)
                .satisfies(
                        error ->
                                assertThat(error.getSuppressed()).containsExactly(factoryFailure, locatorFailure));
        driver.close();
        verify(session, times(1)).close();
        verify(factory, times(1)).close();
        verify(locator, times(1)).close();
    }

    @Test
    void failedInitializationClosesPartiallyCreatedSharedResources(@TempDir Path temp)
            throws Exception {
        ArtemisBenchmarkDriver driver = new ArtemisBenchmarkDriver();
        ServerLocator locator = mock(ServerLocator.class);
        ClientSessionFactory factory = mock(ClientSessionFactory.class);
        ActiveMQException cause = new ActiveMQException("session creation failed");
        IllegalStateException closeFailure = new IllegalStateException("factory close failed");
        when(locator.createSessionFactory()).thenReturn(factory);
        when(factory.createSession()).thenThrow(cause);
        doThrow(closeFailure).when(factory).close();
        Path configuration = temp.resolve("artemis.yaml");
        Files.writeString(configuration, "brokerAddress: tcp://localhost:61616\n");

        try (var client = mockStatic(ActiveMQClient.class)) {
            client
                    .when(() -> ActiveMQClient.createServerLocator("tcp://localhost:61616"))
                    .thenReturn(locator);
            assertThatThrownBy(() -> driver.initialize(configuration, mock(MeterRegistry.class)))
                    .isInstanceOf(IOException.class)
                    .hasCause(cause);
        }
        assertThat(cause.getSuppressed()).containsExactly(closeFailure);
        verify(factory).close();
        verify(locator).close();
    }

    private static void setField(ArtemisBenchmarkDriver driver, String name, Object value)
            throws ReflectiveOperationException {
        Field field = ArtemisBenchmarkDriver.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(driver, value);
    }
}
