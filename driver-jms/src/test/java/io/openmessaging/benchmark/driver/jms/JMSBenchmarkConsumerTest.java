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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import org.junit.jupiter.api.Test;

class JMSBenchmarkConsumerTest {
    @Test
    void getBodyListenerForwardsPayloadAcknowledgesAndStartsConnection() throws Exception {
        Fixture fixture = new Fixture();
        AtomicReference<byte[]> received = new AtomicReference<>();
        AtomicReference<Long> timestamp = new AtomicReference<>();
        JMSBenchmarkConsumer.create(
                fixture.connection,
                fixture.session,
                fixture.consumer,
                callback(received, timestamp),
                true);
        byte[] body = {4, 5, 6};
        fixture.message = proxy(Message.class, (method, args) -> switch (method) {
            case "getBody" -> body;
            case "getLongProperty" -> 123L;
            case "acknowledge" -> {
                fixture.acknowledgements.incrementAndGet();
                yield null;
            }
            default -> null;
        });

        fixture.listener.get().onMessage(fixture.message);

        assertArrayEquals(body, received.get());
        assertEquals(123L, timestamp.get());
        assertEquals(1, fixture.acknowledgements.get());
        assertEquals(1, fixture.connectionStarts.get());
    }

    @Test
    void bytesMessageListenerReadsBodyAndDoesNotAcknowledgeWhenCallbackFails() throws Exception {
        Fixture fixture = new Fixture();
        IllegalStateException callbackFailure = new IllegalStateException("callback failed");
        AtomicInteger callbackInvocations = new AtomicInteger();
        ConsumerCallback callback =
                new ConsumerCallback() {
                    @Override
                    public void messageReceived(byte[] payload, long publishTimestamp) {
                        callbackInvocations.incrementAndGet();
                        assertArrayEquals(new byte[] {8, 9}, payload);
                        assertEquals(77L, publishTimestamp);
                        throw callbackFailure;
                    }

                    @Override
                    public void messageReceived(ByteBuffer payload, long publishTimestamp) {
                        throw new AssertionError("byte array callback expected");
                    }
                };
        JMSBenchmarkConsumer.create(
                fixture.connection, fixture.session, fixture.consumer, callback, false);
        fixture.message =
                proxy(
                        BytesMessage.class,
                        (method, args) ->
                                switch (method) {
                                    case "getBodyLength" -> 2L;
                                    case "getLongProperty" -> 77L;
                                    case "readBytes" -> {
                                        byte[] target = (byte[]) args[0];
                                        target[0] = 8;
                                        target[1] = 9;
                                        yield 2;
                                    }
                                    case "acknowledge" -> {
                                        fixture.acknowledgements.incrementAndGet();
                                        yield null;
                                    }
                                    default -> null;
                                });

        fixture.listener.get().onMessage(fixture.message);

        assertEquals(0, fixture.acknowledgements.get());
        assertEquals(1, callbackInvocations.get());
    }

    @Test
    void closeClosesConnectionWithoutIndividuallyClosingSessionOrConsumer() throws Exception {
        Fixture fixture = new Fixture();
        JMSBenchmarkConsumer benchmarkConsumer =
                JMSBenchmarkConsumer.create(
                        fixture.connection, fixture.session, fixture.consumer, callback(null, null), true);

        benchmarkConsumer.close();

        assertEquals(1, fixture.connectionCloses.get());
        assertEquals(0, fixture.sessionCloses.get());
        assertEquals(0, fixture.consumerCloses.get());
    }

    private static ConsumerCallback callback(
            AtomicReference<byte[]> received, AtomicReference<Long> timestamp) {
        return new ConsumerCallback() {
            @Override
            public void messageReceived(byte[] payload, long publishTimestamp) {
                if (received != null) {
                    received.set(payload);
                    timestamp.set(publishTimestamp);
                }
            }

            @Override
            public void messageReceived(ByteBuffer payload, long publishTimestamp) {
                throw new AssertionError("byte array callback expected");
            }
        };
    }

    private static final class Fixture {
        private final AtomicReference<MessageListener> listener = new AtomicReference<>();
        private final AtomicInteger connectionStarts = new AtomicInteger();
        private final AtomicInteger connectionCloses = new AtomicInteger();
        private final AtomicInteger sessionCloses = new AtomicInteger();
        private final AtomicInteger consumerCloses = new AtomicInteger();
        private final AtomicInteger acknowledgements = new AtomicInteger();
        private final Connection connection =
                proxy(
                        Connection.class,
                        (method, args) -> {
                            if (method.equals("start")) {
                                connectionStarts.incrementAndGet();
                            } else if (method.equals("close")) {
                                connectionCloses.incrementAndGet();
                            }
                            return null;
                        });
        private final Session session =
                proxy(
                        Session.class,
                        (method, args) -> {
                            if (method.equals("close")) {
                                sessionCloses.incrementAndGet();
                            }
                            return null;
                        });
        private final MessageConsumer consumer =
                proxy(
                        MessageConsumer.class,
                        (method, args) -> {
                            if (method.equals("setMessageListener")) {
                                listener.set((MessageListener) args[0]);
                            } else if (method.equals("close")) {
                                consumerCloses.incrementAndGet();
                            }
                            return null;
                        });
        private Message message;
    }

    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return type.cast(
                Proxy.newProxyInstance(
                        type.getClassLoader(),
                        new Class<?>[] {type},
                        (instance, method, args) -> invocation.invoke(method.getName(), args)));
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(String method, Object[] args) throws Throwable;
    }
}
