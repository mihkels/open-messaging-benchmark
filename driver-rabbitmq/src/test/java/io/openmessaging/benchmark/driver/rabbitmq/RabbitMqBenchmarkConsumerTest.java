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
package io.openmessaging.benchmark.driver.rabbitmq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RabbitMqBenchmarkConsumerTest {
    @Test
    void manualAckConsumerSetsPrefetchAndAcknowledgesAfterCallback() throws Exception {
        Fixture fixture = new Fixture();
        byte[] body = {1, 2, 3};
        AtomicReference<byte[]> received = new AtomicReference<>();
        AtomicReference<Long> timestamp = new AtomicReference<>();
        RabbitMqBenchmarkConsumer consumer =
                RabbitMqBenchmarkConsumer.create(
                        fixture.channel,
                        "stream",
                        callback(received, timestamp),
                        true);

        consumer.handleDelivery(
                "tag",
                new Envelope(17L, false, "exchange", "stream"),
                new BasicProperties.Builder().timestamp(new Date(1234L)).build(),
                body);

        assertEquals(RabbitMqBenchmarkConsumer.STREAM_PREFETCH, fixture.prefetch.get());
        assertEquals("stream", fixture.queue.get());
        assertEquals(false, fixture.autoAck.get());
        assertSame(consumer, fixture.consumer.get());
        assertArrayEquals(body, received.get());
        assertEquals(1234L, timestamp.get());
        assertEquals(17L, fixture.ackTag.get());
        assertEquals(1, fixture.acknowledgements.get());
    }

    @Test
    void automaticAckConsumerDoesNotSetPrefetchOrExplicitlyAcknowledge() throws Exception {
        Fixture fixture = new Fixture();
        AtomicReference<byte[]> received = new AtomicReference<>();
        AtomicReference<Long> timestamp = new AtomicReference<>();
        RabbitMqBenchmarkConsumer consumer =
                RabbitMqBenchmarkConsumer.create(fixture.channel, "queue", callback(received, timestamp), false);

        consumer.handleDelivery(
                "tag",
                new Envelope(4L, false, "", "queue"),
                new BasicProperties.Builder().timestamp(new Date(9L)).build(),
                new byte[] {9});

        assertEquals("queue", fixture.queue.get());
        assertEquals(true, fixture.autoAck.get());
        assertEquals(0, fixture.prefetch.get());
        assertEquals(0, fixture.acknowledgements.get());
        assertArrayEquals(new byte[] {9}, received.get());
        assertEquals(9L, timestamp.get());
    }

    @Test
    void callbackFailurePreventsManualAcknowledgementAndCloseFailureIsPropagated()
            throws Exception {
        Fixture fixture = new Fixture();
        IllegalStateException callbackFailure = new IllegalStateException("callback failed");
        ConsumerCallback throwingCallback =
                (ConsumerCallback)
                        Proxy.newProxyInstance(
                                ConsumerCallback.class.getClassLoader(),
                                new Class<?>[] {ConsumerCallback.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("messageReceived")) {
                                        throw callbackFailure;
                                    }
                                    return null;
                                });
        RabbitMqBenchmarkConsumer consumer =
                new RabbitMqBenchmarkConsumer(fixture.channel, throwingCallback, true);

        assertSame(
                callbackFailure,
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                consumer.handleDelivery(
                                        "tag",
                                        new Envelope(3L, false, "", "queue"),
                                        new BasicProperties.Builder()
                                                .timestamp(new Date(2L))
                                                .build(),
                                        new byte[] {1})));

        assertEquals(0, fixture.acknowledgements.get());
        IOException closeFailure = new IOException("channel close failed");
        fixture.closeFailure.set(closeFailure);
        assertSame(closeFailure, assertThrows(IOException.class, consumer::close));
        assertEquals(1, fixture.channelCloses.get());
    }

    @Test
    void closeClosesChannel() throws Exception {
        Fixture fixture = new Fixture();

        new RabbitMqBenchmarkConsumer(
                        fixture.channel,
                        callback(new AtomicReference<>(), new AtomicReference<>()),
                        false)
                .close();

        assertEquals(1, fixture.channelCloses.get());
    }

    private static ConsumerCallback callback(
            AtomicReference<byte[]> received, AtomicReference<Long> timestamp) {
        return new ConsumerCallback() {
            @Override
            public void messageReceived(byte[] payload, long publishTimestamp) {
                received.set(payload);
                timestamp.set(publishTimestamp);
            }

            @Override
            public void messageReceived(java.nio.ByteBuffer payload, long publishTimestamp) {
                throw new AssertionError("byte array callback expected");
            }
        };
    }

    private static final class Fixture {
        private final AtomicInteger prefetch = new AtomicInteger();
        private final AtomicInteger acknowledgements = new AtomicInteger();
        private final AtomicInteger channelCloses = new AtomicInteger();
        private final AtomicReference<String> queue = new AtomicReference<>();
        private final AtomicReference<Boolean> autoAck = new AtomicReference<>();
        private final AtomicReference<Long> ackTag = new AtomicReference<>();
        private final AtomicReference<com.rabbitmq.client.Consumer> consumer = new AtomicReference<>();
        private final AtomicReference<IOException> closeFailure = new AtomicReference<>();
        private final Channel channel =
                proxy(
                        Channel.class,
                        (method, args) -> {
                            switch (method) {
                                case "basicQos" -> prefetch.set((int) args[0]);
                                case "basicConsume" -> {
                                    queue.set((String) args[0]);
                                    autoAck.set((Boolean) args[1]);
                                    consumer.set((com.rabbitmq.client.Consumer) args[2]);
                                }
                                case "basicAck" -> {
                                    ackTag.set((Long) args[0]);
                                    acknowledgements.incrementAndGet();
                                }
                                case "close" -> {
                                    channelCloses.incrementAndGet();
                                    if (closeFailure.get() != null) {
                                        throw closeFailure.get();
                                    }
                                }
                                default -> {
                                    return defaultValue(method);
                                }
                            }
                            return null;
                        });
    }

    private static Object defaultValue(String method) {
        return switch (method) {
            case "isOpen" -> true;
            default -> null;
        };
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
