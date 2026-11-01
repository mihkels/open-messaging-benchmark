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
package io.openmessaging.benchmark.driver.nats;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

class NatsBenchmarkProducerTest {
    private static final String TOPIC = "test-topic";

    @Test
    void publishesAnOwnedTimestampedCopyAndWaitsForAcknowledgement() {
        byte[] payload = samplePayload();
        byte[] original = payload.clone();
        AtomicReference<String> publishedTopic = new AtomicReference<>();
        AtomicReference<byte[]> publishedPayload = new AtomicReference<>();
        CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
        JetStream jetStream =
                jetStream(
                        (topic, message) -> {
                            publishedTopic.set(topic);
                            publishedPayload.set(message);
                            return acknowledgement;
                        });
        NatsBenchmarkProducer producer = new NatsBenchmarkProducer(jetStream, TOPIC);

        long before = System.currentTimeMillis();
        CompletableFuture<Void> send = producer.sendAsync(Optional.of("ignored-key"), payload);
        long after = System.currentTimeMillis();

        byte[] wire = publishedPayload.get();
        assertEquals(TOPIC, publishedTopic.get());
        assertNotSame(payload, wire);
        assertArrayEquals(original, payload);
        assertTimestampAndSuffix(wire, original, before, after);
        assertFalse(send.isDone());

        acknowledgement.complete(null);
        assertNull(send.join());
        assertArrayEquals(original, payload);
    }

    @Test
    void acceptsAnEightBytePayload() {
        byte[] payload = new byte[Long.BYTES];
        Arrays.fill(payload, (byte) 0x7f);
        byte[] original = payload.clone();
        AtomicReference<byte[]> publishedPayload = new AtomicReference<>();
        CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(
                        jetStream(
                                (topic, message) -> {
                                    publishedPayload.set(message);
                                    return acknowledgement;
                                }),
                        TOPIC);

        long before = System.currentTimeMillis();
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), payload);
        long after = System.currentTimeMillis();

        assertNotSame(payload, publishedPayload.get());
        assertArrayEquals(original, payload);
        assertTimestampAndSuffix(publishedPayload.get(), original, before, after);
        acknowledgement.complete(null);
        assertNull(send.join());
    }

    @Test
    void concurrentSendsUseDistinctOwnedBuffers() throws Exception {
        int sendCount = 16;
        byte[] payload = samplePayload();
        byte[] original = payload.clone();
        List<Publication> publications = new CopyOnWriteArrayList<>();
        CountDownLatch allPublishing = new CountDownLatch(sendCount);
        JetStream jetStream =
                jetStream(
                        (topic, message) -> {
                            CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
                            publications.add(new Publication(topic, message, acknowledgement));
                            allPublishing.countDown();
                            try {
                                assertTrue(allPublishing.await(10, TimeUnit.SECONDS), "sends did not overlap");
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError("interrupted while waiting for concurrent sends", e);
                            }
                            return acknowledgement;
                        });
        NatsBenchmarkProducer producer = new NatsBenchmarkProducer(jetStream, TOPIC);
        ExecutorService executor = Executors.newFixedThreadPool(sendCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<CompletableFuture<Void>>> calls = new ArrayList<>();
        try {
            long before = System.currentTimeMillis();
            for (int i = 0; i < sendCount; i++) {
                calls.add(
                        executor.submit(
                                () -> {
                                    assertTrue(start.await(10, TimeUnit.SECONDS));
                                    return producer.sendAsync(Optional.empty(), payload);
                                }));
            }
            start.countDown();

            List<CompletableFuture<Void>> sends = new ArrayList<>();
            for (Future<CompletableFuture<Void>> call : calls) {
                sends.add(call.get(15, TimeUnit.SECONDS));
            }
            long after = System.currentTimeMillis();
            assertEquals(sendCount, publications.size());
            assertArrayEquals(original, payload);
            Set<byte[]> identities = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Publication publication : publications) {
                assertEquals(TOPIC, publication.topic());
                assertNotSame(payload, publication.payload());
                assertTrue(identities.add(publication.payload()), "sends shared a publish buffer");
                assertTimestampAndSuffix(publication.payload(), original, before, after);
            }
            for (CompletableFuture<Void> send : sends) {
                assertFalse(send.isDone(), "send completed before its publish acknowledgement");
            }

            publications.forEach(publication -> publication.acknowledgement().complete(null));
            for (CompletableFuture<Void> send : sends) {
                assertNull(send.get(5, TimeUnit.SECONDS));
            }
            assertArrayEquals(original, payload);
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void propagatesAnExceptionalPublishAcknowledgement() {
        byte[] payload = samplePayload();
        byte[] original = payload.clone();
        CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(jetStream((topic, message) -> acknowledgement), TOPIC);

        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), payload);
        assertFalse(send.isDone());
        IOException error = new IOException("publish rejected");
        acknowledgement.completeExceptionally(error);

        CompletionException failure = assertThrows(CompletionException.class, send::join);
        assertSame(error, failure.getCause());
        assertArrayEquals(original, payload);
    }

    @Test
    void cancelledSendDoesNotRetainTheCallerBuffer() {
        byte[] payload = samplePayload();
        AtomicReference<byte[]> publishedPayload = new AtomicReference<>();
        CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(
                        jetStream(
                                (topic, message) -> {
                                    publishedPayload.set(message);
                                    return acknowledgement;
                                }),
                        TOPIC);

        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), payload);
        byte[] wireSnapshot = publishedPayload.get().clone();
        assertTrue(send.cancel(false));
        Arrays.fill(payload, (byte) 0);

        assertArrayEquals(wireSnapshot, publishedPayload.get());
        acknowledgement.complete(null);
        assertTrue(send.isCancelled());
    }

    @Test
    void rejectsEveryPayloadShorterThanEightBytesWithoutPublishing() {
        List<byte[]> published = new ArrayList<>();
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(
                        jetStream(
                                (topic, message) -> {
                                    published.add(message);
                                    return CompletableFuture.completedFuture(null);
                                }),
                        TOPIC);

        for (int length = 0; length < Long.BYTES; length++) {
            byte[] payload = new byte[length];
            Arrays.fill(payload, (byte) 0x5a);
            byte[] original = payload.clone();
            CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), payload);

            CompletionException failure = assertThrows(CompletionException.class, send::join);
            assertInstanceOf(IllegalArgumentException.class, failure.getCause());
            assertEquals("NATS payload must be at least 8 bytes", failure.getCause().getMessage());
            assertArrayEquals(original, payload);
        }
        assertTrue(published.isEmpty());
    }

    @Test
    void writesTimestampInBigEndianOrder() {
        byte[] message = new byte[10];
        message[8] = 9;
        message[9] = 10;

        NatsBenchmarkProducer.writeLongToBytes(0x0123456789abcdefL, message);

        assertArrayEquals(
                new byte[] {1, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef, 9, 10},
                message);
    }

    private static byte[] samplePayload() {
        byte[] payload = new byte[64];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 19 + 3);
        }
        return payload;
    }

    private static void assertTimestampAndSuffix(
            byte[] wire, byte[] original, long before, long after) {
        assertEquals(original.length, wire.length);
        long timestamp = ByteBuffer.wrap(wire).order(ByteOrder.BIG_ENDIAN).getLong();
        assertTrue(timestamp >= before && timestamp <= after, "unexpected timestamp: " + timestamp);
        assertArrayEquals(
                Arrays.copyOfRange(original, Long.BYTES, original.length),
                Arrays.copyOfRange(wire, Long.BYTES, wire.length));
    }

    private static JetStream jetStream(
            BiFunction<String, byte[], CompletableFuture<PublishAck>> publish) {
        return (JetStream)
                Proxy.newProxyInstance(
                        JetStream.class.getClassLoader(),
                        new Class<?>[] {JetStream.class},
                        (proxy, method, args) -> {
                            if (!method.getName().equals("publishAsync")
                                    || method.getParameterCount() != 2
                                    || method.getParameterTypes()[0] != String.class
                                    || method.getParameterTypes()[1] != byte[].class) {
                                throw new AssertionError("Unexpected JetStream call: " + method);
                            }
                            return publish.apply((String) args[0], (byte[]) args[1]);
                        });
    }

    private record Publication(
            String topic, byte[] payload, CompletableFuture<PublishAck> acknowledgement) {}
}
