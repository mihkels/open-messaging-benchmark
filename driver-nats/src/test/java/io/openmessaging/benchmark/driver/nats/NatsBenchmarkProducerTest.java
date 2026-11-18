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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.lang.reflect.Proxy;
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

    private final List<Headers> headers = new CopyOnWriteArrayList<>();

    @Test
    void publishesAnOwnedUnchangedCopyWithTimestampHeaderAndWaitsForAcknowledgement() {
        byte[] payload = samplePayload();
        byte[] original = payload.clone();
        AtomicReference<String> publishedTopic = new AtomicReference<>();
        AtomicReference<byte[]> publishedPayload = new AtomicReference<>();
        CompletableFuture<PublishAck> acknowledgement = new CompletableFuture<>();
        JetStream jetStream =
                jetStream((topic, message) -> {
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
        // The payload goes out byte for byte, so a delivery stamp in its first bytes survives.
        assertArrayEquals(original, wire);
        assertTimestampHeader(headers.get(0), before, after);
        assertFalse(send.isDone());

        acknowledgement.complete(null);
        assertNull(send.join());
        assertArrayEquals(original, payload);
    }

    @Test
    void publishesPayloadsShorterThanEightBytes() {
        List<byte[]> published = new ArrayList<>();
        NatsBenchmarkProducer producer =
                new NatsBenchmarkProducer(
                        jetStream((topic, message) -> {
                            published.add(message);
                            return CompletableFuture.completedFuture(null);
                        }),
                        TOPIC);

        for (int length = 0; length < Long.BYTES; length++) {
            byte[] payload = new byte[length];
            Arrays.fill(payload, (byte) 0x5a);
            assertNull(producer.sendAsync(Optional.empty(), payload).join());
            assertArrayEquals(payload, published.get(length));
        }
    }

    @Test
    void concurrentSendsUseDistinctOwnedBuffers() throws Exception {
        int sendCount = 16;
        byte[] payload = samplePayload();
        byte[] original = payload.clone();
        List<Publication> publications = new CopyOnWriteArrayList<>();
        CountDownLatch allPublishing = new CountDownLatch(sendCount);
        JetStream jetStream =
                jetStream((topic, message) -> {
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
                        executor.submit(() -> {
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
                assertArrayEquals(original, publication.payload());
            }
            assertEquals(sendCount, headers.size());
            headers.forEach(h -> assertTimestampHeader(h, before, after));
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
                        jetStream((topic, message) -> {
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

    private static byte[] samplePayload() {
        byte[] payload = new byte[64];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 19 + 3);
        }
        return payload;
    }

    private static void assertTimestampHeader(Headers headers, long before, long after) {
        long timestamp =
                Long.parseLong(headers.getFirst(NatsBenchmarkProducer.PUBLISH_TIMESTAMP_HEADER));
        assertTrue(timestamp >= before && timestamp <= after, "unexpected timestamp: " + timestamp);
    }

    private JetStream jetStream(BiFunction<String, byte[], CompletableFuture<PublishAck>> publish) {
        return (JetStream)
                Proxy.newProxyInstance(
                        JetStream.class.getClassLoader(),
                        new Class<?>[] {JetStream.class},
                        (proxy, method, args) -> {
                            if (!method.getName().equals("publishAsync")
                                    || method.getParameterCount() != 3
                                    || method.getParameterTypes()[0] != String.class
                                    || method.getParameterTypes()[1] != Headers.class
                                    || method.getParameterTypes()[2] != byte[].class) {
                                throw new AssertionError("Unexpected JetStream call: " + method);
                            }
                            headers.add((Headers) args[1]);
                            return publish.apply((String) args[0], (byte[]) args[2]);
                        });
    }

    private record Publication(
            String topic, byte[] payload, CompletableFuture<PublishAck> acknowledgement) {}
}
