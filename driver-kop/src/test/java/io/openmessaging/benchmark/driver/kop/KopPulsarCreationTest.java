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
package io.openmessaging.benchmark.driver.kop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.kop.config.ClientType;
import io.openmessaging.benchmark.driver.kop.config.Config;
import io.openmessaging.benchmark.driver.kop.config.PulsarConfig;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.Test;

class KopPulsarCreationTest {
    @Test
    void producerIsHandedOffAndClosedByItsOwner() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<Producer<byte[]>> upstream = new CompletableFuture<>();
        Producer<byte[]> producer = producer();
        when(fixture.producerBuilder.createAsync()).thenReturn(upstream);

        CompletableFuture<BenchmarkProducer> result = fixture.driver.createProducer("topic");
        upstream.complete(producer);
        BenchmarkProducer handle = result.join();
        fixture.driver.close();
        verify(producer, never()).close();
        handle.close();
        verify(producer, times(1)).close();
        verify(fixture.producerBuilder).topic("topic");
    }

    @Test
    void producerCancellationClosesLateSuccessWithoutCancellingUpstream() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<Producer<byte[]>> upstream = new CompletableFuture<>();
        Producer<byte[]> producer = producer();
        when(fixture.producerBuilder.createAsync()).thenReturn(upstream);

        CompletableFuture<BenchmarkProducer> result = fixture.driver.createProducer("topic");
        assertTrue(result.cancel(false));
        assertFalse(upstream.isCancelled());
        upstream.complete(producer);
        verify(producer, times(1)).close();
    }

    @Test
    void synchronousProducerCreationFailureIsReturnedAsAnExceptionalFuture() throws Exception {
        Fixture fixture = new Fixture();
        RuntimeException failure = new RuntimeException("creation failed");
        when(fixture.producerBuilder.createAsync()).thenThrow(failure);

        CompletableFuture<BenchmarkProducer> result = fixture.driver.createProducer("topic");
        assertSame(failure, assertThrows(CompletionException.class, result::join).getCause());
    }

    @Test
    void consumerAggregatesAllPartitionsAndClosesEveryOwnedHandle() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<List<String>> lookup = fixture.lookup();
        CompletableFuture<Consumer<ByteBuffer>> first = new CompletableFuture<>();
        CompletableFuture<Consumer<ByteBuffer>> second = new CompletableFuture<>();
        when(fixture.consumerBuilder.subscribeAsync()).thenReturn(first, second);
        Consumer<ByteBuffer> one = consumer();
        Consumer<ByteBuffer> two = consumer();
        RuntimeException closeFailure = new RuntimeException("first close failed");
        doThrow(closeFailure).when(one).close();

        CompletableFuture<BenchmarkConsumer> result = fixture.createConsumer();
        lookup.complete(List.of("topic-partition-0", "topic-partition-1"));
        second.complete(two);
        assertFalse(result.isDone());
        first.complete(one);
        BenchmarkConsumer handle = result.join();
        verify(fixture.consumerBuilder).topic("topic-partition-0");
        verify(fixture.consumerBuilder).topic("topic-partition-1");
        verify(fixture.consumerBuilder, times(2)).subscriptionName("subscription");
        assertSame(closeFailure, assertThrows(RuntimeException.class, handle::close));
        handle.close();
        verify(one, times(1)).close();
        verify(two, times(1)).close();
    }

    @Test
    void consumerFailureClosesKnownAndLatePartitions() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<List<String>> lookup = fixture.lookup();
        CompletableFuture<Consumer<ByteBuffer>> first = new CompletableFuture<>();
        CompletableFuture<Consumer<ByteBuffer>> second = new CompletableFuture<>();
        CompletableFuture<Consumer<ByteBuffer>> third = new CompletableFuture<>();
        when(fixture.consumerBuilder.subscribeAsync()).thenReturn(first, second, third);
        Consumer<ByteBuffer> known = consumer();
        Consumer<ByteBuffer> late = consumer();
        RuntimeException failure = new RuntimeException("subscribe failed");
        RuntimeException closeFailure = new RuntimeException("cleanup failed");
        doThrow(closeFailure).when(known).close();

        CompletableFuture<BenchmarkConsumer> result = fixture.createConsumer();
        lookup.complete(List.of("p0", "p1", "p2"));
        first.complete(known);
        second.completeExceptionally(failure);
        assertSame(failure, assertThrows(CompletionException.class, result::join).getCause());
        assertEquals(List.of(closeFailure), List.of(failure.getSuppressed()));
        verify(known, times(1)).close();
        assertFalse(third.isCancelled());
        third.complete(late);
        verify(late, times(1)).close();
    }

    @Test
    void consumerCancellationBeforeLookupDoesNotSubscribe() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<List<String>> lookup = fixture.lookup();
        CompletableFuture<BenchmarkConsumer> result = fixture.createConsumer();
        assertTrue(result.cancel(false));
        assertFalse(lookup.isCancelled());
        lookup.complete(List.of("p0"));
        verify(fixture.consumerBuilder, never()).subscribeAsync();
    }

    @Test
    void consumerCancellationClosesKnownAndLatePartitions() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<List<String>> lookup = fixture.lookup();
        CompletableFuture<Consumer<ByteBuffer>> first = new CompletableFuture<>();
        CompletableFuture<Consumer<ByteBuffer>> second = new CompletableFuture<>();
        when(fixture.consumerBuilder.subscribeAsync()).thenReturn(first, second);
        Consumer<ByteBuffer> known = consumer();
        Consumer<ByteBuffer> late = consumer();

        CompletableFuture<BenchmarkConsumer> result = fixture.createConsumer();
        lookup.complete(List.of("p0", "p1"));
        first.complete(known);
        assertTrue(result.cancel(false));
        verify(known, times(1)).close();
        assertFalse(second.isCancelled());
        second.complete(late);
        verify(late, times(1)).close();
    }

    @Test
    void cancellationCleansPartialsWhileASecondSubscriptionIsStarting() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<List<String>> lookup = fixture.lookup();
        Consumer<ByteBuffer> known = consumer();
        Consumer<ByteBuffer> late = consumer();
        CompletableFuture<Consumer<ByteBuffer>> lateFuture = new CompletableFuture<>();
        CountDownLatch starting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger started = new AtomicInteger();
        when(fixture.consumerBuilder.subscribeAsync())
                .thenAnswer(
                        invocation -> {
                            if (started.getAndIncrement() == 0) {
                                return CompletableFuture.completedFuture(known);
                            }
                            starting.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting to release subscription");
                            }
                            return lateFuture;
                        });
        CompletableFuture<BenchmarkConsumer> result = fixture.createConsumer();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> completingLookup =
                    executor.submit(() -> lookup.complete(List.of("p0", "p1", "p2")));
            assertTrue(starting.await(5, TimeUnit.SECONDS));
            assertTrue(result.cancel(false));
            verify(known, times(1)).close();
            release.countDown();
            completingLookup.get(5, TimeUnit.SECONDS);
            lateFuture.complete(late);
            verify(late, times(1)).close();
            assertEquals(2, started.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void lookupFailureIsPreservedWithoutStartingSubscriptions() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<List<String>> lookup = fixture.lookup();
        RuntimeException failure = new RuntimeException("lookup failed");
        CompletableFuture<BenchmarkConsumer> result = fixture.createConsumer();
        lookup.completeExceptionally(failure);
        assertSame(failure, assertThrows(CompletionException.class, result::join).getCause());
        verify(fixture.consumerBuilder, never()).subscribeAsync();
    }

    @SuppressWarnings("unchecked")
    private static Producer<byte[]> producer() {
        return mock(Producer.class);
    }

    @SuppressWarnings("unchecked")
    private static Consumer<ByteBuffer> consumer() {
        return mock(Consumer.class);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class Fixture {
        private final KopBenchmarkDriver driver = new KopBenchmarkDriver();
        private final PulsarClient client = mock(PulsarClient.class);
        private final ProducerBuilder<byte[]> producerBuilder;
        private final ConsumerBuilder<ByteBuffer> consumerBuilder;

        @SuppressWarnings("unchecked")
        private Fixture() throws Exception {
            producerBuilder = mock(ProducerBuilder.class, RETURNS_SELF);
            consumerBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);
            when(producerBuilder.clone()).thenReturn(producerBuilder);
            when(consumerBuilder.clone()).thenReturn(consumerBuilder);
            setField(
                    driver,
                    "config",
                    new Config(ClientType.PULSAR, ClientType.PULSAR, new PulsarConfig(), 100L, ""));
            setField(driver, "client", client);
            setField(driver, "producerBuilder", producerBuilder);
            setField(driver, "consumerBuilder", consumerBuilder);
        }

        private CompletableFuture<List<String>> lookup() {
            CompletableFuture<List<String>> result = new CompletableFuture<>();
            when(client.getPartitionsForTopic("topic", true)).thenReturn(result);
            return result;
        }

        private CompletableFuture<BenchmarkConsumer> createConsumer() {
            return driver.createConsumer("topic", "subscription", mock(ConsumerCallback.class));
        }
    }
}
