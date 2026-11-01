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
package io.openmessaging.benchmark.driver.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class KafkaBenchmarkProducerTest {
    @Test
    @SuppressWarnings("unchecked")
    void synchronousSendFailureCompletesReturnedFutureExceptionally() {
        Producer<String, byte[]> client = mock(Producer.class);
        SerializationException cause = new SerializationException("serialization failed");
        doThrow(cause).when(client).send(any(), any());
        KafkaBenchmarkProducer producer = new KafkaBenchmarkProducer(client, "topic");

        CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), new byte[] {1});

        assertSame(cause, assertThrows(CompletionException.class, future::join).getCause());
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendCompletesOnlyOnCallbackAndPreservesCallbackFailure() {
        Producer<String, byte[]> client = mock(Producer.class);
        KafkaBenchmarkProducer producer = new KafkaBenchmarkProducer(client, "topic");
        CompletableFuture<Void> success = producer.sendAsync(Optional.empty(), new byte[] {1});
        assertFalse(success.isDone());
        ArgumentCaptor<Callback> successCallback = ArgumentCaptor.forClass(Callback.class);
        verify(client).send(any(), successCallback.capture());
        successCallback.getValue().onCompletion(null, null);
        assertTrue(success.isDone());
        success.join();

        Producer<String, byte[]> otherClient = mock(Producer.class);
        CompletableFuture<Void> failure =
                new KafkaBenchmarkProducer(otherClient, "topic")
                        .sendAsync(Optional.of("key"), new byte[] {2});
        ArgumentCaptor<Callback> failureCallback = ArgumentCaptor.forClass(Callback.class);
        verify(otherClient).send(any(), failureCallback.capture());
        RuntimeException cause = new RuntimeException("broker failure");
        failureCallback.getValue().onCompletion(null, cause);
        assertSame(cause, assertThrows(CompletionException.class, failure::join).getCause());
    }

    @Test
    @SuppressWarnings("unchecked")
    void closeFailureSettlesPendingSendsAndRejectsFutureSends() throws Exception {
        Producer<String, byte[]> client = mock(Producer.class);
        RuntimeException closeFailure = new RuntimeException("client close failed");
        doThrow(closeFailure).when(client).close();
        KafkaBenchmarkProducer producer = new KafkaBenchmarkProducer(client, "topic");
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        ArgumentCaptor<Callback> callback = ArgumentCaptor.forClass(Callback.class);
        verify(client).send(any(), callback.capture());

        assertSame(closeFailure, assertThrows(RuntimeException.class, producer::close));
        Throwable sendFailure = assertThrows(CompletionException.class, send::join).getCause();
        assertTrue(sendFailure instanceof IllegalStateException);
        assertSame(closeFailure, sendFailure.getCause());
        callback.getValue().onCompletion(null, null);
        assertSame(sendFailure, assertThrows(CompletionException.class, send::join).getCause());
        CompletableFuture<Void> afterClose = producer.sendAsync(Optional.empty(), new byte[] {2});
        assertTrue(
                assertThrows(CompletionException.class, afterClose::join).getCause()
                        instanceof IllegalStateException);
        producer.close();
        verify(client, times(1)).send(any(), any());
        verify(client, times(1)).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void acknowledgedSendStaysSuccessfulWhenClientCloseThenFails() {
        Producer<String, byte[]> client = mock(Producer.class);
        KafkaBenchmarkProducer producer = new KafkaBenchmarkProducer(client, "topic");
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), new byte[] {1});
        ArgumentCaptor<Callback> callback = ArgumentCaptor.forClass(Callback.class);
        verify(client).send(any(), callback.capture());
        RuntimeException closeFailure = new RuntimeException("client close failed");
        doAnswer(
                        ignored -> {
                            callback.getValue().onCompletion(null, null);
                            throw closeFailure;
                        })
                .when(client)
                .close();

        assertSame(closeFailure, assertThrows(RuntimeException.class, producer::close));
        send.join();
    }

    @Test
    @SuppressWarnings("unchecked")
    void closeSettlesSendStillEnteringTheClientAndIgnoresLateAcknowledgement() throws Exception {
        Producer<String, byte[]> client = mock(Producer.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Callback> callback = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            callback.set(invocation.getArgument(1));
                            entered.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting for send release");
                            }
                            return null;
                        })
                .when(client)
                .send(any(), any());
        KafkaBenchmarkProducer producer = new KafkaBenchmarkProducer(client, "topic");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<CompletableFuture<Void>> sending =
                    executor.submit(() -> producer.sendAsync(Optional.empty(), new byte[] {1}));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            producer.close();
            release.countDown();
            CompletableFuture<Void> send = sending.get(5, TimeUnit.SECONDS);
            Throwable failure = assertThrows(CompletionException.class, send::join).getCause();
            callback.get().onCompletion(null, null);
            assertSame(failure, assertThrows(CompletionException.class, send::join).getCause());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void callbackCloseIsRejectedWithoutClosingOrPoisoningTheProducer() throws Exception {
        Producer<String, byte[]> client = mock(Producer.class);
        KafkaBenchmarkProducer producer = new KafkaBenchmarkProducer(client, "topic");
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Throwable> rejected = new CompletableFuture<>();
        first.whenComplete(
                (unused, error) -> {
                    try {
                        producer.close();
                        rejected.complete(null);
                    } catch (Exception failure) {
                        rejected.complete(failure);
                    }
                });
        ArgumentCaptor<Callback> callback = ArgumentCaptor.forClass(Callback.class);
        verify(client).send(any(), callback.capture());
        callback.getValue().onCompletion(null, null);

        assertTrue(rejected.join() instanceof IllegalStateException);
        verify(client, never()).close();
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        assertFalse(second.isDone());
        producer.close();
        assertTrue(second.isCompletedExceptionally());
        verify(client, times(1)).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void failureCallbacksCannotRecursivelyCloseTheProducer() throws Exception {
        Producer<String, byte[]> client = mock(Producer.class);
        KafkaBenchmarkProducer producer = new KafkaBenchmarkProducer(client, "topic");
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), new byte[] {1});
        CompletableFuture<Void> second = producer.sendAsync(Optional.empty(), new byte[] {2});
        CompletableFuture<Throwable> rejected = new CompletableFuture<>();
        first.whenComplete(
                (unused, error) -> {
                    try {
                        producer.close();
                        rejected.complete(null);
                    } catch (Exception failure) {
                        rejected.complete(failure);
                    }
                });

        producer.close();

        assertTrue(rejected.join() instanceof IllegalStateException);
        assertTrue(first.isCompletedExceptionally());
        assertTrue(second.isCompletedExceptionally());
        verify(client, times(1)).close();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void cancellationReleasesCallerPayloadAndLateCallbackCannotResettleTheSend() throws Exception {
        Producer<String, byte[]> client = mock(Producer.class);
        KafkaBenchmarkProducer producer = new KafkaBenchmarkProducer(client, "topic");
        byte[] payload = new byte[] {1, 2};
        CompletableFuture<Void> send = producer.sendAsync(Optional.empty(), payload);
        ArgumentCaptor<ProducerRecord> record = ArgumentCaptor.forClass(ProducerRecord.class);
        ArgumentCaptor<Callback> callback = ArgumentCaptor.forClass(Callback.class);
        verify(client).send(record.capture(), callback.capture());
        byte[] retained = (byte[]) record.getValue().value();

        assertNotSame(payload, retained);
        assertTrue(send.cancel(true));
        payload[0] = 9;
        assertArrayEquals(new byte[] {1, 2}, retained);
        callback.getValue().onCompletion(null, null);
        assertTrue(send.isCancelled());
        producer.close();
    }
}
