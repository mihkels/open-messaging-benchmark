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
package io.openmessaging.benchmark.driver.natsStreaming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.streaming.MessageHandler;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.SubscriptionOptions;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class NatsStreamingBenchmarkDriverTest {
    @Test
    void producerConnectionFailureIsAnExceptionalFuture() {
        IOException failure = new IOException("connect failed");
        NatsStreamingBenchmarkDriver driver =
                new NatsStreamingBenchmarkDriver(
                        (cluster, client, options) -> {
                            throw failure;
                        });
        CompletableFuture<BenchmarkProducer> future = driver.createProducer("topic");
        assertThat(future).isNotNull();
        assertThatThrownBy(future::join).hasCause(failure);
    }

    @Test
    void interruptedConnectionPreservesTheInterrupt() {
        InterruptedException failure = new InterruptedException("interrupted");
        NatsStreamingBenchmarkDriver driver =
                new NatsStreamingBenchmarkDriver(
                        (cluster, client, options) -> {
                            throw failure;
                        });
        try {
            assertThatThrownBy(() -> driver.createProducer("topic").join()).hasCause(failure);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void failedSubscriptionClosesConnectionAndPreservesBothFailures() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        IOException subscribeFailure = new IOException("subscribe failed");
        IOException closeFailure = new IOException("close failed");
        when(connection.subscribe(
                        anyString(), anyString(), any(MessageHandler.class), any(SubscriptionOptions.class)))
                .thenThrow(subscribeFailure);
        doThrow(closeFailure).when(connection).close();
        NatsStreamingBenchmarkDriver driver =
                new NatsStreamingBenchmarkDriver((cluster, client, options) -> connection);

        CompletableFuture<BenchmarkConsumer> future =
                driver.createConsumer("topic", "group", mock(ConsumerCallback.class));
        assertThat(future).isNotNull();
        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(subscribeFailure);
        assertThat(subscribeFailure.getSuppressed()).containsExactly(closeFailure);
        verify(connection).close();
    }

    @Test
    void producerHandlesShareTheDriverOwnedConnection() throws Exception {
        StreamingConnection connection = mock(StreamingConnection.class);
        AtomicInteger connections = new AtomicInteger();
        NatsStreamingBenchmarkDriver driver =
                new NatsStreamingBenchmarkDriver(
                        (cluster, client, options) -> {
                            connections.incrementAndGet();
                            return connection;
                        });
        BenchmarkProducer first = driver.createProducer("one").join();
        BenchmarkProducer second = driver.createProducer("two").join();
        first.close();
        second.close();
        verify(connection, never()).close();
        driver.close();
        driver.close();
        assertThat(connections).hasValue(1);
        verify(connection, times(1)).close();
        assertThatThrownBy(() -> driver.createProducer("after-close").join())
                .hasCauseInstanceOf(IllegalStateException.class);
    }
}
