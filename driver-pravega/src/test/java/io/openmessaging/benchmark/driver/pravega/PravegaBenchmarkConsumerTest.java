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
package io.openmessaging.benchmark.driver.pravega;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PravegaBenchmarkConsumerTest {
    @Test
    void timeoutIsReportedAndReaderIsStillClosed() throws Exception {
        EventStreamReader<ByteBuffer> reader = mock();
        ExecutorService executor = mock();
        when(executor.awaitTermination(1, TimeUnit.MINUTES)).thenReturn(false);
        PravegaBenchmarkConsumer consumer = new PravegaBenchmarkConsumer(reader, executor);

        assertThatThrownBy(consumer::close).isInstanceOf(TimeoutException.class);
        assertThatThrownBy(consumer::close).isInstanceOf(TimeoutException.class);

        verify(executor).shutdown();
        verify(executor).shutdownNow();
        verify(reader, times(1)).close();
    }

    @Test
    void interruptionStillClosesReaderAndPreservesCleanupFailure() throws Exception {
        EventStreamReader<ByteBuffer> reader = mock();
        ExecutorService executor = mock();
        InterruptedException interrupted = new InterruptedException("interrupted");
        IllegalStateException closeFailure = new IllegalStateException("reader close failed");
        when(executor.awaitTermination(1, TimeUnit.MINUTES)).thenThrow(interrupted);
        doThrow(closeFailure).when(reader).close();
        PravegaBenchmarkConsumer consumer = new PravegaBenchmarkConsumer(reader, executor);

        try {
            assertThatThrownBy(consumer::close).isSameAs(interrupted);
            assertThat(interrupted.getSuppressed()).containsExactly(closeFailure);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        verify(executor).shutdownNow();
        verify(reader).close();
    }

    @Test
    void undeclaredInterruptedReaderCloseRestoresInterruptStatus() throws Exception {
        EventStreamReader<ByteBuffer> reader = mock();
        ExecutorService executor = mock();
        when(executor.awaitTermination(1, TimeUnit.MINUTES)).thenReturn(true);
        InterruptedException interrupted = new InterruptedException("reader interrupted");
        doAnswer(
                        invocation -> {
                            throw interrupted;
                        })
                .when(reader)
                .close();
        PravegaBenchmarkConsumer consumer = new PravegaBenchmarkConsumer(reader, executor);

        try {
            assertThatThrownBy(consumer::close).isSameAs(interrupted);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void closeFromReaderCallbackCanBeRetriedLater() throws Exception {
        EventStreamClientFactory factory = mock();
        ReaderGroupManager groups = mock();
        EventStreamReader<ByteBuffer> reader = mock();
        EventRead<ByteBuffer> message = mock();
        EventRead<ByteBuffer> empty = mock();
        ConsumerCallback callback = mock();
        when(factory.<ByteBuffer>createReader(anyString(), eq("subscription"), any(), any()))
                .thenReturn(reader);
        when(reader.readNextEvent(anyLong())).thenReturn(message, empty);
        when(message.getEvent()).thenReturn(ByteBuffer.allocate(Long.BYTES));
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch delivered = new CountDownLatch(1);
        AtomicReference<PravegaBenchmarkConsumer> current = new AtomicReference<>();
        AtomicReference<Exception> rejected = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            if (!ready.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("consumer was not ready");
                            }
                            try {
                                current.get().close();
                            } catch (Exception failure) {
                                rejected.set(failure);
                            } finally {
                                delivered.countDown();
                            }
                            return null;
                        })
                .when(callback)
                .messageReceived(any(ByteBuffer.class), anyLong());
        PravegaBenchmarkConsumer consumer =
                PravegaBenchmarkConsumer.create(
                        "stream", "scope", "subscription", callback, factory, groups, true);
        current.set(consumer);
        ready.countDown();
        try {
            assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(rejected.get()).isInstanceOf(IllegalStateException.class);
        } finally {
            consumer.close();
        }
        verify(reader).close();
    }
}
