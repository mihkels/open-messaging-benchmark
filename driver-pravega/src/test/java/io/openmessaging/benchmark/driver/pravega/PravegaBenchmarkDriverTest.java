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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.pravega.config.PravegaConfig;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ReaderGroupConfig;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PravegaBenchmarkDriverTest {
    @Test
    void creationFailuresCompleteExceptionally() throws Exception {
        Fixture fixture = new Fixture();
        IllegalStateException topicFailure = new IllegalStateException("scope failed");
        IllegalStateException producerFailure = new IllegalStateException("writer failed");
        IllegalStateException consumerFailure = new IllegalStateException("reader group failed");
        when(fixture.streams.createScope("scope")).thenThrow(topicFailure);
        when(fixture.factory.createEventWriter(eq("topic"), any(), any())).thenThrow(producerFailure);
        when(fixture.groups.createReaderGroup(eq("subscription"), any(ReaderGroupConfig.class)))
                .thenThrow(consumerFailure);

        assertThatThrownBy(() -> fixture.driver.createTopic("topic", 1).join()).hasCause(topicFailure);
        assertThatThrownBy(() -> fixture.driver.createProducer("topic").join())
                .hasCause(producerFailure);
        assertThatThrownBy(
                        () ->
                                fixture
                                        .driver
                                        .createConsumer("topic", "subscription", mock(ConsumerCallback.class))
                                        .join())
                .hasCause(consumerFailure);
        fixture.driver.close();
        verify(fixture.streams, never()).sealStream("scope", "topic");
    }

    @Test
    void sneakyInterruptionIsPropagatedAndRestoresInterruptStatus() throws Exception {
        Fixture fixture = new Fixture();
        InterruptedException cause = new InterruptedException("controller interrupted");
        when(fixture.streams.createScope("scope"))
                .thenAnswer(
                        invocation -> {
                            throw cause;
                        });
        try {
            assertThatThrownBy(() -> fixture.driver.createTopic("topic", 1).join()).hasCause(cause);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
            fixture.driver.close();
        }
    }

    @Test
    void closeAttemptsEverySharedResourceAndRemainingTopic() throws Exception {
        Fixture fixture = new Fixture();
        fixture.driver.createTopic("first", 1).join();
        fixture.driver.createTopic("second", 1).join();
        IllegalStateException factoryFailure = new IllegalStateException("factory close failed");
        IllegalStateException groupFailure = new IllegalStateException("group close failed");
        IllegalStateException topicFailure = new IllegalStateException("seal failed");
        IllegalStateException streamFailure = new IllegalStateException("manager close failed");
        doThrow(factoryFailure).when(fixture.factory).close();
        doThrow(groupFailure).when(fixture.groups).close();
        when(fixture.streams.sealStream("scope", "first")).thenThrow(topicFailure);
        doThrow(streamFailure).when(fixture.streams).close();

        assertThatThrownBy(fixture.driver::close)
                .isSameAs(factoryFailure)
                .satisfies(
                        error ->
                                assertThat(error.getSuppressed())
                                        .containsExactly(groupFailure, topicFailure, streamFailure));
        fixture.driver.close();
        verify(fixture.factory, times(1)).close();
        verify(fixture.groups, times(1)).close();
        verify(fixture.streams, times(1)).close();
        verify(fixture.streams).sealStream("scope", "second");
        verify(fixture.streams).deleteStream("scope", "second");
    }

    @Test
    void closeWaitsForInFlightTopicCreationAndCleansUpItsResult() throws Exception {
        Fixture fixture = new Fixture();
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(fixture.streams.createStream(eq("scope"), eq("late"), any()))
                .thenAnswer(
                        invocation -> {
                            creating.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("topic creation was not released");
                            }
                            return true;
                        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var creation = executor.submit(() -> fixture.driver.createTopic("late", 1).join());
            assertThat(creating.await(5, TimeUnit.SECONDS)).isTrue();
            var close =
                    executor.submit(
                            () -> {
                                fixture.driver.close();
                                return null;
                            });

            assertThatThrownBy(() -> close.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            verify(fixture.streams, never()).close();
            release.countDown();
            creation.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        verify(fixture.streams).sealStream("scope", "late");
        verify(fixture.streams).deleteStream("scope", "late");
        verify(fixture.streams).close();
        assertThat(fixture.driver.createTopic("after-close", 1)).isCompletedExceptionally();
    }

    @Test
    void interruptedCloseStillCleansUpLateTopic() throws Exception {
        Fixture fixture = new Fixture();
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        AtomicReference<Thread> closeThread = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        when(fixture.streams.createStream(eq("scope"), eq("late"), any()))
                .thenAnswer(
                        invocation -> {
                            creating.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("topic creation was not released");
                            }
                            return true;
                        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var creation = executor.submit(() -> fixture.driver.createTopic("late", 1).join());
            assertThat(creating.await(5, TimeUnit.SECONDS)).isTrue();
            var close =
                    executor.submit(
                            () -> {
                                closeThread.set(Thread.currentThread());
                                closing.countDown();
                                try {
                                    fixture.driver.close();
                                    return null;
                                } catch (Exception failure) {
                                    return failure;
                                } finally {
                                    interruptRestored.set(Thread.currentThread().isInterrupted());
                                }
                            });
            assertThat(closing.await(5, TimeUnit.SECONDS)).isTrue();
            closeThread.get().interrupt();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (closeThread.get().isInterrupted() && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            assertThat(closeThread.get().isInterrupted()).isFalse();
            release.countDown();
            creation.get(5, TimeUnit.SECONDS);

            assertThat(close.get(5, TimeUnit.SECONDS)).isInstanceOf(InterruptedException.class);
            assertThat(interruptRestored).isTrue();
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        verify(fixture.streams).sealStream("scope", "late");
        verify(fixture.streams).deleteStream("scope", "late");
        verify(fixture.streams).close();
    }

    @Test
    void failedInitializationClosesPartialClients(@TempDir Path temp) throws Exception {
        PravegaBenchmarkDriver driver = new PravegaBenchmarkDriver();
        StreamManager streams = mock(StreamManager.class);
        ReaderGroupManager groups = mock(ReaderGroupManager.class);
        IllegalStateException cause = new IllegalStateException("client factory failed");
        IllegalStateException closeFailure = new IllegalStateException("reader group close failed");
        doThrow(closeFailure).when(groups).close();
        Path configuration = temp.resolve("pravega.yaml");
        Files.writeString(
                configuration, "client:\n  controllerURI: tcp://localhost:9090\n  scopeName: scope\n");

        try (var streamManagers = mockStatic(StreamManager.class);
                var readerManagers = mockStatic(ReaderGroupManager.class);
                var factories = mockStatic(EventStreamClientFactory.class)) {
            streamManagers.when(() -> StreamManager.create(any(ClientConfig.class))).thenReturn(streams);
            readerManagers
                    .when(() -> ReaderGroupManager.withScope(eq("scope"), any(ClientConfig.class)))
                    .thenReturn(groups);
            factories
                    .when(() -> EventStreamClientFactory.withScope(eq("scope"), any(ClientConfig.class)))
                    .thenThrow(cause);

            assertThatThrownBy(() -> driver.initialize(configuration, mock(MeterRegistry.class)))
                    .isSameAs(cause);
        }
        assertThat(cause.getSuppressed()).containsExactly(closeFailure);
        verify(groups).close();
        verify(streams).close();
        driver.close();
    }

    @Test
    void initializationPreservesUndeclaredInterruption(@TempDir Path temp) throws Exception {
        PravegaBenchmarkDriver driver = new PravegaBenchmarkDriver();
        StreamManager streams = mock(StreamManager.class);
        ReaderGroupManager groups = mock(ReaderGroupManager.class);
        InterruptedException interrupted = new InterruptedException("client factory interrupted");
        Path configuration = temp.resolve("pravega.yaml");
        Files.writeString(
                configuration, "client:\n  controllerURI: tcp://localhost:9090\n  scopeName: scope\n");

        try (var streamManagers = mockStatic(StreamManager.class);
                var readerManagers = mockStatic(ReaderGroupManager.class);
                var factories = mockStatic(EventStreamClientFactory.class)) {
            streamManagers.when(() -> StreamManager.create(any(ClientConfig.class))).thenReturn(streams);
            readerManagers
                    .when(() -> ReaderGroupManager.withScope(eq("scope"), any(ClientConfig.class)))
                    .thenReturn(groups);
            factories
                    .when(() -> EventStreamClientFactory.withScope(eq("scope"), any(ClientConfig.class)))
                    .thenAnswer(
                            invocation -> {
                                throw interrupted;
                            });

            assertThatThrownBy(() -> driver.initialize(configuration, mock(MeterRegistry.class)))
                    .isSameAs(interrupted);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        verify(groups).close();
        verify(streams).close();
        driver.close();
    }

    @Test
    void interruptedTopicCleanupStillClosesTheStreamManager() throws Exception {
        Fixture fixture = new Fixture();
        fixture.driver.createTopic("topic", 1).join();
        InterruptedException interrupted = new InterruptedException("seal interrupted");
        when(fixture.streams.sealStream("scope", "topic"))
                .thenAnswer(
                        invocation -> {
                            throw interrupted;
                        });
        try {
            assertThatThrownBy(fixture.driver::close).isSameAs(interrupted);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        verify(fixture.streams).close();
    }

    private static final class Fixture {
        private final PravegaBenchmarkDriver driver = new PravegaBenchmarkDriver();
        private final StreamManager streams = mock(StreamManager.class);
        private final ReaderGroupManager groups = mock(ReaderGroupManager.class);
        private final EventStreamClientFactory factory = mock(EventStreamClientFactory.class);

        private Fixture() throws ReflectiveOperationException {
            setField(driver, "config", new PravegaConfig());
            setField(driver, "scopeName", "scope");
            setField(driver, "streamManager", streams);
            setField(driver, "readerGroupManager", groups);
            setField(driver, "clientFactory", factory);
        }
    }

    private static void setField(PravegaBenchmarkDriver driver, String name, Object value)
            throws ReflectiveOperationException {
        Field field = PravegaBenchmarkDriver.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(driver, value);
    }
}
