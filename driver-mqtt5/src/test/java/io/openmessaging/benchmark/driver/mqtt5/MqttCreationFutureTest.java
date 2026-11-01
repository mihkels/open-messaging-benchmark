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
package io.openmessaging.benchmark.driver.mqtt5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MqttCreationFutureTest {
    @Test
    void canceledCreationWaitsForLateConnectionAndCleanup() throws Exception {
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger closeCount = new AtomicInteger();
        MqttCreationFuture<AutoCloseable> future =
                new MqttCreationFuture<>(
                        () -> {
                            closeCount.incrementAndGet();
                            closing.countDown();
                            if (!release.await(2, TimeUnit.SECONDS)) {
                                throw new IOException("timed out waiting to release cleanup");
                            }
                        });

        assertThat(future.cancel(false)).isTrue();
        assertThat(future.quiescence()).isNotDone();
        future.connected();
        assertThat(closing.await(2, TimeUnit.SECONDS)).isTrue();
        future.finished();
        assertThat(future.quiescence()).isNotDone();
        release.countDown();

        future.quiescence().get(2, TimeUnit.SECONDS);
        assertThat(closeCount).hasValue(1);
    }

    @Test
    void canceledCreationCleansUpAfterConnectionFailure() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        MqttCreationFuture<AutoCloseable> future =
                new MqttCreationFuture<>(closeCount::incrementAndGet);

        assertThat(future.cancel(false)).isTrue();
        future.fail(new IOException("connection failed"));
        future.finished();
        future.quiescence().get(2, TimeUnit.SECONDS);

        assertThat(closeCount).hasValue(1);
    }

    @Test
    void cleanupFailureIsVisibleInQuiescenceAndOriginalFailure() throws Exception {
        IOException cleanupFailure = new IOException("cleanup failed");
        IOException creationFailure = new IOException("connection failed");
        MqttCreationFuture<AutoCloseable> future =
                new MqttCreationFuture<>(
                        () -> {
                            throw cleanupFailure;
                        });

        future.fail(creationFailure);
        future.finished();

        assertThatThrownBy(future::join).hasCause(creationFailure);
        assertThat(creationFailure.getSuppressed()).containsExactly(cleanupFailure);
        assertThatThrownBy(() -> future.quiescence().get(2, TimeUnit.SECONDS)).hasCause(cleanupFailure);
    }

    @Test
    void successfulHandoffIsNotClosedByLaterCancellation() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        AutoCloseable resource = closeCount::incrementAndGet;
        MqttCreationFuture<AutoCloseable> future = new MqttCreationFuture<>(resource);

        future.connected();
        future.succeed();
        future.finished();

        assertThat(future.get(2, TimeUnit.SECONDS)).isSameAs(resource);
        assertThat(future.cancel(false)).isFalse();
        future.quiescence().get(2, TimeUnit.SECONDS);
        assertThat(closeCount).hasValue(0);
    }

    @Test
    void disconnectBeforeHandoffFailsCreationInsteadOfReturningAStaleClient() throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        MqttCreationFuture<AutoCloseable> future =
                new MqttCreationFuture<>(closeCount::incrementAndGet);
        future.connected();

        assertThat(future.stopReconnectOnDisconnect()).isTrue();
        future.succeed();
        future.finished();

        assertThatThrownBy(future::join)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("disconnected before creation");
        future.quiescence().get(2, TimeUnit.SECONDS);
        assertThat(closeCount).hasValue(1);
    }

    @Test
    void disconnectAfterHandoffAllowsClientReconnect() throws Exception {
        MqttCreationFuture<AutoCloseable> future = new MqttCreationFuture<>(() -> {});
        future.connected();
        future.succeed();
        future.finished();
        future.get(2, TimeUnit.SECONDS);

        assertThat(future.stopReconnectOnDisconnect()).isFalse();
    }
}
