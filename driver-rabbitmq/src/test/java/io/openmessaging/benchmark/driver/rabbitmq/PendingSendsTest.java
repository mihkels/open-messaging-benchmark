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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.ShutdownSignalException;
import io.openmessaging.benchmark.driver.rabbitmq.PendingSends.PendingSend;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class PendingSendsTest {
    @Test
    void reentrantPublishIsRejectedAndClosedGateReportsCause() throws Exception {
        PendingSends sends = new PendingSends();
        assertThat(sends.beginPublish().rejection()).isNull();
        assertThat(sends.beginPublish().rejection())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Reentrant RabbitMQ publish");
        assertThat(sends.endPublish()).isEmpty();

        assertThatThrownBy(() -> sendsWhilePublishing().close()).hasMessageContaining("reentrantly");

        sends.close();
        assertThat(sends.beginPublish().rejection()).hasMessage("RabbitMQ producer is closed");
        assertThat(sends.register(send(0, 1))).hasMessage("RabbitMQ producer is closed");
    }

    @Test
    void registerRejectsStaleGenerationsAndDuplicateSequences() {
        PendingSends sends = initialized();
        assertThat(sends.register(send(5, 1))).hasMessage("RabbitMQ channel changed during publish");

        PendingSend first = send(0, 1);
        assertThat(sends.register(first)).isNull();
        assertThat(sends.register(send(0, 1)))
                .hasMessageContaining("Duplicate RabbitMQ publish sequence: 1");
        first.future.complete(null);
        assertThat(sends.register(send(0, 1))).isNull();

        PendingSends.RecoveryStart recovery = sends.beginRecovery();
        assertThat(recovery.generation()).isEqualTo(1);
        assertThat(recovery.sends()).hasSize(1);
        assertThat(sends.register(send(0, 2))).hasMessage("RabbitMQ channel is recovering");
    }

    @Test
    void singleConfirmsSkipCompletedAndUnknownTags() {
        PendingSends sends = initialized();
        PendingSend done = send(0, 1);
        PendingSend open = send(0, 2);
        sends.register(done);
        sends.register(open);
        done.future.complete(null);

        assertThat(sends.confirm(0, 1, false, false)).isEmpty();
        assertThat(sends.confirm(0, 9, false, false)).isEmpty();
        assertThat(sends.confirm(7, 2, false, false)).isEmpty();
        assertThat(sends.confirm(0, 2, false, true)).containsExactly(open);
        assertThat(open.failure).hasMessage("Message was negatively acknowledged");
        assertThat(sends.confirm(0, 2, false, false)).isEmpty();
    }

    @Test
    void multipleConfirmsDropCompletedAndAlreadyDecidedSends() {
        PendingSends sends = initialized();
        PendingSend done = send(0, 1);
        PendingSend decided = send(0, 2);
        PendingSend open = send(0, 3);
        sends.register(done);
        sends.register(decided);
        sends.register(open);
        done.future.complete(null);
        decided.decided = true;

        assertThat(sends.confirm(0, 3, true, false)).containsExactly(open);
        assertThat(open.failure).isNull();
    }

    @Test
    void failIgnoresUnknownDecidedOrCompletedSendsAndDefersWhilePublishing() throws Exception {
        PendingSends sends = initialized();
        RuntimeException cause = new RuntimeException("io");
        assertThat(sends.fail(send(0, 1), cause)).isEmpty();

        PendingSend decided = send(0, 2);
        sends.register(decided);
        decided.decided = true;
        assertThat(sends.fail(decided, cause)).isEmpty();

        PendingSend done = send(0, 3);
        sends.register(done);
        done.future.complete(null);
        assertThat(sends.fail(done, cause)).isEmpty();

        PendingSend open = send(0, 4);
        sends.register(open);
        sends.beginPublish();
        assertThat(sends.fail(open, cause)).isEmpty();
        assertThat(sends.endPublish()).containsExactly(open);
        assertThat(open.failure).isSameAs(cause);
    }

    @Test
    void shutdownUsesSignalOrDefaultCauseOnlyForTheCurrentGeneration() {
        PendingSends sends = initialized();
        PendingSend open = send(0, 1);
        sends.register(open);
        assertThat(sends.shutdown(3, null)).isEmpty();
        assertThat(sends.shutdown(0, null)).containsExactly(open);
        assertThat(open.failure).hasMessage("RabbitMQ channel shut down");

        PendingSends signalled = initialized();
        ShutdownSignalException signal = new ShutdownSignalException(false, false, null, null);
        signalled.shutdown(0, signal);
        assertThat(signalled.register(send(0, 1))).isSameAs(signal);
        signalled.shutdown(0, null);
        assertThat(signalled.register(send(0, 1))).isSameAs(signal);

        PendingSends closing = initialized();
        closing.close();
        closing.shutdown(0, signal);
        assertThat(closing.register(send(0, 1))).hasMessage("RabbitMQ producer is closed");
    }

    @Test
    void recoveryRequestedBeforeInitializationRunsAfterIt() {
        PendingSends sends = new PendingSends();
        assertThat(sends.beginRecovery()).isNull();
        assertThat(sends.recoveryFinished()).isFalse();
        assertThat(sends.finishInitialization()).isTrue();
        assertThat(sends.finishInitialization()).isFalse();
        assertThat(sends.recoveryFinished()).isTrue();
    }

    @Test
    void clientRecoveryInProgressBlocksAndRequeuesDriverRecovery() {
        PendingSends sends = initialized();
        PendingSend open = send(0, 1);
        sends.register(open);
        assertThat(sends.recoveryStarted()).containsExactly(open);
        assertThat(open.failure).hasMessage("RabbitMQ channel is recovering");
        assertThat(sends.beginRecovery()).isNull();
        assertThat(sends.finishInitialization()).isFalse();
        assertThat(sends.recoveryFinished()).isTrue();

        PendingSends.RecoveryStart start = sends.beginRecovery();
        assertThat(start).isNotNull();
        assertThat(sends.beginRecovery()).isNull();
        assertThat(sends.recoveryFinished()).isFalse();
        assertThat(sends.endRecovery(start.generation(), true)).isTrue();
        assertThat(sends.register(send(start.generation(), 1))).isNotNull();
    }

    @Test
    void installedRecoveryReopensThePublishGate() throws Exception {
        PendingSends sends = initialized();
        PendingSends.RecoveryStart start = sends.beginRecovery();
        assertThat(sends.endRecovery(start.generation() + 1, true)).isFalse();
        assertThat(sends.endRecovery(start.generation(), true)).isFalse();
        assertThat(sends.beginPublish().rejection()).isNull();
        assertThat(sends.register(send(start.generation(), 1))).isNull();
        sends.endPublish();

        PendingSends notInstalled = initialized();
        PendingSends.RecoveryStart failed = notInstalled.beginRecovery();
        assertThat(notInstalled.endRecovery(failed.generation(), false)).isFalse();
        assertThat(notInstalled.beginPublish().rejection())
                .hasMessage("RabbitMQ channel is recovering");
    }

    @Test
    void closeStopsRecoveryAndIsIdempotent() {
        PendingSends sends = initialized();
        PendingSends.RecoveryStart start = sends.beginRecovery();
        assertThat(sends.beginRecovery()).isNull();
        PendingSends.CloseResult first = sends.close();
        assertThat(first.closeChannel()).isTrue();
        assertThat(sends.close().closeChannel()).isFalse();
        assertThat(sends.isClosed()).isTrue();
        assertThat(sends.endRecovery(start.generation(), true)).isFalse();
        assertThat(sends.recoveryStarted()).isEmpty();
        assertThat(sends.recoveryFinished()).isFalse();

        PendingSends idle = initialized();
        idle.close();
        assertThat(idle.beginRecovery()).isNull();
    }

    @Test
    void stopDropsCompletedSendsAndKeepsExistingDecisions() {
        PendingSends sends = initialized();
        PendingSend done = send(0, 1);
        PendingSend nacked = send(0, 2);
        sends.register(done);
        sends.register(nacked);
        done.future.complete(null);
        sends.confirm(0, 2, false, true);

        assertThat(sends.close().sends()).containsExactly(nacked);
        assertThat(nacked.failure).hasMessage("Message was negatively acknowledged");
        sends.remove(nacked);
    }

    private static PendingSends initialized() {
        PendingSends sends = new PendingSends();
        sends.finishInitialization();
        return sends;
    }

    private static PendingSends sendsWhilePublishing() throws InterruptedException {
        PendingSends sends = new PendingSends();
        sends.beginPublish();
        return sends;
    }

    private static PendingSend send(long generation, long sequence) {
        return new PendingSend(generation, sequence, new CompletableFuture<>());
    }
}
