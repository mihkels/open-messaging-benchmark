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
package io.github.mihkels.omb.worker;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mihkels.omb.driver.RebalanceEvent;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class RunCallbacksTest {

    @Test
    void interruptedPausedReceiveReleasesItsSlotAndKeepsTheInterrupt() throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(key -> null);
        try {
            RunCallbacks callbacks =
                    new RunCallbacks(
                            new WorkerStats(registry),
                            failure -> {
                                throw new AssertionError(failure);
                            });
            callbacks.pause();

            Thread.currentThread().interrupt();
            callbacks.receive(16, System.currentTimeMillis());

            assertThat(Thread.interrupted()).isTrue();
            assertThat(callbacks.awaitQuiescence(System.nanoTime() + TimeUnit.SECONDS.toNanos(1)))
                    .isTrue();
        } finally {
            registry.close();
        }
    }

    @Test
    void sealedQuiescenceWakesWhenTheLastSendCompletes() throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(key -> null);
        try {
            RunCallbacks callbacks =
                    new RunCallbacks(
                            new WorkerStats(registry),
                            failure -> {
                                throw new AssertionError(failure);
                            });
            assertThat(callbacks.beginSend()).isTrue();
            assertThat(callbacks.seal(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))).isTrue();

            CompletableFuture<Boolean> quiesced = new CompletableFuture<>();
            Thread waiter =
                    Thread.ofPlatform()
                            .start(() -> {
                                try {
                                    quiesced.complete(
                                            callbacks.awaitQuiescence(System.nanoTime() + TimeUnit.MINUTES.toNanos(1)));
                                } catch (Throwable t) {
                                    quiesced.completeExceptionally(t);
                                }
                            });
            while (waiter.getState() != Thread.State.TIMED_WAITING) {
                Thread.onSpinWait();
            }
            callbacks.completeProbe(null);

            // A lost wakeup would leave the waiter parked until its one-minute deadline.
            assertThat(quiesced.get(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            registry.close();
        }
    }

    @Test
    void gateSealWaitsForACallbackOnAnotherThreadToLeave() throws Exception {
        RunCallbacks.Gate gate = new RunCallbacks.Gate();
        CompletableFuture<Void> inside = new CompletableFuture<>();
        CompletableFuture<Void> leave = new CompletableFuture<>();
        Thread callback =
                Thread.ofPlatform()
                        .start(() -> {
                            int slot = gate.enter();
                            inside.complete(null);
                            leave.join();
                            gate.exit(slot);
                        });
        inside.get(5, TimeUnit.SECONDS);

        assertThat(gate.awaitEmpty(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50))).isFalse();
        leave.complete(null);
        assertThat(gate.awaitEmpty(System.nanoTime() + TimeUnit.SECONDS.toNanos(5))).isTrue();
        callback.join();
    }

    @Test
    void gateIsReentrantAndEmptyOnlyOnceEveryEntryLeft() throws Exception {
        RunCallbacks.Gate gate = new RunCallbacks.Gate();
        int outer = gate.enter();
        int inner = gate.enter();

        gate.exit(inner);
        assertThat(gate.awaitEmpty(System.nanoTime())).isFalse();
        gate.exit(outer);
        assertThat(gate.awaitEmpty(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))).isTrue();
    }

    @Test
    void sealedCallbacksRecordNothing() throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(key -> null);
        try {
            WorkerStats stats = new WorkerStats(registry);
            RunCallbacks callbacks =
                    new RunCallbacks(
                            stats,
                            failure -> {
                                throw new AssertionError(failure);
                            });
            assertThat(callbacks.seal(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))).isTrue();

            assertThat(callbacks.beginSend()).isFalse();
            callbacks.receive(16, System.currentTimeMillis());

            assertThat(stats.toCountersStats().messagesReceived()).isZero();
            assertThat(callbacks.awaitQuiescence(System.nanoTime() + TimeUnit.SECONDS.toNanos(1)))
                    .isTrue();
        } finally {
            registry.close();
        }
    }

    @Test
    void callbacksRejectedAfterSealNeverEnterTheGate() throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(key -> null);
        try {
            AtomicInteger entries = new AtomicInteger();
            RunCallbacks.Gate gate =
                    new RunCallbacks.Gate() {
                        @Override
                        int enter() {
                            entries.incrementAndGet();
                            return super.enter();
                        }
                    };
            RunCallbacks callbacks =
                    new RunCallbacks(
                            new WorkerStats(registry),
                            failure -> {
                                throw new AssertionError(failure);
                            },
                            gate);
            assertThat(callbacks.beginSend()).isTrue();
            assertThat(callbacks.seal(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))).isTrue();
            entries.set(0);

            // A rejected caller that held a slot could keep a later seal waiting forever
            assertThat(callbacks.beginSend()).isFalse();
            callbacks.receive(16, System.currentTimeMillis());
            callbacks
                    .forTopic("topic")
                    .partitionsChanged(
                            new RebalanceEvent(
                                    RebalanceEvent.Kind.ASSIGNED, System.currentTimeMillis(), List.of(0)));
            callbacks.completeSend(16, 0, 0, System::nanoTime, null);

            assertThat(entries).hasValue(0);
            assertThat(callbacks.awaitQuiescence(System.nanoTime() + TimeUnit.SECONDS.toNanos(1)))
                    .isTrue();
        } finally {
            registry.close();
        }
    }
}
