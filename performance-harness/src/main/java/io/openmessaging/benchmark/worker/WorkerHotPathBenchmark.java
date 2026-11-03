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
package io.openmessaging.benchmark.worker;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.utils.UniformRateLimiter;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/** Production accounting paths; shared cases can be compared with the uncontended thread cases. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class WorkerHotPathBenchmark {
    @State(Scope.Thread)
    public abstract static class Fixture {
        @Param({"1024"})
        public int payloadSize;

        PrometheusMeterRegistry registry;
        WorkerStats stats;
        RunCallbacks callbacks;
        UniformRateLimiter limiter;
        MessageProducer sender;
        BenchmarkProducer producer;
        byte[] payload;
        final LongSupplier clock = System::nanoTime;
        volatile Throwable callbackFailure;

        @Setup
        public void setup() {
            registry = new PrometheusMeterRegistry(key -> null);
            stats = new WorkerStats(registry);
            callbacks = new RunCallbacks(stats, failure -> callbackFailure = failure);
            limiter = new UniformRateLimiter(1_000_000);
            // Rounded interval zero: exercise production acquire/sleep checks without pacing a CPU test.
            sender = new MessageProducer(new UniformRateLimiter(1e12), callbacks);
            payload = new byte[payloadSize];
            producer =
                    new BenchmarkProducer() {
                        private final CompletableFuture<Void> complete =
                                CompletableFuture.completedFuture(null);

                        @Override
                        public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] data) {
                            return complete;
                        }

                        @Override
                        public void close() {}
                    };
        }

        @TearDown
        public void teardown() throws Exception {
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                if (!callbacks.seal(deadline)
                        || !callbacks.awaitQuiescence(deadline)
                        || callbackFailure != null
                        || stats.toCountersStats().messageSendErrors() != 0) {
                    throw new IllegalStateException("Benchmark accounting failed", callbackFailure);
                }
            } finally {
                registry.close();
            }
        }
    }

    @State(Scope.Benchmark)
    public static class Shared extends Fixture {}

    @State(Scope.Thread)
    public static class PerThread extends Fixture {}

    @State(Scope.Group)
    public static class SnapshotGroup extends Fixture {}

    private static WorkerStats record(Fixture state) {
        long now = System.nanoTime();
        state.stats.recordProducerSuccess(state.payload.length, now - 20_000, now - 10_000, now);
        state.stats.recordMessageReceived(state.payload.length, 1000);
        return state.stats;
    }

    @Benchmark
    @Threads(4)
    public WorkerStats sharedStats(Shared state) {
        return record(state);
    }

    @Benchmark
    public WorkerStats threadStats(PerThread state) {
        return record(state);
    }

    private static RunCallbacks callbackSend(Fixture state) {
        long now = System.nanoTime();
        if (!state.callbacks.beginSend()) {
            throw new IllegalStateException("Callbacks unexpectedly sealed");
        }
        state.callbacks.completeSend(state.payload.length, now - 10_000, now, state.clock, null);
        return state.callbacks;
    }

    @Benchmark
    @Threads(4)
    public RunCallbacks sharedSendCallback(Shared state) {
        return callbackSend(state);
    }

    @Benchmark
    public RunCallbacks threadSendCallback(PerThread state) {
        return callbackSend(state);
    }

    @Benchmark
    @Threads(4)
    public RunCallbacks sharedReceiveCallback(Shared state) {
        state.callbacks.messageReceived(state.payload, System.currentTimeMillis() - 1);
        return state.callbacks;
    }

    @Benchmark
    public RunCallbacks threadReceiveCallback(PerThread state) {
        state.callbacks.messageReceived(state.payload, System.currentTimeMillis() - 1);
        return state.callbacks;
    }

    @Benchmark
    @Threads(4)
    public WorkerStats sharedCompletedFutureSend(Shared state) throws InterruptedException {
        state.sender.sendMessage(state.producer, Optional.empty(), state.payload);
        return state.stats;
    }

    @Benchmark
    public WorkerStats threadCompletedFutureSend(PerThread state) throws InterruptedException {
        state.sender.sendMessage(state.producer, Optional.empty(), state.payload);
        return state.stats;
    }

    @Benchmark
    @Threads(4)
    public long sharedLimiter(Shared state) {
        return state.limiter.acquire();
    }

    @Benchmark
    public long threadLimiter(PerThread state) {
        return state.limiter.acquire();
    }

    // Keep histograms populated by real accounting while the other group thread snapshots them.
    @Benchmark
    @Group("liveSnapshot")
    @GroupThreads(3)
    public WorkerStats snapshotRecording(SnapshotGroup state) {
        return record(state);
    }

    @Benchmark
    @Group("liveSnapshot")
    @GroupThreads(1)
    public PeriodStats snapshot(SnapshotGroup state) {
        return state.stats.toPeriodStats();
    }
}
