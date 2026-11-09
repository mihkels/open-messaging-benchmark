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
package io.openmessaging.benchmark.driver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/** Per-send cost of the lifecycle that synchronously publishing producers share. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class PublicationLifecycleBenchmark {
    @State(Scope.Thread)
    public abstract static class Fixture {
        PublicationLifecycle lifecycle;
        PublicationLifecycle.@Nullable Settle deferred;

        @Setup
        public void setup() {
            lifecycle = new PublicationLifecycle("benchmark");
        }

        @TearDown
        public void teardown() {
            lifecycle.close();
        }
    }

    @State(Scope.Benchmark)
    public static class Shared extends Fixture {}

    @State(Scope.Thread)
    public static class PerThread extends Fixture {}

    // A client that acknowledges before its send call returns, as synchronous clients do
    private static CompletableFuture<Void> settledInline(PublicationLifecycle lifecycle) {
        return lifecycle.publish(settle -> settle.complete(null));
    }

    @Benchmark
    public CompletableFuture<Void> threadSettledInline(PerThread state) {
        return settledInline(state.lifecycle);
    }

    @Benchmark
    @Threads(4)
    public CompletableFuture<Void> sharedSettledInline(Shared state) {
        return settledInline(state.lifecycle);
    }

    // The variant for clients that settle before returning, as Redis uses
    @Benchmark
    public CompletableFuture<Void> threadSynchronous(PerThread state) {
        return state.lifecycle.publishSync(() -> {});
    }

    @Benchmark
    @Threads(4)
    public CompletableFuture<Void> sharedSynchronous(Shared state) {
        return state.lifecycle.publishSync(() -> {});
    }

    // A client that acknowledges later from its own thread; settles the previous send first
    @Benchmark
    public CompletableFuture<Void> threadSettledLater(PerThread state) {
        PublicationLifecycle.Settle previous = state.deferred;
        if (previous != null) {
            previous.complete(null);
        }
        return state.lifecycle.publish(settle -> state.deferred = settle);
    }
}
