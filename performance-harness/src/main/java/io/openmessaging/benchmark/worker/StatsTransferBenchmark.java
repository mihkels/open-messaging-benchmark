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
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.jackson.ObjectMappers;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class StatsTransferBenchmark {
    @State(Scope.Thread)
    public static class Transfer {
        @Param({"1", "8", "32"})
        public int aggregationSize;

        @Param({"64"})
        public int samples;

        PeriodStats period;
        CumulativeLatencies cumulative;
        PeriodStats[] periods;
        CumulativeLatencies[] cumulatives;
        ObjectMapper mapper;
        ObjectWriter writer;
        byte[] periodJson;
        byte[] cumulativeJson;

        @Setup
        public void setup() {
            PrometheusMeterRegistry registry = new PrometheusMeterRegistry(key -> null);
            try {
                WorkerStats stats = new WorkerStats(registry);
                for (int i = 0; i < samples; i++) {
                    long delay = (i + 1L) * 1000;
                    stats.recordProducerSuccess(1024, 0, delay, delay * 2);
                    stats.recordMessageReceived(1024, (i + 1L) * 100);
                }
                period = stats.toPeriodStats();
                cumulative = stats.toCumulativeLatencies();
            } finally {
                registry.close();
            }
            periods = new PeriodStats[aggregationSize];
            cumulatives = new CumulativeLatencies[aggregationSize];
            // Production plus() is non-mutating; each input can safely reuse the same snapshot.
            Arrays.fill(periods, period);
            Arrays.fill(cumulatives, cumulative);
            mapper = ObjectMappers.DEFAULT.mapper();
            writer = ObjectMappers.DEFAULT.writer();
            periodJson = writer.writeValueAsBytes(period);
            cumulativeJson = writer.writeValueAsBytes(cumulative);
            if (mapper.readValue(periodJson, PeriodStats.class).messagesSent() != samples
                    || mapper
                                    .readValue(cumulativeJson, CumulativeLatencies.class)
                                    .publishLatency
                                    .getTotalCount()
                            != samples) {
                throw new IllegalStateException("Production serialization round trip failed");
            }
        }
    }

    @Benchmark
    public PeriodStats aggregatePeriod(Transfer state) {
        return PeriodStats.aggregate(Arrays.asList(state.periods));
    }

    @Benchmark
    public CumulativeLatencies aggregateCumulative(Transfer state) {
        CumulativeLatencies result = state.cumulatives[0];
        for (int i = 1; i < state.cumulatives.length; i++) {
            result = result.plus(state.cumulatives[i]);
        }
        return result;
    }

    @Benchmark
    public byte[] serializePeriod(Transfer state) {
        return state.writer.writeValueAsBytes(state.period);
    }

    @Benchmark
    public PeriodStats deserializePeriod(Transfer state) {
        return state.mapper.readValue(state.periodJson, PeriodStats.class);
    }

    @Benchmark
    public byte[] serializeCumulative(Transfer state) {
        return state.writer.writeValueAsBytes(state.cumulative);
    }

    @Benchmark
    public CumulativeLatencies deserializeCumulative(Transfer state) {
        return state.mapper.readValue(state.cumulativeJson, CumulativeLatencies.class);
    }
}
