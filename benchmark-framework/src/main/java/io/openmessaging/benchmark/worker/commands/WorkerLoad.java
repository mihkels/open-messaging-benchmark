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
package io.openmessaging.benchmark.worker.commands;

import org.jspecify.annotations.Nullable;

/**
 * How busy a worker JVM was during one report interval.
 *
 * @param cpu process CPU time as a fraction of the interval times the available cores, 0 to 1
 * @param gc GC pause time as a fraction of the interval, 0 to 1
 */
public record WorkerLoad(double cpu, double gc) {

    /**
     * Computes the load of one interval.
     *
     * @param cpuNanos process CPU time used in the interval
     * @param gcMillis GC pause time in the interval
     * @param wallNanos length of the interval
     * @param cores available processors
     * @return the load, or null when the interval is empty or a reading is unavailable
     */
    public static @Nullable WorkerLoad of(long cpuNanos, long gcMillis, long wallNanos, int cores) {
        if (wallNanos <= 0 || cores <= 0 || cpuNanos < 0 || gcMillis < 0) {
            return null;
        }
        return new WorkerLoad(
                Math.min(1, cpuNanos / ((double) wallNanos * cores)),
                Math.min(1, gcMillis * 1e6 / wallNanos));
    }

    /** The per-field maximum, where a null side is ignored. */
    public static @Nullable WorkerLoad max(@Nullable WorkerLoad a, @Nullable WorkerLoad b) {
        if (a == null || b == null) {
            return a == null ? b : a;
        }
        return new WorkerLoad(Math.max(a.cpu, b.cpu), Math.max(a.gc, b.gc));
    }
}
