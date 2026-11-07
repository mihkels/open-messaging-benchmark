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

import com.sun.management.OperatingSystemMXBean;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.DoubleSupplier;

/** On-demand process telemetry, kept separate from message instruments and their labels. */
public final class JvmResourceMetrics {
    private final Map<String, DoubleSupplier> readings = new LinkedHashMap<>();

    public JvmResourceMetrics() {
        var operatingSystem = ManagementFactory.getOperatingSystemMXBean();
        var memory = ManagementFactory.getMemoryMXBean();
        var threads = ManagementFactory.getThreadMXBean();
        var collectors = ManagementFactory.getGarbageCollectorMXBeans();
        var buffers = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        readings.put("pid", () -> ProcessHandle.current().pid());
        readings.put(
                "cpu_seconds",
                () ->
                        operatingSystem instanceof OperatingSystemMXBean bean && bean.getProcessCpuTime() >= 0
                                ? bean.getProcessCpuTime() / 1e9
                                : Double.NaN);
        readings.put("heap_used_bytes", () -> memory.getHeapMemoryUsage().getUsed());
        readings.put("heap_committed_bytes", () -> memory.getHeapMemoryUsage().getCommitted());
        readings.put("nonheap_used_bytes", () -> memory.getNonHeapMemoryUsage().getUsed());
        readings.put(
                "direct_used_bytes",
                () ->
                        buffers.stream()
                                .filter(b -> "direct".equals(b.getName()))
                                .mapToLong(BufferPoolMXBean::getMemoryUsed)
                                .sum());
        readings.put("platform_threads", () -> threads.getThreadCount());
        readings.put(
                "gc_count",
                () ->
                        collectors.stream().anyMatch(c -> c.getCollectionCount() < 0)
                                ? Double.NaN
                                : collectors.stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum());
        readings.put(
                "gc_collection_seconds",
                () ->
                        collectors.stream().anyMatch(c -> c.getCollectionTime() < 0)
                                ? Double.NaN
                                : collectors.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).sum()
                                        / 1000.0);
    }

    public void bindTo(MeterRegistry registry, String role) {
        readings.forEach((name, reading) ->
                Gauge.builder("omb_resource_" + name, reading, DoubleSupplier::getAsDouble)
                        .tag("role", role)
                        .strongReference(true)
                        .register(registry));
    }

    public Map<String, Double> snapshot() {
        Map<String, Double> result = new LinkedHashMap<>();
        readings.forEach((name, reading) -> {
            double value = reading.getAsDouble();
            result.put(name, Double.isFinite(value) ? value : null);
        });
        return result;
    }
}
