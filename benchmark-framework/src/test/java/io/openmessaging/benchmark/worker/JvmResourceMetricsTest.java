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

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;

class JvmResourceMetricsTest {
    @Test
    void reportsResourceUnitsWithoutChangingMessageMetrics() {
        var readings = new JvmResourceMetrics();
        var snapshot = readings.snapshot();
        assertThat(snapshot.get("pid")).isEqualTo((double) ProcessHandle.current().pid());
        assertThat(snapshot.get("heap_used_bytes")).isPositive();
        assertThat(snapshot.get("heap_committed_bytes"))
                .isGreaterThanOrEqualTo(snapshot.get("heap_used_bytes"));
        assertThat(snapshot.get("platform_threads")).isPositive();
        if (snapshot.get("cpu_seconds") != null) {
            assertThat(readings.snapshot().get("cpu_seconds"))
                    .isGreaterThanOrEqualTo(snapshot.get("cpu_seconds"));
        }
        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try {
            readings.bindTo(registry, "driver");
            readings.bindTo(registry, "supervisor");
            assertThat(registry.scrape())
                    .contains("role=\"driver\"", "role=\"supervisor\"", "omb_resource_heap_used_bytes")
                    .doesNotContain("messages_sent");
        } finally {
            registry.close();
        }
    }
}
