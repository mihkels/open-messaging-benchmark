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
package io.openmessaging.benchmark.metrics.otlp;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OtlpExportTest {

    @Test
    void returnsTheRegistryItselfWhenDisabled() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        assertThat(OtlpExport.withOtlp(registry, Map.of())).isSameAs(registry);
        assertThat(OtlpExport.withOtlp(registry, Map.of(OtlpExport.ENABLED_ENV, "false")))
                .isSameAs(registry);
    }

    @Test
    void addsAnOtlpRegistryBesideTheGivenOneWhenEnabled() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        // Nothing listens there; the final publish on close fails quietly
        MeterRegistry combined = OtlpExport.withOtlp(registry, Map.of(OtlpExport.ENABLED_ENV, "true"));
        try {
            assertThat(combined).isInstanceOf(CompositeMeterRegistry.class);
            assertThat(((CompositeMeterRegistry) combined).getRegistries())
                    .hasSize(2)
                    .contains(registry)
                    .anyMatch(OtlpMeterRegistry.class::isInstance);

            Counter.builder("messages_sent").register(combined).increment();
            assertThat(registry.get("messages_sent").counter().count()).isEqualTo(1.0);
        } finally {
            combined.close();
        }
        assertThat(registry.isClosed()).isTrue();
    }

    @Test
    void defaultsTheStepAndServiceName() {
        var config = OtlpExport.config(Map.of());

        assertThat(config.step()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.resourceAttributes()).containsEntry("service.name", "omb-worker");
    }

    @Test
    void leavesTheStepToTheExportIntervalVariable() {
        var config = OtlpExport.config(Map.of("OTEL_METRIC_EXPORT_INTERVAL", "5000"));

        assertThat(config.get("otlp.step")).isNull();
    }
}
