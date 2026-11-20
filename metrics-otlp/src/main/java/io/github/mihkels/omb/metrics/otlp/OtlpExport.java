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
package io.github.mihkels.omb.metrics.otlp;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Optional OTLP push export of worker metrics, configured with the standard {@code
 * OTEL_EXPORTER_OTLP_*}, {@code OTEL_METRIC_EXPORT_INTERVAL}, {@code OTEL_RESOURCE_ATTRIBUTES} and
 * {@code OTEL_SERVICE_NAME} environment variables.
 */
public final class OtlpExport {

    /** Environment variable that enables the export when set to {@code true}. */
    public static final String ENABLED_ENV = "OMB_METRICS_OTLP";

    // Micrometer's push default is one minute, too coarse for a benchmark run
    static final String DEFAULT_STEP = "10s";
    static final String DEFAULT_SERVICE_NAME = "omb-worker";

    private OtlpExport() {}

    /**
     * Whether the export is enabled.
     *
     * @param environment the process environment
     * @return {@code true} if {@link #ENABLED_ENV} is {@code true}
     */
    public static boolean enabled(Map<String, String> environment) {
        return Boolean.parseBoolean(environment.get(ENABLED_ENV));
    }

    /**
     * Adds OTLP export to a registry when enabled. Closing the returned registry publishes the last
     * step and closes {@code registry} too.
     *
     * @param registry the registry meters must keep reaching, such as the Prometheus one
     * @param environment the process environment
     * @return {@code registry} itself when disabled, else a composite of it and an OTLP registry
     */
    public static MeterRegistry withOtlp(MeterRegistry registry, Map<String, String> environment) {
        if (!enabled(environment)) {
            return registry;
        }
        CompositeMeterRegistry composite = new CompositeMeterRegistry();
        composite.add(registry);
        composite.add(new OtlpMeterRegistry(config(environment), Clock.SYSTEM));
        return composite;
    }

    // OtlpConfig reads the OTEL_* variables itself; environment only decides the step fallback
    static OtlpConfig config(Map<String, String> environment) {
        return new OtlpConfig() {
            @Override
            public @Nullable String get(String key) {
                return "otlp.step".equals(key) && !environment.containsKey("OTEL_METRIC_EXPORT_INTERVAL")
                        ? DEFAULT_STEP
                        : null;
            }

            @Override
            public Map<String, String> resourceAttributes() {
                Map<String, String> attributes = new HashMap<>(OtlpConfig.super.resourceAttributes());
                attributes.putIfAbsent("service.name", DEFAULT_SERVICE_NAME);
                return attributes;
            }
        };
    }
}
