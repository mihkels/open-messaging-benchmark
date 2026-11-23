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

import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BenchmarkWorkerMetricsTest {
    @Test
    void appendsDriverMetricsToSupervisorMetrics() {
        assertThat(BenchmarkWorker.metrics("supervisor 1\n", () -> "driver 2\n"))
                .isEqualTo("supervisor 1\ndriver 2\n");
    }

    @Test
    void declaresEachSharedFamilyOnce() {
        String supervisor =
                "# HELP omb_resource_cpu CPU\n# TYPE omb_resource_cpu gauge\n"
                        + "omb_resource_cpu{process=\"supervisor\"} 1.0\n";
        String driver =
                "# HELP omb_resource_cpu CPU\n# TYPE omb_resource_cpu gauge\n"
                        + "omb_resource_cpu{process=\"driver\"} 2.0\n"
                        + "# HELP messages_sent_total Sent\n# TYPE messages_sent_total counter\n"
                        + "messages_sent_total 3.0\n";

        assertThat(BenchmarkWorker.metrics(supervisor, () -> driver))
                .isEqualTo(
                        supervisor
                                + "omb_resource_cpu{process=\"driver\"} 2.0\n"
                                + "# HELP messages_sent_total Sent\n# TYPE messages_sent_total counter\n"
                                + "messages_sent_total 3.0\n");
    }

    @Test
    void servesSupervisorMetricsWhenTheDriverCannotBeScraped() {
        assertThat(
                        BenchmarkWorker.metrics(
                                "supervisor 1\n",
                                () -> {
                                    throw new IllegalStateException("Driver metrics scrape returned HTTP 500");
                                }))
                .isEqualTo("supervisor 1\n");
    }

    @Test
    void mergesMultipleSharedFamiliesContiguouslyInFirstSeenOrder() {
        String supervisor =
                """
                # HELP jvm_memory_used_bytes Used bytes
                # TYPE jvm_memory_used_bytes gauge
                jvm_memory_used_bytes{area="heap",process="supervisor"} 10.0
                # HELP omb_resource_cpu CPU
                # TYPE omb_resource_cpu gauge
                omb_resource_cpu{process="supervisor"} 1.0
                """;
        String driver =
                """
                # HELP jvm_memory_used_bytes Used bytes
                # TYPE jvm_memory_used_bytes gauge
                jvm_memory_used_bytes{area="heap",process="driver"} 20.0
                # HELP omb_resource_cpu CPU
                # TYPE omb_resource_cpu gauge
                omb_resource_cpu{process="driver"} 2.0
                # HELP messages_sent_total Sent
                # TYPE messages_sent_total counter
                messages_sent_total 3.0
                """;

        assertThat(BenchmarkWorker.metrics(supervisor, () -> driver))
                .isEqualTo(
                        """
                        # HELP jvm_memory_used_bytes Used bytes
                        # TYPE jvm_memory_used_bytes gauge
                        jvm_memory_used_bytes{area="heap",process="supervisor"} 10.0
                        jvm_memory_used_bytes{area="heap",process="driver"} 20.0
                        # HELP omb_resource_cpu CPU
                        # TYPE omb_resource_cpu gauge
                        omb_resource_cpu{process="supervisor"} 1.0
                        omb_resource_cpu{process="driver"} 2.0
                        # HELP messages_sent_total Sent
                        # TYPE messages_sent_total counter
                        messages_sent_total 3.0
                        """);
    }

    @Test
    void mergesHistogramFamiliesContiguouslyAndKeepsSingleEof() {
        String supervisor =
                """
                # HELP op_seconds Operation latency
                # TYPE op_seconds histogram
                # UNIT op_seconds seconds
                op_seconds_bucket{process="supervisor",le="0.1"} 1
                op_seconds_bucket{process="supervisor",le="+Inf"} 1
                op_seconds_count{process="supervisor"} 1
                op_seconds_sum{process="supervisor"} 0.05
                # HELP supervisor_only Only on supervisor
                # TYPE supervisor_only gauge
                supervisor_only 1.0
                # EOF
                """;
        String driver =
                """
                # HELP op_seconds Operation latency
                # TYPE op_seconds histogram
                # UNIT op_seconds seconds
                op_seconds_bucket{process="driver",le="0.1"} 2
                op_seconds_bucket{process="driver",le="+Inf"} 2
                op_seconds_count{process="driver"} 2
                op_seconds_sum{process="driver"} 0.08
                # HELP driver_only Only on driver
                # TYPE driver_only gauge
                driver_only 2.0
                # EOF
                """;

        assertThat(BenchmarkWorker.metrics(supervisor, () -> driver))
                .isEqualTo(
                        """
                        # HELP op_seconds Operation latency
                        # TYPE op_seconds histogram
                        # UNIT op_seconds seconds
                        op_seconds_bucket{process="supervisor",le="0.1"} 1
                        op_seconds_bucket{process="supervisor",le="+Inf"} 1
                        op_seconds_count{process="supervisor"} 1
                        op_seconds_sum{process="supervisor"} 0.05
                        op_seconds_bucket{process="driver",le="0.1"} 2
                        op_seconds_bucket{process="driver",le="+Inf"} 2
                        op_seconds_count{process="driver"} 2
                        op_seconds_sum{process="driver"} 0.08
                        # HELP supervisor_only Only on supervisor
                        # TYPE supervisor_only gauge
                        supervisor_only 1.0
                        # HELP driver_only Only on driver
                        # TYPE driver_only gauge
                        driver_only 2.0
                        # EOF
                        """);
    }

    @Test
    void mergesLivePrometheusRegistriesWithUniqueContiguousFamilies() {
        PrometheusMeterRegistry supervisor = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        PrometheusMeterRegistry driver = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try {
            new JvmResourceMetrics().bindTo(supervisor, "supervisor");
            new JvmResourceMetrics().bindTo(driver, "driver");
            Timer.builder("omb.publish.latency")
                    .publishPercentileHistogram()
                    .register(driver)
                    .record(Duration.ofMillis(5));
            new WorkerStats(driver).recordMessageSent();

            String merged = BenchmarkWorker.metrics(supervisor.scrape(), driver::scrape);

            assertThat(merged).contains("role=\"supervisor\"");
            assertThat(merged).contains("role=\"driver\"");
            assertThat(merged).contains("omb_publish_latency_seconds_bucket");
            assertThat(merged).contains("messages_sent_total");

            assertEveryFamilyContiguous(merged);
        } finally {
            supervisor.close();
            driver.close();
        }
    }

    private static void assertEveryFamilyContiguous(String exposition) {
        Set<String> closedFamilies = new HashSet<>();
        Set<String> seenHelp = new HashSet<>();
        Set<String> seenType = new HashSet<>();
        String currentFamily = null;
        for (String line : exposition.lines().toList()) {
            if (line.isEmpty() || line.equals("# EOF")) {
                continue;
            }
            if (line.startsWith("# HELP ") || line.startsWith("# TYPE ") || line.startsWith("# UNIT ")) {
                String[] parts = line.split(" ", 4);
                String kind = parts[1];
                String family = parts[2];
                if (kind.equals("HELP")) {
                    assertThat(seenHelp.add(family)).as("duplicate HELP for %s", family).isTrue();
                } else if (kind.equals("TYPE")) {
                    assertThat(seenType.add(family)).as("duplicate TYPE for %s", family).isTrue();
                }
                if (currentFamily != null && !currentFamily.equals(family)) {
                    assertThat(closedFamilies).doesNotContain(family);
                    closedFamilies.add(currentFamily);
                }
                currentFamily = family;
                continue;
            }
            if (line.startsWith("#")) {
                continue;
            }
            String sample = sampleName(line);
            boolean sameFamily =
                    currentFamily != null
                            && (sample.equals(currentFamily) || sample.startsWith(currentFamily + "_"));
            if (!sameFamily) {
                if (currentFamily != null) {
                    closedFamilies.add(currentFamily);
                }
                assertThat(closedFamilies).as("split family %s", sample).doesNotContain(sample);
                currentFamily = sample;
            }
        }
    }

    private static String sampleName(String line) {
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '{' || ch == ' ' || ch == '\t') {
                return line.substring(0, i);
            }
        }
        return line;
    }
}
