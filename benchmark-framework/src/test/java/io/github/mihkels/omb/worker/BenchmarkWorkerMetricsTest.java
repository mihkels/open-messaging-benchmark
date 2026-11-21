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
}
