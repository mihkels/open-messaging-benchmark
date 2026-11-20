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
