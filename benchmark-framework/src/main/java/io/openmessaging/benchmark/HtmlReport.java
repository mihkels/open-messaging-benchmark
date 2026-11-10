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
package io.openmessaging.benchmark;

import io.openmessaging.benchmark.worker.jackson.ObjectMappers;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Renders benchmark results as one HTML page with interactive charts. The results are embedded as
 * JSON; the page loads the charting library from a CDN.
 */
public final class HtmlReport {
    private static final String TEMPLATE = "report.html";
    private static final String DATA_PLACEHOLDER = "__OMB_DATA__";

    private HtmlReport() {}

    /**
     * Renders the report.
     *
     * @param runs the results keyed by the label shown for each run, in display order
     * @return the HTML page
     */
    public static String render(Map<String, TestResult> runs) {
        Map<String, TestResult> ordered = new LinkedHashMap<>(runs);
        String json = ObjectMappers.DEFAULT.writer().writeValueAsString(ordered);
        // Keep result strings from closing the script element the JSON sits in
        return template().replace(DATA_PLACEHOLDER, json.replace("</", "<\\/"));
    }

    private static String template() {
        try (InputStream in =
                Objects.requireNonNull(
                        HtmlReport.class.getResourceAsStream(TEMPLATE), "missing resource " + TEMPLATE)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
