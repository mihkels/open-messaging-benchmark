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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

class HtmlReportTest {
    private static final Pattern DATA =
            Pattern.compile(
                    "<script type=\"application/json\" id=\"omb-data\">(.*?)</script>", Pattern.DOTALL);

    @TempDir Path directory;

    private static TestResult result(String driver, double rate) {
        TestResult result = new TestResult();
        result.workload = "w";
        result.driver = driver;
        result.publishRate = List.of(rate, rate);
        result.aggregatedPublishLatencyQuantiles = new TreeMap<>(Map.of(50.0, 1.0, 99.0, 5.0));
        return result;
    }

    private static JsonNode data(String html) {
        Matcher matcher = DATA.matcher(html);
        assertThat(matcher.find()).as("data element").isTrue();
        return new ObjectMapper().readTree(matcher.group(1));
    }

    @Test
    void embedsEveryRunInOrderAsJson() {
        Map<String, TestResult> runs = new LinkedHashMap<>();
        runs.put("b.json", result("B", 2000));
        runs.put("a.json", result("A", 1000));

        String html = HtmlReport.render(runs);

        assertThat(html).contains("<!DOCTYPE html>").doesNotContain("__OMB_DATA__");
        JsonNode data = data(html);
        assertThat(data.propertyNames()).containsExactly("b.json", "a.json");
        assertThat(data.get("a.json").get("driver").stringValue()).isEqualTo("A");
        assertThat(
                        data.get("b.json").get("aggregatedPublishLatencyQuantiles").get("99.0").doubleValue())
                .isEqualTo(5.0);
    }

    @Test
    void loadsAPinnedEchartsWithIntegrity() {
        String html = HtmlReport.render(Map.of("r.json", result("A", 1)));

        assertThat(html)
                .contains("https://cdn.jsdelivr.net/npm/echarts@5.6.0/dist/echarts.min.js")
                .containsPattern("integrity=\"sha384-[A-Za-z0-9+/=]+\"")
                .contains(
                        "id=\"spectrum-publish\"", "id=\"throughput\"", "id=\"latency\"", "id=\"capacity\"");
    }

    @Test
    void resultStringsCannotCloseTheDataElement() {
        TestResult result = result("A", 1);
        result.workload = "</script><script>alert(1)</script>";

        String html = HtmlReport.render(Map.of("r.json", result));

        assertThat(html).doesNotContain("</script><script>alert(1)");
        assertThat(data(html).get("r.json").get("workload").stringValue())
                .isEqualTo("</script><script>alert(1)</script>");
    }

    @Test
    void writesNothingWithoutAReportPathOrResults() throws Exception {
        Path report = directory.resolve("report.html");
        Benchmark.writeHtmlReport(null, Map.of("r.json", result("A", 1)));
        Benchmark.writeHtmlReport("", Map.of("r.json", result("A", 1)));
        Benchmark.writeHtmlReport(report.toString(), Map.of());
        assertThat(report).doesNotExist();

        Benchmark.writeHtmlReport(report.toString(), Map.of("r.json", result("A", 1)));
        assertThat(Files.readString(report)).contains("\"r.json\"");
    }
}
