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
package io.github.mihkels.omb.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class ScenarioMainMetricLineTest {
    /** The patterns before java:S8786 was fixed, used with {@code matches()}. */
    private static final Pattern FORMER_RESOURCE =
            Pattern.compile("omb_resource_(\\w+)\\{[^}]*role=\"([^\"]+)\"[^}]*}\\s+(\\S+).*");

    private static final Pattern FORMER_RECEIVE_ERRORS =
            Pattern.compile("synthetic_receive_errors_total(?:\\{[^}]*})?\\s+(\\S+).*");

    private static final List<String> LINES =
            List.of(
                    "omb_resource_heap_used_bytes{role=\"driver\"} 1.5E7",
                    "omb_resource_heap_used_bytes{area=\"heap\",role=\"supervisor\"} 42.0",
                    "omb_resource_cpu_seconds{role=\"driver\",area=\"x\"} 3",
                    "omb_resource_cpu_seconds{a=\"1\",role=\"driver\",b=\"2\"}   7 1700000000000",
                    "omb_resource_threads{role=\"first\",role=\"second\"} 9",
                    "omb_resource_threads{myrole=\"x\"} 1",
                    "omb_resource_threads{area=\"heap\"} 1",
                    "omb_resource_threads{role=\"\"} 1",
                    "omb_resource_threads{role=\"driver\"}1",
                    "omb_resource_threads 1",
                    "# HELP omb_resource_threads live threads",
                    "omb_resource_{role=\"driver\"} 1",
                    "omb_resource_threads{role=\"driver\"} NaN",
                    "omb_resource_threads{role=\"driver\"} Infinity",
                    "synthetic_receive_errors_total 4.0",
                    "synthetic_receive_errors_total{driver=\"synthetic\"} 5",
                    "synthetic_receive_errors_total{} 6 1700000000000",
                    "synthetic_receive_errors_total{a=\"1\"}7",
                    "synthetic_receive_errors_total_created 1",
                    "synthetic_receive_errors_total{a=\"1\" 7",
                    "x_synthetic_receive_errors_total 1",
                    "",
                    "   ");

    @Test
    void parsesResourceGaugeWithRoleInAnyLabelPosition() {
        assertResource(
                "heap_used_bytes", "driver", 1.5e7, "omb_resource_heap_used_bytes{role=\"driver\"} 1.5E7");
        assertResource("heap_used_bytes", "supervisor", 42.0, LINES.get(1));
        assertResource("cpu_seconds", "driver", 3, LINES.get(2));
        assertResource("cpu_seconds", "driver", 7, LINES.get(3));
        assertResource("threads", "second", 9, LINES.get(4));
    }

    @Test
    void ignoresLinesThatAreNotRoleLabelledResourceGauges() {
        assertTrue(ScenarioMain.parseResource("omb_resource_threads{area=\"heap\"} 1").isEmpty());
        assertTrue(ScenarioMain.parseResource("omb_resource_threads 1").isEmpty());
        assertTrue(ScenarioMain.parseResource("omb_resource_threads{role=\"\"} 1").isEmpty());
        assertTrue(ScenarioMain.parseResource("# HELP omb_resource_threads live threads").isEmpty());
        assertTrue(ScenarioMain.parseResource("omb_resource_threads{role=\"driver\"}1").isEmpty());
    }

    @Test
    void parsesReceiveErrorsWithAndWithoutLabels() {
        assertEquals(
                OptionalDouble.of(4.0),
                ScenarioMain.parseReceiveErrors("synthetic_receive_errors_total 4.0"));
        assertEquals(OptionalDouble.of(5), ScenarioMain.parseReceiveErrors(LINES.get(15)));
        assertEquals(OptionalDouble.of(6), ScenarioMain.parseReceiveErrors(LINES.get(16)));
        assertFalse(
                ScenarioMain.parseReceiveErrors("synthetic_receive_errors_total_created 1").isPresent());
        assertFalse(
                ScenarioMain.parseReceiveErrors("synthetic_receive_errors_total{a=\"1\" 7").isPresent());
    }

    @Test
    void rejectsNonNumericValuesAsBefore() {
        assertThrows(
                NumberFormatException.class,
                () -> ScenarioMain.parseResource("omb_resource_threads{role=\"driver\"} many"));
        assertThrows(
                NumberFormatException.class,
                () -> ScenarioMain.parseReceiveErrors("synthetic_receive_errors_total x"));
    }

    @Test
    void agreesWithFormerPatterns() {
        for (String line : LINES) {
            Matcher former = FORMER_RESOURCE.matcher(line);
            Optional<ScenarioMain.ResourceSample> sample = ScenarioMain.parseResource(line);
            if (former.matches()) {
                ScenarioMain.ResourceSample expected =
                        new ScenarioMain.ResourceSample(
                                former.group(1), former.group(2), Double.parseDouble(former.group(3)));
                assertEquals(Optional.of(expected), sample, line);
            } else {
                assertTrue(sample.isEmpty(), line);
            }
            Matcher formerErrors = FORMER_RECEIVE_ERRORS.matcher(line);
            OptionalDouble errors = ScenarioMain.parseReceiveErrors(line);
            assertEquals(formerErrors.matches(), errors.isPresent(), line);
            if (errors.isPresent()) {
                assertEquals(Double.parseDouble(formerErrors.group(1)), errors.getAsDouble(), line);
            }
        }
    }

    @Test
    void handlesLongAdversarialLinesQuickly() {
        String labels = "role=\"x\",".repeat(20_000) + "a=\"" + " ".repeat(100_000);
        assertTimeoutPreemptively(
                Duration.ofSeconds(2),
                () -> {
                    assertTrue(ScenarioMain.parseResource("omb_resource_threads{" + labels + " 1").isEmpty());
                    assertFalse(
                            ScenarioMain.parseReceiveErrors("synthetic_receive_errors_total{" + labels + " 1")
                                    .isPresent());
                    assertFalse(
                            ScenarioMain.parseReceiveErrors(
                                            "synthetic_receive_errors_total" + " ".repeat(200_000))
                                    .isPresent());
                });
    }

    private static void assertResource(String name, String role, double value, String line) {
        assertEquals(
                Optional.of(new ScenarioMain.ResourceSample(name, role, value)),
                ScenarioMain.parseResource(line),
                line);
    }
}
