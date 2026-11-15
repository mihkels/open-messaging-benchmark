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
import static org.assertj.core.api.Assertions.assertThatCode;

import io.openmessaging.benchmark.Finding.Severity;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkExitCodeTest {
    private static final Finding INFO = new Finding("tailSamples", Severity.INFO, "few", null, null);
    private static final Finding WARNING =
            new Finding("shortWindow", Severity.WARNING, "short", 1.0, 5.0);
    private static final Finding INVALID =
            new Finding("rateShortfall", Severity.INVALID, "slow", 0.2, 0.05);

    @Test
    void failOnInvalidFailsOnlyInvalidRuns() {
        Benchmark.Arguments arguments = arguments(true, false);

        assertThat(Benchmark.exitCode(arguments, written(INVALID), Map.of(), false))
                .isEqualTo(Benchmark.VALIDITY_FAILURE_EXIT_CODE);
        assertThat(Benchmark.exitCode(arguments, written(WARNING), Map.of(), false)).isZero();
        assertThat(Benchmark.exitCode(arguments, written(WARNING), Map.of(), true))
                .isEqualTo(Benchmark.THRESHOLD_FAILURE_EXIT_CODE);
    }

    @Test
    void failOnWarningFailsWarningAndInvalidRuns() {
        Benchmark.Arguments arguments = arguments(false, true);

        assertThat(Benchmark.exitCode(arguments, written(WARNING), Map.of(), false))
                .isEqualTo(Benchmark.VALIDITY_FAILURE_EXIT_CODE);
        assertThat(Benchmark.exitCode(arguments, written(INVALID), Map.of(), false))
                .isEqualTo(Benchmark.VALIDITY_FAILURE_EXIT_CODE);
        assertThat(Benchmark.exitCode(arguments, written(INFO), Map.of(), false)).isZero();
    }

    @Test
    void validityFailureWinsOverThresholdFailure() {
        assertThat(Benchmark.exitCode(arguments(true, false), written(INVALID), Map.of(), true))
                .isEqualTo(Benchmark.VALIDITY_FAILURE_EXIT_CODE);
    }

    @Test
    void resultWithoutValidityAndRunsWithoutFlagsDoNotFail() {
        Map<String, TestResult> legacy = Map.of("old.json", new TestResult());

        assertThat(Benchmark.exitCode(arguments(true, true), legacy, Map.of(), false)).isZero();
        assertThat(Benchmark.exitCode(arguments(false, false), written(INVALID), Map.of(), false))
                .isZero();
        assertThat(Benchmark.exitCode(arguments(false, false), written(INVALID), Map.of(), true))
                .isEqualTo(Benchmark.THRESHOLD_FAILURE_EXIT_CODE);
    }

    @Test
    void failOnWarningAlsoJudgesRepetitionSummaries() {
        Map<String, Validity> summaries = Map.of("result-summary.json", Validity.of(List.of(WARNING)));

        assertThat(Benchmark.exitCode(arguments(false, true), written(INFO), summaries, false))
                .isEqualTo(Benchmark.VALIDITY_FAILURE_EXIT_CODE);
        assertThat(Benchmark.exitCode(arguments(true, false), written(INFO), summaries, false))
                .isZero();
    }

    @Test
    void printsVerdictsWithAndWithoutChecks() {
        Map<String, TestResult> written = new LinkedHashMap<>();
        written.putAll(written(INFO));
        written.put("b.json", written(WARNING, INFO, WARNING).get("a.json"));

        assertThatCode(() -> Benchmark.printVerdicts(written)).doesNotThrowAnyException();
        assertThatCode(() -> Benchmark.printVerdicts(Map.of())).doesNotThrowAnyException();
    }

    private static Benchmark.Arguments arguments(boolean failOnInvalid, boolean failOnWarning) {
        Benchmark.Arguments arguments = new Benchmark.Arguments();
        arguments.failOnInvalid = failOnInvalid;
        arguments.failOnWarning = failOnWarning;
        return arguments;
    }

    private static Map<String, TestResult> written(Finding... findings) {
        TestResult result = new TestResult();
        result.findings = List.of(findings);
        result.validity = Validity.of(result.findings);
        return Map.of("a.json", result);
    }
}
