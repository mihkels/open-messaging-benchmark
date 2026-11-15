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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openmessaging.benchmark.Finding.Severity;
import java.util.List;

/**
 * Whether a completed run is a trustworthy measurement, written to the result's {@code validity}.
 *
 * @param status the verdict: {@code invalid} with any invalid finding, else {@code warning} with
 *     any warning finding, else {@code valid}
 * @param findings the findings the verdict is based on
 */
public record Validity(Status status, List<Finding> findings) {

    public Validity {
        findings = List.copyOf(findings);
    }

    /** The verdict, from best to worst. */
    public enum Status {
        @JsonProperty("valid")
        VALID,
        @JsonProperty("warning")
        WARNING,
        @JsonProperty("invalid")
        INVALID
    }

    /** The verdict of these findings. */
    static Validity of(List<Finding> findings) {
        Status status = Status.VALID;
        for (Finding finding : findings) {
            if (finding.severity() == Severity.INVALID) {
                status = Status.INVALID;
            } else if (finding.severity() == Severity.WARNING && status == Status.VALID) {
                status = Status.WARNING;
            }
        }
        return new Validity(status, findings);
    }
}
