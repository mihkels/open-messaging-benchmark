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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/**
 * One observation about how trustworthy a completed run is, written to the result's {@code
 * findings}.
 *
 * @param check the name of the check that produced it, such as {@code rateShortfall}
 * @param severity how much it undermines the result
 * @param message a human-readable explanation
 * @param value the observed value, if the check has one
 * @param limit the limit the value was compared against, if any
 */
public record Finding(
        String check,
        Severity severity,
        String message,
        @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable Double value,
        @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable Double limit) {

    /** How much a finding undermines the result, from least to most. */
    public enum Severity {
        @JsonProperty("info")
        INFO,
        @JsonProperty("warning")
        WARNING,
        @JsonProperty("invalid")
        INVALID
    }
}
