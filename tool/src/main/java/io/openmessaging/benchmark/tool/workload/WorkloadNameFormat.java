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
package io.openmessaging.benchmark.tool.workload;

import io.openmessaging.benchmark.Workload;
import org.apache.commons.text.StringSubstitutor;

/**
 * Generates {@link Workload} names based on a template. Substitutes template place-holders of the
 * form {@code ${variableName}}, where {@code variableName} is a workload field supported by {@link
 * WorkloadSetTemplate}. Numeric values will typically be in a form that includes an SI suffix.
 *
 * @param format the template string used to build workload names
 */
record WorkloadNameFormat(String format) {

    /**
     * Generates a name for the given workload. If the workload has a non-null name, that is simply
     * returned. Otherwise, the format string is used to generate a name.
     *
     * @param workload the workload
     * @return the name of the workload
     */
    String from(Workload workload) {
        if (workload.name != null) {
            return workload.name;
        }
        return StringSubstitutor.replace(
                format, SupportedWorkloadFields.nameVariables(workload), "${", "}");
    }
}
