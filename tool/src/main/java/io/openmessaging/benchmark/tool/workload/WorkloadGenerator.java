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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.yaml.YAMLFactory;

/** Expands a {@link WorkloadSetTemplate} into a set of {@link Workload Workloads}. */
final class WorkloadGenerator {
    private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);

    private final WorkloadSetTemplate template;
    private final WorkloadNameFormat nameFormat;

    /**
     * Creates a new WorkloadGenerator with the specified template.
     *
     * @param template the workload set template to use for generation
     * @throws IllegalArgumentException if template is null
     */
    WorkloadGenerator(WorkloadSetTemplate template) {
        this.template = Objects.requireNonNull(template, "Template cannot be null");
        this.nameFormat = new WorkloadNameFormat(template.nameFormat());
    }

    /**
     * Generates a list of workloads based on the template configuration. Creates all combinations of
     * the parameter lists defined in the template.
     *
     * @return a list of generated workloads
     * @throws IOException if there's an error during workload generation
     */
    List<Workload> generate() throws IOException {
        log.info("Generating workloads from template with name format: {}", template.nameFormat());

        List<Workload> workloads = new ArrayList<>();

        List<List<Integer>> values =
                SupportedWorkloadFields.SWEEP_FIELDS.stream()
                        .map(field -> field.valuesOrDefault(template))
                        .toList();
        generateCombinations(values, new int[values.size()], 0, workloads);

        log.info("Generated {} workloads", workloads.size());
        return workloads;
    }

    private void generateCombinations(
            List<List<Integer>> values, int[] selected, int index, List<Workload> workloads) {
        if (index == values.size()) {
            Workload workload = new Workload();
            for (int i = 0; i < selected.length; i++) {
                SupportedWorkloadFields.SWEEP_FIELDS.get(i).assign().accept(workload, selected[i]);
            }
            SupportedWorkloadFields.applyFixedFields(template, workload);
            workload.name = nameFormat.from(workload);
            workloads.add(workload);
            return;
        }

        for (int value : values.get(index)) {
            selected[index] = value;
            generateCombinations(values, selected, index + 1, workloads);
        }
    }

    /**
     * Creates a deep copy of the given workload.
     *
     * @param workload the workload to copy
     * @return a new workload instance with the same values
     */
    @SuppressWarnings("unused")
    private Workload copyOf(Workload workload) {
        Objects.requireNonNull(workload, "Workload cannot be null");

        // Use Jackson for deep copying
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        ObjectReader reader =
                mapper.readerFor(Workload.class).without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        String serialized = mapper.writeValueAsString(workload);
        return reader.readValue(serialized);
    }
}
