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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.openmessaging.benchmark.Workload;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.yaml.YAMLFactory;

/** Generates a set of {@link Workload} definition files from a {@link WorkloadSetTemplate} file. */
public class WorkloadGenerationTool {
    public static final Logger log = LoggerFactory.getLogger(WorkloadGenerationTool.class);
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectReader templateReader =
            mapper
                    .readerFor(WorkloadSetTemplate.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private static final ObjectWriter workloadWriter = mapper.writer();

    static void main(String[] args) throws IOException {
        final WorkloadGenerationTool.Arguments arguments = new WorkloadGenerationTool.Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("workload-generator");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            log.error(e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(-1);
        }

        // Dump configuration variables
        log.info("Starting benchmark with config: {}", mapper.writeValueAsString(arguments));

        WorkloadSetTemplate template = templateReader.readValue(arguments.templateFile);
        List<Workload> workloads = new WorkloadGenerator(template).generate();
        for (Workload w : workloads) {
            var outputFile = new File(arguments.outputFolder, w.name + ".yaml");
            workloadWriter.writeValue(outputFile, w);
        }
    }

    static class Arguments {
        @Parameter(
                names = {"-t", "--template-file"},
                description = "Path to a YAML file containing the workload template",
                required = true)
        public File templateFile;

        @Parameter(
                names = {"-o", "--output-folder"},
                description = "Output",
                required = true)
        public File outputFolder;

        @Parameter(
                names = {"-h", "--help"},
                description = "Help message",
                help = true)
        boolean help;
    }
}
