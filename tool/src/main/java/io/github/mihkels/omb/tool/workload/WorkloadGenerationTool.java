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
package io.github.mihkels.omb.tool.workload;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.github.mihkels.omb.Workload;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.deser.DeserializationProblemHandler;
import tools.jackson.dataformat.yaml.YAMLFactory;

/** Generates a set of {@link Workload} definition files from a {@link WorkloadSetTemplate} file. */
public class WorkloadGenerationTool {
    private static final Logger log = LoggerFactory.getLogger(WorkloadGenerationTool.class);
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
            System.exit(0);
        }

        // Dump configuration variables
        log.atInfo()
                .setMessage("Starting benchmark with config: {}")
                .addArgument(() -> mapper.writeValueAsString(arguments))
                .log();

        List<String> ignored = new ArrayList<>();
        WorkloadSetTemplate template = readTemplate(arguments.templateFile, ignored);
        ignored.forEach(
                field -> log.warn("Ignoring unknown template field '{}'; check its spelling", field));
        List<Workload> workloads = new WorkloadSetGenerator(template).generate();
        for (Workload w : workloads) {
            var outputFile = new File(arguments.outputFolder, w.name + ".yaml");
            workloadWriter.writeValue(outputFile, w);
        }
    }

    /**
     * Reads a template. Unknown fields are skipped, so templates written for other versions still
     * load, but each is collected: a misspelt field silently drops that dimension of the sweep.
     *
     * @param file the template file
     * @param ignoredFields receives the name of every unknown field
     * @return the template
     */
    static WorkloadSetTemplate readTemplate(File file, List<String> ignoredFields) {
        return templateReader
                .withHandler(
                        new DeserializationProblemHandler() {
                            @Override
                            public boolean handleUnknownProperty(
                                    DeserializationContext ctxt,
                                    JsonParser p,
                                    ValueDeserializer<?> deserializer,
                                    Object beanOrClass,
                                    String propertyName) {
                                ignoredFields.add(propertyName);
                                p.skipChildren();
                                return true;
                            }
                        })
                .readValue(file);
    }

    static class Arguments {
        @Parameter(
                names = {"-t", "--template-file"},
                description = "Path to a YAML file containing the workload template",
                required = true)
        @SuppressWarnings("NullAway.Init") // JCommander sets required parameters
        public File templateFile;

        @Parameter(
                names = {"-o", "--output-folder"},
                description = "Output",
                required = true)
        @SuppressWarnings("NullAway.Init") // JCommander sets required parameters
        public File outputFolder;

        @Parameter(
                names = {"-h", "--help"},
                description = "Help message",
                help = true)
        boolean help;
    }
}
