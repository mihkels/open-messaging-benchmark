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

import static java.util.stream.Collectors.joining;

import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import tools.jackson.databind.JsonNode;

/**
 * The workload and driver schemas published under docs/reference, packaged into this jar.
 * YamlSchemaTest generates them from {@link Workload} and {@link
 * io.openmessaging.benchmark.worker.DriverConfiguration}.
 */
final class YamlSchemas {
    private static final SchemaRegistry REGISTRY =
            SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);

    static final Schema WORKLOAD = load("workload-schema.json");
    static final Schema DRIVER = load("driver-schema.json");

    private YamlSchemas() {}

    /**
     * Checks a parsed YAML document against a schema.
     *
     * @param schema {@link #WORKLOAD} or {@link #DRIVER}
     * @param document the parsed file
     * @param context what the file is, such as {@code "Invalid workload x.yaml"}, leading the message
     * @throws IllegalArgumentException listing every violation, nested ones prefixed with their path
     */
    static void validate(Schema schema, JsonNode document, String context) {
        List<Error> errors = schema.validate(document);
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(
                    context + ": " + errors.stream().map(YamlSchemas::describe).collect(joining("; ")));
        }
    }

    // Top-level problems have an empty location; nested ones lead with the path to the bad value
    private static String describe(Error error) {
        String location = error.getInstanceLocation().toString();
        return location.isEmpty() ? error.getMessage() : location + ": " + error.getMessage();
    }

    private static Schema load(String file) {
        try (InputStream input = YamlSchemas.class.getResourceAsStream("schema/" + file)) {
            if (input == null) {
                throw new IllegalStateException(
                        "Bundled schema " + file + " is missing from the classpath");
            }
            return REGISTRY.getSchema(input);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
