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
package io.github.mihkels.omb;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.victools.jsonschema.generator.FieldScope;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonSchemaModule;
import io.github.mihkels.omb.worker.DriverConfiguration;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Generates docs/reference/workload-schema.json and driver-schema.json from {@link Workload} and
 * {@link DriverConfiguration}, and fails when the committed files are stale. The CLI validates
 * workload and driver files against the committed copies, so this is where their rules live.
 */
class YamlSchemaTest {
    private static final Path REFERENCE = Path.of("..", "docs", "reference");
    private static final String BASE_ID =
            "https://mihkels.github.io/open-messaging-benchmark/reference/";
    private static final String UPDATE_COMMAND =
            "mvn -pl benchmark-framework test -Dtest=YamlSchemaTest -DupdateSchema=true";

    private static final Set<String> REQUIRED_WORKLOAD_FIELDS =
            Set.of(
                    "name",
                    "topics",
                    "partitionsPerTopic",
                    "subscriptionsPerTopic",
                    "producersPerTopic",
                    "consumerPerSubscription");
    private static final Set<String> AT_LEAST_ONE =
            Set.of(
                    "topics",
                    "partitionsPerTopic",
                    "subscriptionsPerTopic",
                    "producersPerTopic",
                    "consumerPerSubscription");

    private static final ObjectMapper JSON = JsonMapper.builder().build();

    @Test
    void committedWorkloadSchemaMatchesTheWorkloadClass() throws IOException {
        checkOrUpdate("workload-schema.json", workloadSchema());
    }

    @Test
    void committedDriverSchemaMatchesTheDriverConfigurationClass() throws IOException {
        checkOrUpdate("driver-schema.json", driverSchema());
    }

    static String workloadSchema() {
        SchemaGeneratorConfigBuilder builder = baseConfig();
        builder.with(
                Option.FORBIDDEN_ADDITIONAL_PROPERTIES_BY_DEFAULT,
                Option.MAP_VALUES_AS_ADDITIONAL_PROPERTIES);
        builder
                .forFields()
                .withRequiredCheck(YamlSchemaTest::isRequired)
                .withNumberInclusiveMinimumResolver(
                        field -> isWorkloadField(field, AT_LEAST_ONE) ? BigDecimal.ONE : null)
                // Explicit nulls keep optional fields at their default; primitives and required
                // fields have nothing to fall back to
                .withNullableCheck(field -> !field.getType().isPrimitive() && !isRequired(field));
        ObjectNode schema = new SchemaGenerator(builder.build()).generateSchema(Workload.class);
        // Rules that depend on another field being absent or empty; Workload.validate() covers the rest
        ArrayNode allOf = schema.putArray("allOf");
        allOf.add(
                requiredUnlessNonEmpty(
                        "stages", "array", "minItems", "producerRate", "testDurationMinutes"));
        allOf.add(
                requiredUnlessNonEmpty(
                        "messageSizeDistribution", "object", "minProperties", "messageSize"));
        return render(
                schema,
                "workload-schema.json",
                "OpenMessaging Benchmark workload",
                "Workload YAML file passed to the benchmark CLI.");
    }

    static String driverSchema() {
        SchemaGeneratorConfigBuilder builder = baseConfig();
        // Driver-specific keys are free-form; each driver reads its own settings
        builder
                .forFields()
                .withRequiredCheck(_ -> true)
                .withNullableCheck(_ -> false)
                .withStringMinLengthResolver(_ -> 1);
        ObjectNode schema =
                new SchemaGenerator(builder.build()).generateSchema(DriverConfiguration.class);
        return render(
                schema,
                "driver-schema.json",
                "OpenMessaging Benchmark driver configuration",
                "Driver YAML file passed to the benchmark CLI; keys other than name and driverClass are"
                        + " read by the driver.");
    }

    private static SchemaGeneratorConfigBuilder baseConfig() {
        return new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON)
                .with(new JacksonSchemaModule());
    }

    // Nested records (stages, consumerPause, payloadGenerator, keyDistribution) need every component
    private static boolean isRequired(FieldScope field) {
        return field.getDeclaringType().getErasedType().isRecord()
                || isWorkloadField(field, REQUIRED_WORKLOAD_FIELDS);
    }

    private static boolean isWorkloadField(FieldScope field, Set<String> names) {
        return field.getDeclaringType().getErasedType() == Workload.class
                && names.contains(field.getDeclaredName());
    }

    private static ObjectNode requiredUnlessNonEmpty(
            String absent, String type, String minKeyword, String... required) {
        ObjectNode rule = JSON.createObjectNode();
        ObjectNode nonEmpty = rule.putObject("if").putObject("not");
        nonEmpty.putArray("required").add(absent);
        nonEmpty.putObject("properties").putObject(absent).put("type", type).put(minKeyword, 1);
        ArrayNode then = rule.putObject("then").putArray("required");
        for (String field : required) {
            then.add(field);
        }
        return rule;
    }

    // $schema, $id, title and description first, then the generated body
    private static String render(
            ObjectNode generated, String file, String title, String description) {
        ObjectNode schema = JSON.createObjectNode();
        schema.put("$schema", SchemaVersion.DRAFT_2020_12.getIdentifier());
        schema.put("$id", BASE_ID + file);
        schema.put("title", title);
        schema.put("description", description);
        generated
                .properties()
                .forEach(entry -> {
                    if (!schema.has(entry.getKey())) {
                        schema.set(entry.getKey(), entry.getValue());
                    }
                });
        return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(schema) + "\n";
    }

    private static void checkOrUpdate(String file, String generated) throws IOException {
        Path committed = REFERENCE.resolve(file);
        if (Boolean.getBoolean("updateSchema")) {
            Files.writeString(committed, generated);
            return;
        }
        assertThat(Files.exists(committed))
                .as("%s is missing; run: %s", committed, UPDATE_COMMAND)
                .isTrue();
        assertThat(Files.readString(committed))
                .as("%s is stale; run: %s", committed, UPDATE_COMMAND)
                .isEqualTo(generated);
    }
}
