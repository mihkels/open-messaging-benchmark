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

import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import io.openmessaging.benchmark.driver.RebalanceEvent;
import io.openmessaging.benchmark.worker.commands.RebalanceRecord;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/** Checks that result documents match the hand-written docs/reference/results-schema.json. */
class ResultsSchemaTest {
    private static final Path SCHEMA = Path.of("..", "docs", "reference", "results-schema.json");
    private static final ObjectMapper mapper = new ObjectMapper();
    private static Schema schema;

    @BeforeAll
    static void loadSchema() throws Exception {
        try (InputStream input = Files.newInputStream(SCHEMA)) {
            schema =
                    SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12).getSchema(input);
        }
    }

    @Test
    void goldenResultMatchesTheSchema() throws Exception {
        assertThat(messages(schema.validate(golden()))).isEmpty();
    }

    @Test
    void partitionTelemetryFieldsMatchTheSchema() throws Exception {
        ObjectNode document = golden();
        mapper
                .<ObjectNode>valueToTree(telemetryResult())
                .properties()
                .forEach(field -> {
                    if (List.of("partitionSkew", "rebalanceEvents", "droppedRebalanceEvents")
                            .contains(field.getKey())) {
                        document.set(field.getKey(), field.getValue());
                    }
                });
        assertThat(document.has("partitionSkew")).isTrue();
        assertThat(document.has("rebalanceEvents")).isTrue();
        assertThat(document.has("droppedRebalanceEvents")).isTrue();

        assertThat(messages(schema.validate(document))).isEmpty();
    }

    @Test
    void schemaRejectsAMalformedRebalanceEvent() throws Exception {
        ObjectNode document = golden();
        ObjectNode event = mapper.valueToTree(telemetryResult().rebalanceEvents.getFirst());
        event.put("kind", "MOVED");
        document.putArray("rebalanceEvents").add(event);

        assertThat(messages(schema.validate(document))).isNotEmpty();
    }

    @Test
    void schemaRejectsAMalformedSkewEntry() throws Exception {
        ObjectNode document = golden();
        ObjectNode skew = mapper.valueToTree(telemetryResult().partitionSkew.getFirst());
        skew.put("publishGini", 1.5);
        skew.put("unexpected", true);
        document.putArray("partitionSkew").add(skew);

        assertThat(messages(schema.validate(document))).hasSize(2);
    }

    private static TestResult telemetryResult() {
        TestResult result = new TestResult();
        result.partitionSkew =
                List.of(
                        new PartitionSkew(
                                0,
                                3,
                                1.5,
                                1.2,
                                0.2,
                                0.1,
                                List.of(new PartitionSkew.PartitionLatency("t", 2, 12.5)),
                                List.of()));
        result.rebalanceEvents =
                List.of(
                        new RebalanceRecord(
                                0, 1_000L, "local", "t", RebalanceEvent.Kind.ASSIGNED, List.of(0, 1)));
        result.droppedRebalanceEvents = 4;
        return result;
    }

    // The golden file drops the timing-dependent summary values; put plausible ones back
    private static ObjectNode golden() throws Exception {
        try (InputStream input =
                ResultsSchemaTest.class.getResourceAsStream("/golden/test-result.json")) {
            ObjectNode document = (ObjectNode) mapper.readTree(input);
            ((ObjectNode) document.get("summary"))
                    .put("publishRateAvg", 10.0)
                    .put("consumeRateAvg", 10.0)
                    .put("throughputMbSec", 0.01);
            return document;
        }
    }

    private static List<String> messages(List<Error> errors) {
        return errors.stream().map(Error::getMessage).toList();
    }
}
