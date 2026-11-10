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
import static org.mockito.Mockito.mock;

import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.RunPhase;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.slf4j.MDC;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class JsonLoggingTest {

    @Test
    void jsonLayoutRendersPhaseEventsWithTheRunContext(@TempDir Path directory) throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        URI original = context.getConfigLocation();
        System.setProperty("omb.log.format", "json");
        System.setProperty("omb.log.name", directory.resolve("omb").toString());
        try {
            context.setConfigLocation(
                    Objects.requireNonNull(JsonLoggingTest.class.getResource("/log4j2.xml")).toURI());
            MDC.put("run_id", "run-1");
            WorkloadGenerator generator;
            try {
                generator = new WorkloadGenerator("driver", new Workload(), mock(Worker.class));
            } finally {
                MDC.remove("run_id");
            }
            // The run context comes from the generator's creator, not the logging thread
            try (generator) {
                generator.enterPhase(RunPhase.WARMUP);
            }
        } finally {
            System.clearProperty("omb.log.format");
            System.clearProperty("omb.log.name");
            context.setConfigLocation(original);
        }

        ObjectMapper mapper = new ObjectMapper();
        List<JsonNode> events =
                Files.readAllLines(directory.resolve("omb.log")).stream()
                        .map(mapper::readTree)
                        .filter(event -> "phase".equals(event.path("event").asString()))
                        .toList();
        assertThat(events)
                .singleElement()
                .satisfies(event -> {
                    assertThat(event.path("phase").asString()).isEqualTo("WARMUP");
                    assertThat(event.path("run_id").asString()).isEqualTo("run-1");
                    assertThat(event.path("message").asString()).isEqualTo("Run phase: WARMUP");
                    assertThat(event.has("@timestamp")).isTrue();
                });
    }
}
