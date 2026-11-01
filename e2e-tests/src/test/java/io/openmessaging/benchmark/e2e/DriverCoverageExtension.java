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
package io.openmessaging.benchmark.e2e;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;

/** Records actual contract outcomes for the current Failsafe JVM, not old XML reports. */
final class DriverCoverageExtension implements BeforeAllCallback, TestWatcher {
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final Map<String, Map<String, String>> OUTCOMES = new TreeMap<>();
    private static Set<String> pluginIds;
    private static Path report;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        synchronized (OUTCOMES) {
            if (pluginIds == null) {
                Path root = Path.of(System.getProperty("omb.home"));
                pluginIds = discoverPluginIds(root);
                report = root.resolve("e2e-tests/target/testcontainers-coverage.txt");
                writeReport();
                System.out.println("Testcontainers contract coverage: " + report);
            }
        }
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        record(context, "PASSED");
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        record(context, "FAILED");
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        record(context, "SKIPPED");
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        record(context, "SKIPPED");
    }

    private static void record(ExtensionContext context, String status) {
        context
                .getTestMethod()
                .ifPresent(
                        method -> {
                            DriverContract contract = method.getAnnotation(DriverContract.class);
                            if (contract == null) {
                                return;
                            }
                            synchronized (OUTCOMES) {
                                if (pluginIds == null) {
                                    return;
                                }
                                String test = method.getDeclaringClass().getSimpleName() + "#" + method.getName();
                                OUTCOMES
                                        .computeIfAbsent(contract.value(), ignored -> new TreeMap<>())
                                        .put(test, status);
                                try {
                                    writeReport();
                                } catch (IOException e) {
                                    throw new IllegalStateException("Cannot write Testcontainers coverage report", e);
                                }
                            }
                        });
    }

    static Set<String> discoverPluginIds(Path root) throws IOException {
        Set<String> ids = new TreeSet<>();
        try (var modules = Files.newDirectoryStream(root, "driver-*")) {
            for (Path module : modules) {
                Path descriptor = module.resolve("src/main/plugin/plugin.yaml");
                if (!Files.isRegularFile(descriptor)) {
                    continue;
                }
                var id = YAML.readTree(descriptor.toFile()).get("id");
                if (id == null || !id.isTextual() || id.asString().isBlank()) {
                    throw new IllegalStateException("Plugin descriptor has no id: " + descriptor);
                }
                if (!ids.add(id.asString())) {
                    throw new IllegalStateException(
                            "Duplicate plugin id in " + descriptor + ": " + id.asString());
                }
            }
        }
        if (ids.isEmpty()) {
            throw new IllegalStateException("No shipped driver plugin descriptors found under " + root);
        }
        return ids;
    }

    static String render(
            Set<String> ids, Map<String, Map<String, String>> outcomes, String selection) {
        StringBuilder text =
                new StringBuilder("Testcontainers contract outcomes for the current Failsafe run\n")
                        .append("Selection: ")
                        .append(selection)
                        .append("\n\n");
        Set<String> allIds = new TreeSet<>(ids);
        allIds.addAll(outcomes.keySet());
        for (String id : allIds) {
            Map<String, String> tests = outcomes.getOrDefault(id, Map.of());
            String state;
            if (!ids.contains(id)) {
                state = "UNKNOWN_PLUGIN";
            } else if (tests.containsValue("FAILED")) {
                state = "FAILED";
            } else if (tests.containsValue("PASSED")) {
                state = "PASSED";
            } else if (tests.containsValue("SKIPPED")) {
                state = "SKIPPED";
            } else {
                state = "NOT_EXECUTED";
            }
            text.append(id).append(" | ").append(state);
            tests.forEach((test, result) -> text.append(" | ").append(test).append("=").append(result));
            text.append("\n");
        }
        return text.toString();
    }

    private static void writeReport() throws IOException {
        Files.createDirectories(report.getParent());
        Files.writeString(
                report,
                render(pluginIds, OUTCOMES, System.getProperty("it.test", "<all integration tests>")));
    }
}
