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
package io.openmessaging.benchmark.plugin;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.PatternSyntaxException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLMapper;

/**
 * A driver plugin bundle: a directory with a {@code plugin.yaml} descriptor and its jars.
 *
 * @param directory the bundle directory
 * @param descriptor the parsed descriptor
 */
public record PluginBundle(Path directory, PluginDescriptor descriptor) {

    public static final String DESCRIPTOR_FILE = "plugin.yaml";

    private static final ObjectMapper MAPPER =
            YAMLMapper.builder()
                    .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                    .build();

    /** Loads and validates the bundle in {@code directory}. */
    public static PluginBundle load(Path directory) {
        Path dir = directory.toAbsolutePath().normalize();
        Path file = dir.resolve(DESCRIPTOR_FILE);
        if (!Files.isRegularFile(file)) {
            throw new PluginException("Plugin bundle " + dir + " has no " + DESCRIPTOR_FILE);
        }
        PluginDescriptor descriptor;
        try {
            descriptor = MAPPER.readValue(file.toFile(), PluginDescriptor.class);
        } catch (JacksonException e) {
            throw new PluginException(
                    "Invalid plugin descriptor " + file + ": " + e.getOriginalMessage(), e);
        }
        if (descriptor == null) {
            throw new PluginException("Invalid plugin descriptor " + file + ": file is empty");
        }
        List<String> errors = descriptor.validate();
        if (!errors.isEmpty()) {
            throw new PluginException(
                    "Invalid plugin descriptor " + file + ":\n  - " + String.join("\n  - ", errors));
        }
        PluginBundle bundle = new PluginBundle(dir, descriptor);
        bundle.classpath(); // fail fast on missing jars
        return bundle;
    }

    public String id() {
        return descriptor.id();
    }

    /**
     * Resolves the bundle classpath to existing absolute paths, in declaration order. Plain entries
     * must exist, wildcard entries may match nothing.
     */
    public List<Path> classpath() {
        List<Path> result = new ArrayList<>();
        for (String entry : descriptor.classpath()) {
            List<Path> resolved = resolve(entry);
            if (resolved.isEmpty() && !isPattern(entry)) {
                throw new PluginException(
                        "Plugin bundle '"
                                + id()
                                + "' at "
                                + directory
                                + ": classpath entry '"
                                + entry
                                + "' does not match any file");
            }
            addAll(result, resolved);
        }
        for (String entry : descriptor.optionalClasspath()) {
            addAll(result, resolve(entry));
        }
        if (result.isEmpty()) {
            throw new PluginException(
                    "Plugin bundle '" + id() + "' at " + directory + ": classpath does not match any file");
        }
        return result;
    }

    private static boolean isPattern(String entry) {
        return entry.contains("*") || entry.contains("?");
    }

    private static void addAll(List<Path> target, List<Path> paths) {
        for (Path path : paths) {
            if (!target.contains(path)) {
                target.add(path);
            }
        }
    }

    private List<Path> resolve(String entry) {
        Path relative = Path.of(entry.replace('\\', '/')).normalize();
        Path candidate = directory.resolve(relative).normalize();
        if (!candidate.startsWith(directory)) {
            throw new PluginException(
                    "Plugin bundle '" + id() + "': entry '" + entry + "' escapes " + directory);
        }
        String fileName = String.valueOf(relative.getFileName());
        if (!fileName.contains("*") && !fileName.contains("?")) {
            return Files.exists(candidate) ? List.of(candidate) : List.of();
        }
        Path parent = candidate.getParent();
        if (parent == null || !Files.isDirectory(parent)) {
            return List.of();
        }
        PathMatcher matcher;
        try {
            matcher = FileSystems.getDefault().getPathMatcher("glob:" + fileName);
        } catch (PatternSyntaxException e) {
            throw new PluginException(
                    "Invalid classpath glob '" + entry + "' in plugin '" + id() + "': " + e.getDescription(),
                    e);
        }
        List<Path> matches = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent)) {
            for (Path path : stream) {
                if (Files.isRegularFile(path) && matcher.matches(path.getFileName())) {
                    matches.add(path);
                }
            }
        } catch (IOException e) {
            throw new PluginException("Cannot list " + parent + " for bundle '" + id() + "'", e);
        }
        matches.sort(null);
        return matches;
    }
}
