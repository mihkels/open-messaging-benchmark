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

import io.openmessaging.benchmark.driver.BenchmarkDriver;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The benchmark core shared by every driver process: the framework itself, driver-api and the
 * libraries they need at runtime. The list of artifacts is generated at build time into {@value
 * #RESOURCE}; the jars are located on the classpath of the running JVM.
 */
public final class HostRuntime {

    static final String RESOURCE = "META-INF/omb/worker-runtime-artifacts.txt";

    private static final Pattern ARTIFACT =
            Pattern.compile(
                    "^\\s*([^:\\s]+):([^:\\s]+):([^:\\s]+)(?::([^:\\s]+))?:([^:\\s]+):([a-z]+)(?:\\s.*)?$");

    /**
     * A jar or class directory of the core, with its Maven coordinates.
     *
     * @param path the jar or class directory
     * @param coordinates its Maven coordinates
     */
    public record Entry(Path path, ArtifactCoordinates coordinates) {}

    private final List<Entry> entries;

    HostRuntime(List<Entry> entries) {
        this.entries = List.copyOf(entries);
    }

    public List<Entry> entries() {
        return entries;
    }

    /**
     * Locates the core of the running JVM.
     *
     * @return the core classpath entries of this JVM
     */
    public static HostRuntime current() {
        ClassLoader loader = HostRuntime.class.getClassLoader();
        List<String> lines = new ArrayList<>();
        InputStream in = loader.getResourceAsStream(RESOURCE);
        if (in == null) {
            throw new PluginException("Missing " + RESOURCE + " in the benchmark framework");
        }
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            reader.lines().forEach(lines::add);
        } catch (IOException e) {
            throw new PluginException("Cannot read " + RESOURCE, e);
        }

        Map<String, Path> byFileName = new LinkedHashMap<>();
        for (Path path : jvmClasspath()) {
            byFileName.putIfAbsent(String.valueOf(path.getFileName()), path);
        }

        List<Entry> entries = new ArrayList<>();
        entries.add(
                new Entry(
                        codeSource(HostRuntime.class),
                        new ArtifactCoordinates("io.openmessaging.benchmark", "benchmark-framework", "local")));
        List<String> missing = new ArrayList<>();
        for (String line : lines) {
            Matcher m = ARTIFACT.matcher(line);
            if (!m.matches() || !"jar".equals(m.group(3))) {
                continue;
            }
            String groupId = m.group(1);
            String artifactId = m.group(2);
            String classifier = m.group(4);
            String version = m.group(5);
            ArtifactCoordinates coordinates = new ArtifactCoordinates(groupId, artifactId, version);
            Optional<Path> path = locate(byFileName, loader, coordinates, classifier);
            if (path.isPresent()) {
                entries.add(new Entry(path.get(), coordinates));
            } else {
                missing.add(coordinates + (classifier == null ? "" : ":" + classifier));
            }
        }
        if (!missing.isEmpty()) {
            throw new PluginException(
                    "Cannot locate benchmark core artifacts on the classpath: " + missing);
        }
        return new HostRuntime(entries);
    }

    private static Optional<Path> locate(
            Map<String, Path> byFileName, ClassLoader loader, ArtifactCoordinates c, String classifier) {
        String suffix = (classifier == null ? "" : "-" + classifier) + ".jar";
        String base = c.artifactId() + "-" + c.version() + suffix;
        for (String name : List.of(base, c.groupId() + "-" + base)) {
            Path path = byFileName.get(name);
            if (path != null) {
                return Optional.of(path);
            }
        }
        if (classifier == null) {
            URL url =
                    loader.getResource(
                            "META-INF/maven/" + c.groupId() + "/" + c.artifactId() + "/pom.properties");
            if (url != null && "jar".equals(url.getProtocol())) {
                try {
                    JarURLConnection connection = (JarURLConnection) url.openConnection();
                    connection.setUseCaches(false);
                    return Optional.of(Path.of(connection.getJarFileURL().toURI()));
                } catch (IOException | URISyntaxException | IllegalArgumentException e) {
                    // fall through
                }
            }
        }
        if ("driver-api".equals(c.artifactId()) && "io.openmessaging.benchmark".equals(c.groupId())) {
            return Optional.of(codeSource(BenchmarkDriver.class));
        }
        return Optional.empty();
    }

    /**
     * Entries of {@code java.class.path}, including manifest {@code Class-Path} references.
     *
     * @return the classpath entries of this JVM
     */
    static List<Path> jvmClasspath() {
        List<Path> result = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path", "").split(File.pathSeparator)) {
            if (entry.isBlank()) {
                continue;
            }
            Path path = Path.of(entry).toAbsolutePath().normalize();
            result.add(path);
            if (Files.isRegularFile(path) && entry.endsWith(".jar")) {
                result.addAll(manifestClasspath(path));
            }
        }
        return result;
    }

    private static List<Path> manifestClasspath(Path jar) {
        List<Path> result = new ArrayList<>();
        try (JarFile jarFile = new JarFile(jar.toFile())) {
            Manifest manifest = jarFile.getManifest();
            String classPath =
                    manifest == null ? null : manifest.getMainAttributes().getValue("Class-Path");
            if (classPath == null) {
                return result;
            }
            URI base = jar.toUri();
            for (String ref : classPath.trim().split("\\s+")) {
                try {
                    URI uri = base.resolve(ref);
                    if ("file".equals(uri.getScheme())) {
                        result.add(Path.of(uri).normalize());
                    }
                } catch (IllegalArgumentException e) {
                    // ignore malformed references
                }
            }
        } catch (IOException e) {
            // not a readable jar
        }
        return result;
    }

    private static Path codeSource(Class<?> type) {
        CodeSource source = type.getProtectionDomain().getCodeSource();
        if (source == null || source.getLocation() == null) {
            throw new PluginException("Cannot determine the location of " + type.getName());
        }
        try {
            return Path.of(source.getLocation().toURI()).toAbsolutePath().normalize();
        } catch (URISyntaxException e) {
            throw new PluginException("Cannot determine the location of " + type.getName(), e);
        }
    }
}
