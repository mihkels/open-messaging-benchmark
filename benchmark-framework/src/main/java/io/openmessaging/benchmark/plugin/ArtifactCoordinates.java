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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Maven coordinates of a jar on a driver process classpath.
 *
 * @param groupId the Maven group id
 * @param artifactId the Maven artifact id
 * @param version the Maven version
 * @param classifier the Maven classifier, or {@code null} for the main artifact
 */
public record ArtifactCoordinates(
        String groupId, String artifactId, String version, String classifier) {

    public ArtifactCoordinates(String groupId, String artifactId, String version) {
        this(groupId, artifactId, version, null);
    }

    public String key() {
        return groupId + ":" + artifactId;
    }

    public String variantKey() {
        return classifier == null ? key() : key() + ":" + classifier;
    }

    @Override
    public String toString() {
        return groupId
                + ":"
                + artifactId
                + ":"
                + version
                + (classifier == null ? "" : ":" + classifier);
    }

    /**
     * Detects the coordinates of a jar from its embedded {@code pom.properties}. Shaded jars embed
     * the descriptors of every artifact they contain, so a descriptor is only accepted when the jar
     * file name starts with {@code artifactId-version} or {@code groupId-artifactId-version}, which
     * is how Maven names copied dependencies.
     */
    public static Optional<ArtifactCoordinates> detect(Path jar) {
        String fileName = String.valueOf(jar.getFileName());
        if (!Files.isRegularFile(jar) || !fileName.endsWith(".jar")) {
            return Optional.empty();
        }
        try (ZipFile zip = new ZipFile(jar.toFile())) {
            List<ArtifactCoordinates> candidates = new ArrayList<>();
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                String name = entry.getName();
                if (name.startsWith("META-INF/maven/") && name.endsWith("/pom.properties")) {
                    Properties properties = new Properties();
                    try (InputStream in = zip.getInputStream(entry)) {
                        properties.load(in);
                    }
                    String g = properties.getProperty("groupId");
                    String a = properties.getProperty("artifactId");
                    String v = properties.getProperty("version");
                    if (g != null && a != null && v != null) {
                        candidates.add(new ArtifactCoordinates(g.trim(), a.trim(), v.trim()));
                    }
                }
            }
            return candidates.stream()
                    .map(c -> c.withFileClassifier(fileName))
                    .flatMap(Optional::stream)
                    .findFirst();
        } catch (IOException e) {
            throw new PluginException("Cannot inspect plugin jar " + jar, e);
        }
    }

    private Optional<ArtifactCoordinates> withFileClassifier(String fileName) {
        String av = artifactId + "-" + version;
        for (String base : List.of(groupId + "-" + av, av)) {
            if (fileName.equals(base + ".jar")) {
                return Optional.of(this);
            }
            if (fileName.startsWith(base + "-") && fileName.endsWith(".jar")) {
                String suffix = fileName.substring(base.length() + 1, fileName.length() - 4);
                if (!suffix.isEmpty()) {
                    return Optional.of(new ArtifactCoordinates(groupId, artifactId, version, suffix));
                }
            }
        }
        return Optional.empty();
    }
}
