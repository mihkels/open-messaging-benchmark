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
 */
public record ArtifactCoordinates(String groupId, String artifactId, String version) {

    public String key() {
        return groupId + ":" + artifactId;
    }

    @Override
    public String toString() {
        return groupId + ":" + artifactId + ":" + version;
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
            return candidates.stream().filter(c -> c.matchesFileName(fileName)).findFirst();
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    boolean matchesFileName(String fileName) {
        String av = artifactId + "-" + version;
        return fileName.startsWith(av + ".")
                || fileName.startsWith(av + "-")
                || fileName.startsWith(groupId + "-" + av + ".")
                || fileName.startsWith(groupId + "-" + av + "-");
    }
}
