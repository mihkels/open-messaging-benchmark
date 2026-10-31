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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.maven.artifact.versioning.ComparableVersion;

/**
 * Composes the classpath of a driver process: the benchmark core first, then the jars of the
 * resolved bundles.
 *
 * <p>Jars of the same artifact and classifier are reconciled by Maven coordinates:
 *
 * <ul>
 *   <li>a bundle copy of a core artifact with the same version is dropped;
 *   <li>a bundle copy of a core artifact with a different version is an error for the driver SPI
 *       boundary types (driver-api, Micrometer and the Prometheus model), otherwise the core
 *       version wins and a warning is reported;
 *   <li>bundles combined in one process (e.g. JMS and its delegate) must agree on versions of the
 *       same artifact variant, except for annotation-only artifacts, where the highest version wins
 *       and a warning is reported. Different classifiers, such as native platform libraries, are
 *       kept.
 * </ul>
 */
public final class RuntimeClasspath {

    /** Artifacts whose types cross the driver SPI and must be identical in host and plugin. */
    static final Set<String> BOUNDARY_ARTIFACTS =
            Set.of(
                    "io.openmessaging.benchmark:driver-api",
                    "io.micrometer:micrometer-core",
                    "io.micrometer:micrometer-commons",
                    "io.micrometer:micrometer-observation",
                    "io.micrometer:micrometer-registry-prometheus",
                    "io.prometheus:prometheus-metrics-core",
                    "io.prometheus:prometheus-metrics-model");

    /**
     * Artifacts that only carry annotations without runtime behavior, so bundles combined in one
     * process can safely share the highest version.
     */
    static final Set<String> ANNOTATION_ARTIFACTS =
            Set.of(
                    "com.google.errorprone:error_prone_annotations",
                    "com.google.code.findbugs:jsr305",
                    "com.google.j2objc:j2objc-annotations",
                    "org.checkerframework:checker-qual",
                    "org.codehaus.mojo:animal-sniffer-annotations",
                    "com.github.spotbugs:spotbugs-annotations",
                    "org.jetbrains:annotations",
                    "org.jspecify:jspecify");

    private final List<Path> entries;
    private final List<String> warnings;

    private RuntimeClasspath(List<Path> entries, List<String> warnings) {
        this.entries = List.copyOf(entries);
        this.warnings = List.copyOf(warnings);
    }

    public List<Path> entries() {
        return entries;
    }

    public List<String> warnings() {
        return warnings;
    }

    public static RuntimeClasspath compose(HostRuntime host, ResolvedDriver driver) {
        Set<Path> result = new LinkedHashSet<>();
        Map<String, ArtifactCoordinates> hostArtifacts = new HashMap<>();
        for (HostRuntime.Entry entry : host.entries()) {
            result.add(entry.path());
            hostArtifacts.put(entry.coordinates().variantKey(), entry.coordinates());
        }

        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        Map<String, ArtifactCoordinates> bundleArtifacts = new HashMap<>();
        Map<String, String> bundleOwners = new HashMap<>();
        Map<String, Path> bundlePaths = new HashMap<>();
        for (PluginBundle bundle : driver.bundles()) {
            for (Path path : bundle.classpath()) {
                Optional<ArtifactCoordinates> detected = ArtifactCoordinates.detect(path);
                if (detected.isEmpty()) {
                    result.add(path);
                    continue;
                }
                ArtifactCoordinates c = detected.get();
                ArtifactCoordinates hostCopy = hostArtifacts.get(c.variantKey());
                if (hostCopy != null) {
                    if (!hostCopy.version().equals(c.version())) {
                        String message =
                                "Plugin '"
                                        + bundle.id()
                                        + "' bundles "
                                        + c
                                        + " but the benchmark core provides version "
                                        + hostCopy.version();
                        if (BOUNDARY_ARTIFACTS.contains(c.key())) {
                            errors.add(
                                    message
                                            + "; this artifact is part of the driver API and must be "
                                            + "provided by the benchmark (declare it with provided scope)");
                        } else {
                            warnings.add(message + "; using the core version");
                        }
                    }
                    continue;
                }
                ArtifactCoordinates other = bundleArtifacts.putIfAbsent(c.variantKey(), c);
                if (other != null) {
                    if (!other.version().equals(c.version()) && ANNOTATION_ARTIFACTS.contains(c.key())) {
                        boolean newer = compareVersions(c.version(), other.version()) > 0;
                        ArtifactCoordinates chosen = newer ? c : other;
                        warnings.add(
                                "Plugins '"
                                        + bundleOwners.get(c.variantKey())
                                        + "' and '"
                                        + bundle.id()
                                        + "' bundle "
                                        + other
                                        + " and "
                                        + c
                                        + "; using annotation artifact "
                                        + chosen);
                        if (newer) {
                            result.remove(bundlePaths.get(c.variantKey()));
                            result.add(path);
                            bundleArtifacts.put(c.variantKey(), c);
                            bundlePaths.put(c.variantKey(), path);
                            bundleOwners.put(c.variantKey(), bundle.id());
                        }
                    } else if (!other.version().equals(c.version())) {
                        errors.add(
                                "Plugins '"
                                        + bundleOwners.get(c.variantKey())
                                        + "' and '"
                                        + bundle.id()
                                        + "' run in the same process but bundle "
                                        + other
                                        + " and "
                                        + c);
                    }
                    continue;
                }
                bundleOwners.put(c.variantKey(), bundle.id());
                bundlePaths.put(c.variantKey(), path);
                result.add(path);
            }
        }
        if (!errors.isEmpty()) {
            throw new PluginException(
                    "Cannot build the classpath for "
                            + driver.driverClass()
                            + ":\n  - "
                            + String.join("\n  - ", errors));
        }
        return new RuntimeClasspath(new ArrayList<>(result), warnings);
    }

    static int compareVersions(String left, String right) {
        return new ComparableVersion(left).compareTo(new ComparableVersion(right));
    }
}
