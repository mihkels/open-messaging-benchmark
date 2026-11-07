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
        Composition composition = new Composition();
        for (HostRuntime.Entry entry : host.entries()) {
            composition.addHost(entry);
        }
        for (PluginBundle bundle : driver.bundles()) {
            for (Path path : bundle.classpath()) {
                composition.addBundleJar(bundle, path);
            }
        }
        if (!composition.errors.isEmpty()) {
            throw new PluginException(
                    "Cannot build the classpath for "
                            + driver.driverClass()
                            + ":\n  - "
                            + String.join("\n  - ", composition.errors));
        }
        return new RuntimeClasspath(new ArrayList<>(composition.result), composition.warnings);
    }

    /** The classpath being composed and what is known about the artifacts already on it. */
    private static final class Composition {
        private final Set<Path> result = new LinkedHashSet<>();
        private final Map<String, ArtifactCoordinates> hostArtifacts = new HashMap<>();
        private final List<String> warnings = new ArrayList<>();
        private final List<String> errors = new ArrayList<>();
        private final Map<String, ArtifactCoordinates> bundleArtifacts = new HashMap<>();
        private final Map<String, String> bundleOwners = new HashMap<>();
        private final Map<String, Path> bundlePaths = new HashMap<>();

        void addHost(HostRuntime.Entry entry) {
            result.add(entry.path());
            hostArtifacts.put(entry.coordinates().variantKey(), entry.coordinates());
        }

        void addBundleJar(PluginBundle bundle, Path path) {
            Optional<ArtifactCoordinates> detected = ArtifactCoordinates.detect(path);
            if (detected.isEmpty()) {
                result.add(path);
                return;
            }
            ArtifactCoordinates c = detected.get();
            ArtifactCoordinates hostCopy = hostArtifacts.get(c.variantKey());
            if (hostCopy != null) {
                reconcileWithHost(bundle, c, hostCopy);
                return;
            }
            ArtifactCoordinates other = bundleArtifacts.putIfAbsent(c.variantKey(), c);
            if (other != null) {
                reconcileWithBundle(bundle, path, c, other);
                return;
            }
            bundleOwners.put(c.variantKey(), bundle.id());
            bundlePaths.put(c.variantKey(), path);
            result.add(path);
        }

        // The core's copy always wins; a version mismatch is an error for driver API artifacts.
        private void reconcileWithHost(
                PluginBundle bundle, ArtifactCoordinates c, ArtifactCoordinates hostCopy) {
            if (hostCopy.version().equals(c.version())) {
                return;
            }
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

        // Annotation artifacts resolve to the newer version; other mismatches are errors.
        private void reconcileWithBundle(
                PluginBundle bundle, Path path, ArtifactCoordinates c, ArtifactCoordinates other) {
            if (other.version().equals(c.version())) {
                return;
            }
            if (!ANNOTATION_ARTIFACTS.contains(c.key())) {
                errors.add(
                        "Plugins '"
                                + bundleOwners.get(c.variantKey())
                                + "' and '"
                                + bundle.id()
                                + "' run in the same process but bundle "
                                + other
                                + " and "
                                + c);
                return;
            }
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
        }
    }

    static int compareVersions(String left, String right) {
        return new ComparableVersion(left).compareTo(new ComparableVersion(right));
    }
}
