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

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Discovers driver plugin bundles and maps driver classes to the bundles that provide them.
 *
 * <p>Bundles are searched in:
 *
 * <ul>
 *   <li>{@code <home>/drivers/*} — the binary distribution layout;
 *   <li>{@code <home>/*}{@code /target/omb-plugin} — bundles built in a source checkout;
 *   <li>every entry of the {@value #PLUGIN_PATH_PROPERTY} system property or {@value
 *       #PLUGIN_PATH_ENV} environment variable, each either a bundle directory or a directory of
 *       bundles.
 * </ul>
 *
 * <p>The home directory is taken from the {@value #HOME_PROPERTY} system property, the {@value
 * #HOME_ENV} environment variable or the working directory, in this order.
 */
public final class PluginRegistry {

    public static final String HOME_PROPERTY = "omb.home";
    public static final String HOME_ENV = "OMB_HOME";
    public static final String PLUGIN_PATH_PROPERTY = "omb.plugin.path";
    public static final String PLUGIN_PATH_ENV = "OMB_PLUGIN_PATH";

    private final List<PluginBundle> bundles;
    private final Map<String, PluginBundle> byDriverClass;
    private final List<String> errors;
    private final Map<String, List<String>> errorsByDriverClass;
    private final List<Path> searched;

    private PluginRegistry(
            List<PluginBundle> bundles,
            Map<String, PluginBundle> byDriverClass,
            List<String> errors,
            Map<String, List<String>> errorsByDriverClass,
            List<Path> searched) {
        this.bundles = List.copyOf(bundles);
        this.byDriverClass = Map.copyOf(byDriverClass);
        this.errors = List.copyOf(errors);
        Map<String, List<String>> copiedErrors = new LinkedHashMap<>();
        errorsByDriverClass.forEach(
                (driver, problems) -> copiedErrors.put(driver, List.copyOf(problems)));
        this.errorsByDriverClass = Map.copyOf(copiedErrors);
        this.searched = List.copyOf(searched);
    }

    /**
     * Discovers bundles using the configured home directory and plugin path.
     *
     * @return the discovered bundles
     */
    public static PluginRegistry discover() {
        List<Path> locations = new ArrayList<>();
        Path home = home();
        locations.add(home.resolve("drivers"));
        try (DirectoryStream<Path> modules = Files.newDirectoryStream(home, Files::isDirectory)) {
            List<Path> sorted = new ArrayList<>();
            modules.forEach(m -> sorted.add(m.resolve("target").resolve("omb-plugin")));
            sorted.sort(null);
            locations.addAll(sorted);
        } catch (IOException e) {
            throw new PluginException("Cannot list benchmark home " + home, e);
        }
        String extra = System.getProperty(PLUGIN_PATH_PROPERTY, System.getenv(PLUGIN_PATH_ENV));
        if (extra != null) {
            for (String entry : extra.split(File.pathSeparator)) {
                if (!entry.isBlank()) {
                    locations.add(home.resolve(entry.trim()));
                }
            }
        }
        return discover(locations);
    }

    /**
     * Discovers bundles in the given locations.
     *
     * @param locations bundle directories or directories of bundles
     * @return the discovered bundles
     */
    public static PluginRegistry discover(List<Path> locations) {
        Set<Path> bundleDirs = new LinkedHashSet<>();
        for (Path location : locations) {
            if (Files.isRegularFile(location.resolve(PluginBundle.DESCRIPTOR_FILE))) {
                bundleDirs.add(realPath(location));
            } else if (Files.isDirectory(location)) {
                List<Path> children = new ArrayList<>();
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(location)) {
                    for (Path child : stream) {
                        if (Files.isRegularFile(child.resolve(PluginBundle.DESCRIPTOR_FILE))) {
                            children.add(realPath(child));
                        }
                    }
                } catch (IOException e) {
                    throw new PluginException("Cannot list plugin location " + location, e);
                }
                children.sort(null);
                bundleDirs.addAll(children);
            }
        }

        List<String> errors = new ArrayList<>();
        List<PluginBundle> bundles = new ArrayList<>();
        Map<String, PluginBundle> byId = new LinkedHashMap<>();
        Map<String, PluginBundle> byDriverClass = new LinkedHashMap<>();
        Map<String, List<String>> errorsByDriverClass = new LinkedHashMap<>();
        for (Path dir : bundleDirs) {
            PluginBundle bundle;
            try {
                bundle = PluginBundle.load(dir);
            } catch (PluginException e) {
                errors.add(e.getMessage());
                continue;
            }
            PluginBundle sameId = byId.putIfAbsent(bundle.id(), bundle);
            if (sameId != null) {
                String error =
                        "Duplicate plugin id '"
                                + bundle.id()
                                + "' in "
                                + sameId.directory()
                                + " and "
                                + bundle.directory();
                errors.add(error);
                sameId
                        .descriptor()
                        .drivers()
                        .forEach(driver -> addDriverError(errorsByDriverClass, driver, error));
                bundle
                        .descriptor()
                        .drivers()
                        .forEach(driver -> addDriverError(errorsByDriverClass, driver, error));
                continue;
            }
            for (String driver : bundle.descriptor().drivers()) {
                PluginBundle other = byDriverClass.putIfAbsent(driver, bundle);
                if (other != null) {
                    String error =
                            "Driver class "
                                    + driver
                                    + " is provided by both '"
                                    + other.id()
                                    + "' ("
                                    + other.directory()
                                    + ") and '"
                                    + bundle.id()
                                    + "' ("
                                    + bundle.directory()
                                    + ")";
                    errors.add(error);
                    addDriverError(errorsByDriverClass, driver, error);
                }
            }
            bundles.add(bundle);
        }
        return new PluginRegistry(bundles, byDriverClass, errors, errorsByDriverClass, locations);
    }

    private static void addDriverError(
            Map<String, List<String>> errorsByDriverClass, String driver, String error) {
        errorsByDriverClass.computeIfAbsent(driver, unused -> new ArrayList<>()).add(error);
    }

    public static Path home() {
        String home = System.getProperty(HOME_PROPERTY, System.getenv(HOME_ENV));
        Path path = home == null || home.isBlank() ? Path.of("") : Path.of(home);
        return path.toAbsolutePath().normalize();
    }

    public List<PluginBundle> bundles() {
        return bundles;
    }

    public List<String> errors() {
        return errors;
    }

    /**
     * Resolves the bundles needed by {@code driverClass}, following class references declared by the
     * bundles in {@code driverConfig}.
     *
     * @param driverClass the driver class selected by the driver configuration
     * @param driverConfig the parsed driver configuration
     * @return the bundles to load, primary bundle first
     */
    public ResolvedDriver resolve(String driverClass, Map<String, Object> driverConfig) {
        failOnDriverErrors(driverClass);
        PluginBundle primary = byDriverClass.get(driverClass);
        if (primary == null) {
            throw new PluginException(
                    "No driver plugin bundle provides driver class "
                            + driverClass
                            + ". Known driver classes: "
                            + new TreeSet<>(byDriverClass.keySet())
                            + ". Searched: "
                            + searched
                            + invalidBundleDetails());
        }
        return new ResolvedDriver(driverClass, withReferences(primary, driverConfig));
    }

    /**
     * Resolves {@code driverClass} using an explicitly given bundle directory or jar, as selected
     * with {@code --legacy-driver-name}. A directory without {@code plugin.yaml} is treated as an
     * unpackaged driver: {@code target/classes}, {@code *.jar} and {@code lib/*.jar} are loaded.
     *
     * @param driverClass the driver class selected by the driver configuration
     * @param driverConfig the parsed driver configuration
     * @param location the bundle directory or jar to load the driver from
     * @return the bundles to load, primary bundle first
     */
    public ResolvedDriver resolveOverride(
            String driverClass, Map<String, Object> driverConfig, Path location) {
        PluginBundle primary = legacyBundle(driverClass, location);
        List<PluginBundle> result = new ArrayList<>(List.of(primary));
        for (PluginBundle referenced : withReferences(primary, driverConfig)) {
            if (referenced != primary) {
                result.add(referenced);
            }
        }
        return new ResolvedDriver(driverClass, result);
    }

    private List<PluginBundle> withReferences(PluginBundle primary, Map<String, Object> config) {
        List<PluginBundle> result = new ArrayList<>();
        result.add(primary);
        for (int i = 0; i < result.size(); i++) {
            for (String property : result.get(i).descriptor().classReferenceProperties()) {
                Object value = config == null ? null : config.get(property);
                if (value == null || value.toString().isBlank()) {
                    continue;
                }
                String referencedClass = value.toString().trim();
                failOnDriverErrors(referencedClass);
                PluginBundle referenced = byDriverClass.get(referencedClass);
                if (referenced == null) {
                    throw new PluginException(
                            "Driver property '"
                                    + property
                                    + "' references "
                                    + referencedClass
                                    + " but no driver plugin bundle provides it. Known driver classes: "
                                    + new TreeSet<>(byDriverClass.keySet())
                                    + invalidBundleDetails());
                }
                if (!result.contains(referenced)) {
                    result.add(referenced);
                }
            }
        }
        return result;
    }

    private void failOnDriverErrors(String driverClass) {
        List<String> problems = errorsByDriverClass.get(driverClass);
        if (problems != null && !problems.isEmpty()) {
            throw new PluginException(
                    "Invalid driver plugin bundles for "
                            + driverClass
                            + ":\n  - "
                            + String.join("\n  - ", problems));
        }
    }

    private String invalidBundleDetails() {
        return errors.isEmpty() ? "" : ". Invalid bundles:\n  - " + String.join("\n  - ", errors);
    }

    static PluginBundle legacyBundle(String driverClass, Path location) {
        Path path = location.toAbsolutePath().normalize();
        if (Files.isRegularFile(path.resolve(PluginBundle.DESCRIPTOR_FILE))) {
            PluginBundle bundle = PluginBundle.load(path);
            if (!bundle.descriptor().drivers().contains(driverClass)) {
                throw new PluginException(
                        "Plugin bundle "
                                + path
                                + " does not provide "
                                + driverClass
                                + ", it provides "
                                + bundle.descriptor().drivers());
            }
            return bundle;
        }
        List<String> classpath;
        Path dir;
        String fileName = String.valueOf(path.getFileName());
        if (Files.isRegularFile(path) && fileName.endsWith(".jar")) {
            dir = path.getParent();
            classpath = List.of(fileName);
        } else if (Files.isDirectory(path)) {
            dir = path;
            classpath = List.of();
        } else {
            throw new PluginException("Driver location " + path + " does not exist");
        }
        PluginDescriptor descriptor =
                new PluginDescriptor(
                        PluginDescriptor.CURRENT_DESCRIPTOR_VERSION,
                        "override-" + Integer.toHexString(path.hashCode()),
                        null,
                        io.openmessaging.benchmark.driver.DriverApiVersion.CURRENT,
                        null,
                        List.of(driverClass),
                        classpath,
                        classpath.isEmpty() ? List.of("target/classes", "*.jar", "lib/*.jar") : List.of(),
                        List.of(),
                        "Driver loaded from " + path);
        PluginBundle bundle = new PluginBundle(dir, descriptor);
        try {
            bundle.classpath();
        } catch (PluginException e) {
            throw new PluginException(
                    "Driver location "
                            + path
                            + " contains no plugin.yaml, target/classes, *.jar or lib/*.jar",
                    e);
        }
        return bundle;
    }

    private static Path realPath(Path path) {
        try {
            return path.toRealPath();
        } catch (IOException e) {
            return path.toAbsolutePath().normalize();
        }
    }
}
