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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openmessaging.benchmark.driver.DriverApiVersion;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Contents of a driver plugin bundle descriptor ({@code plugin.yaml}).
 *
 * @param descriptorVersion format version of this descriptor, currently {@code 1}
 * @param id unique bundle identifier
 * @param version bundle version, informational
 * @param driverApiVersion driver SPI contract the bundle was built against, see {@link
 *     DriverApiVersion}
 * @param javaVersion minimum Java feature release required by the bundle, optional
 * @param drivers fully qualified {@code BenchmarkDriver} implementation classes provided
 * @param classpath required classpath entries relative to the bundle directory; files, directories
 *     or {@code dir/*.jar} style globs
 * @param optionalClasspath classpath entries that may be absent, e.g. user supplied extensions
 * @param classReferenceProperties driver configuration properties naming other driver classes whose
 *     bundles must also be loaded into this driver's process
 * @param description free text description
 */
public record PluginDescriptor(
        @JsonProperty(value = "descriptorVersion", required = true) Integer descriptorVersion,
        @JsonProperty(value = "id", required = true) String id,
        @JsonProperty("version") String version,
        @JsonProperty(value = "driverApiVersion", required = true) Integer driverApiVersion,
        @JsonProperty("javaVersion") Integer javaVersion,
        @JsonProperty(value = "drivers", required = true) List<String> drivers,
        @JsonProperty(value = "classpath", required = true) List<String> classpath,
        @JsonProperty("optionalClasspath") List<String> optionalClasspath,
        @JsonProperty("classReferenceProperties") List<String> classReferenceProperties,
        @JsonProperty("description") String description) {

    public static final int CURRENT_DESCRIPTOR_VERSION = 1;

    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._-]+");
    private static final Pattern CLASS_NAME =
            Pattern.compile(
                    "[\\p{javaJavaIdentifierStart}][\\p{javaJavaIdentifierPart}]*"
                            + "(\\.[\\p{javaJavaIdentifierStart}][\\p{javaJavaIdentifierPart}]*)*");

    public PluginDescriptor {
        drivers = drivers == null ? List.of() : List.copyOf(drivers);
        classpath = classpath == null ? List.of() : List.copyOf(classpath);
        optionalClasspath = optionalClasspath == null ? List.of() : List.copyOf(optionalClasspath);
        classReferenceProperties =
                classReferenceProperties == null ? List.of() : List.copyOf(classReferenceProperties);
    }

    /** Returns every problem with this descriptor; an empty list means the descriptor is valid. */
    public List<String> validate() {
        List<String> errors = new ArrayList<>();
        if (descriptorVersion == null || descriptorVersion != CURRENT_DESCRIPTOR_VERSION) {
            errors.add(
                    "unsupported descriptorVersion "
                            + descriptorVersion
                            + ", expected "
                            + CURRENT_DESCRIPTOR_VERSION);
        }
        if (id == null || !ID.matcher(id).matches()) {
            errors.add("id '" + id + "' must match " + ID.pattern());
        }
        if (driverApiVersion == null || driverApiVersion != DriverApiVersion.CURRENT) {
            errors.add(
                    "bundle targets driverApiVersion "
                            + driverApiVersion
                            + " but this benchmark implements driverApiVersion "
                            + DriverApiVersion.CURRENT);
        }
        int runtimeJava = Runtime.version().feature();
        if (javaVersion != null && javaVersion > runtimeJava) {
            errors.add("bundle requires Java " + javaVersion + " but runs on Java " + runtimeJava);
        }
        if (drivers.isEmpty()) {
            errors.add("drivers must list at least one driver class");
        }
        for (String driver : drivers) {
            if (driver == null || !CLASS_NAME.matcher(driver).matches()) {
                errors.add("invalid driver class name '" + driver + "'");
            }
        }
        if (classpath.isEmpty()) {
            errors.add("classpath must list at least one entry");
        }
        validateEntries("classpath", classpath, errors);
        validateEntries("optionalClasspath", optionalClasspath, errors);
        for (String property : classReferenceProperties) {
            if (property == null || property.isBlank()) {
                errors.add("classReferenceProperties must not contain blank names");
            }
        }
        return errors;
    }

    private static void validateEntries(String field, List<String> entries, List<String> errors) {
        for (String entry : entries) {
            if (entry == null || entry.isBlank()) {
                errors.add(field + " must not contain blank entries");
                continue;
            }
            Path path = Path.of(entry.replace('\\', '/'));
            if (path.isAbsolute() || entry.startsWith("/") || entry.startsWith("\\")) {
                errors.add(field + " entry '" + entry + "' must be relative to the bundle directory");
                continue;
            }
            Path normalized = path.normalize();
            if (normalized.toString().isEmpty() || normalized.startsWith("..")) {
                errors.add(field + " entry '" + entry + "' escapes the bundle directory");
            }
            Path parent = normalized.getParent();
            if (parent != null && parent.toString().contains("*")) {
                errors.add(field + " entry '" + entry + "' may only use wildcards in the file name");
            }
        }
    }
}
