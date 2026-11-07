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

import static org.assertj.core.api.Assertions.assertThat;

import io.openmessaging.benchmark.driver.DriverApiVersion;
import java.util.List;
import org.junit.jupiter.api.Test;

class PluginDescriptorTest {
    @Test
    void acceptsAValidDescriptor() {
        assertThat(validDescriptor().validate()).isEmpty();
    }

    @Test
    void validatesLongAndUnicodeClassNamesWithoutBacktracking() {
        List<String> drivers =
                List.of("Driver", "α.类$Inner_1", "p.".repeat(10_000) + "Driver", "p." + "D".repeat(20_000));
        for (String driver : drivers) {
            assertThat(
                            descriptor(
                                            1,
                                            "plugin",
                                            DriverApiVersion.CURRENT,
                                            List.of(driver),
                                            List.of("driver.jar"),
                                            List.of(),
                                            List.of())
                                    .validate())
                    .isEmpty();
            for (String suffix : List.of(".", "!")) {
                assertThat(
                                descriptor(
                                                1,
                                                "plugin",
                                                DriverApiVersion.CURRENT,
                                                List.of(driver + suffix),
                                                List.of("driver.jar"),
                                                List.of(),
                                                List.of())
                                        .validate())
                        .containsExactly("invalid driver class name '" + driver + suffix + "'");
            }
        }
    }

    @Test
    void reportsInvalidDescriptorFields() {
        List<InvalidDescriptor> invalid =
                List.of(
                        new InvalidDescriptor(
                                descriptor(
                                        2,
                                        "plugin",
                                        DriverApiVersion.CURRENT,
                                        List.of("test.Driver"),
                                        List.of("driver.jar"),
                                        List.of(),
                                        List.of()),
                                "unsupported descriptorVersion 2"),
                        new InvalidDescriptor(
                                descriptor(
                                        1,
                                        "../plugin",
                                        DriverApiVersion.CURRENT,
                                        List.of("test.Driver"),
                                        List.of("driver.jar"),
                                        List.of(),
                                        List.of()),
                                "id '../plugin'"),
                        new InvalidDescriptor(
                                descriptor(
                                        1,
                                        "plugin",
                                        DriverApiVersion.CURRENT + 1,
                                        List.of("test.Driver"),
                                        List.of("driver.jar"),
                                        List.of(),
                                        List.of()),
                                "driverApiVersion " + (DriverApiVersion.CURRENT + 1)),
                        new InvalidDescriptor(
                                descriptor(
                                        1,
                                        "plugin",
                                        DriverApiVersion.CURRENT,
                                        List.of("not-a-class"),
                                        List.of("driver.jar"),
                                        List.of(),
                                        List.of()),
                                "invalid driver class name"),
                        new InvalidDescriptor(
                                descriptor(
                                        1,
                                        "plugin",
                                        DriverApiVersion.CURRENT,
                                        List.of("test.Driver"),
                                        List.of(),
                                        List.of(),
                                        List.of()),
                                "classpath must list at least one entry"),
                        new InvalidDescriptor(
                                descriptor(
                                        1,
                                        "plugin",
                                        DriverApiVersion.CURRENT,
                                        List.of("test.Driver"),
                                        List.of("../outside.jar"),
                                        List.of(),
                                        List.of()),
                                "escapes the bundle directory"),
                        new InvalidDescriptor(
                                descriptor(
                                        1,
                                        "plugin",
                                        DriverApiVersion.CURRENT,
                                        List.of("test.Driver"),
                                        List.of("/absolute/driver.jar"),
                                        List.of(),
                                        List.of()),
                                "must be relative to the bundle directory"),
                        new InvalidDescriptor(
                                new PluginDescriptor(
                                        1,
                                        "plugin",
                                        "1.0",
                                        DriverApiVersion.CURRENT,
                                        Runtime.version().feature() + 1,
                                        List.of("test.Driver"),
                                        List.of("driver.jar"),
                                        List.of(),
                                        List.of(),
                                        null),
                                "requires Java"),
                        new InvalidDescriptor(
                                descriptor(
                                        1,
                                        "plugin",
                                        DriverApiVersion.CURRENT,
                                        List.of("test.Driver"),
                                        List.of("lib/*.jar"),
                                        List.of(),
                                        List.of(" ")),
                                "classReferenceProperties must not contain blank names"));

        for (InvalidDescriptor testCase : invalid) {
            assertThat(testCase.descriptor().validate())
                    .as("descriptor should report %s", testCase.expectedError())
                    .anySatisfy(error -> assertThat(error).contains(testCase.expectedError()));
        }
    }

    @Test
    void rejectsBundlesBuiltForAnOlderDriverApi() {
        PluginDescriptor descriptor =
                descriptor(
                        1,
                        "plugin",
                        DriverApiVersion.CURRENT - 1,
                        List.of("test.Driver"),
                        List.of("driver.jar"),
                        List.of(),
                        List.of());

        assertThat(descriptor.validate())
                .anySatisfy(error ->
                        assertThat(error)
                                .contains("bundle targets driverApiVersion " + (DriverApiVersion.CURRENT - 1)));
    }

    @Test
    void reportsMissingRequiredFieldsAsErrors() {
        PluginDescriptor descriptor =
                new PluginDescriptor(null, null, null, null, null, null, null, null, null, null);

        assertThat(descriptor.drivers()).isEmpty();
        assertThat(descriptor.classpath()).isEmpty();
        assertThat(descriptor.optionalClasspath()).isEmpty();
        assertThat(descriptor.classReferenceProperties()).isEmpty();
        assertThat(descriptor.validate())
                .anySatisfy(error -> assertThat(error).contains("unsupported descriptorVersion null"))
                .anySatisfy(error -> assertThat(error).contains("id 'null'"))
                .anySatisfy(error -> assertThat(error).contains("driverApiVersion null"))
                .anySatisfy(error -> assertThat(error).contains("drivers must list at least one"))
                .anySatisfy(error -> assertThat(error).contains("classpath must list at least one"));
    }

    @Test
    void acceptsBundlesForTheRuntimeJavaVersion() {
        PluginDescriptor descriptor =
                new PluginDescriptor(
                        1,
                        "plugin",
                        "1.0",
                        DriverApiVersion.CURRENT,
                        Runtime.version().feature(),
                        List.of("test.Driver"),
                        List.of("driver.jar"),
                        List.of("lib/*.jar"),
                        List.of("driverClass"),
                        "desc");

        assertThat(descriptor.validate()).isEmpty();
    }

    @Test
    void reportsInvalidClasspathEntries() {
        assertThat(classpathErrors(" ")).anySatisfy(e -> assertThat(e).contains("blank entries"));
        assertThat(classpathErrors("\\driver.jar"))
                .anySatisfy(e -> assertThat(e).contains("must be relative"));
        assertThat(classpathErrors("."))
                .anySatisfy(e -> assertThat(e).contains("escapes the bundle directory"));
        assertThat(classpathErrors("lib*/driver.jar"))
                .anySatisfy(e -> assertThat(e).contains("wildcards in the file name"));
        assertThat(classpathErrors("lib?/driver.jar"))
                .anySatisfy(e -> assertThat(e).contains("wildcards in the file name"));
        assertThat(classpathErrors("lib/driver.jar")).isEmpty();
    }

    @Test
    void reportsInvalidOptionalClasspathEntries() {
        PluginDescriptor descriptor =
                descriptor(
                        1,
                        "plugin",
                        DriverApiVersion.CURRENT,
                        List.of("test.Driver"),
                        List.of("driver.jar"),
                        List.of(""),
                        List.of());

        assertThat(descriptor.validate())
                .containsExactly("optionalClasspath must not contain blank entries");
    }

    private static List<String> classpathErrors(String entry) {
        return descriptor(
                        1,
                        "plugin",
                        DriverApiVersion.CURRENT,
                        List.of("test.Driver"),
                        List.of(entry),
                        List.of(),
                        List.of())
                .validate();
    }

    private static PluginDescriptor validDescriptor() {
        return descriptor(
                1,
                "plugin",
                DriverApiVersion.CURRENT,
                List.of("test.Driver"),
                List.of("driver.jar"),
                List.of(),
                List.of());
    }

    private static PluginDescriptor descriptor(
            Integer descriptorVersion,
            String id,
            Integer driverApiVersion,
            List<String> drivers,
            List<String> classpath,
            List<String> optionalClasspath,
            List<String> classReferenceProperties) {
        return new PluginDescriptor(
                descriptorVersion,
                id,
                "1.0",
                driverApiVersion,
                null,
                drivers,
                classpath,
                optionalClasspath,
                classReferenceProperties,
                null);
    }

    private record InvalidDescriptor(PluginDescriptor descriptor, String expectedError) {}
}
