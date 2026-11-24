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
package io.github.mihkels.omb.arch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.lang.ArchRule;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.ClassForNameFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.ClassForNameReferenceFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.InstanceLoggerFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.Jackson2ModuleFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.JulLoggerFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.MutableLoggerFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.PrintStackTraceFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.PrintStackTraceReferenceFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.PublicLoggerFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.RuntimeExitFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.RuntimeHaltFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.RuntimeHaltReferenceFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.SystemErrFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.SystemExitFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.SystemExitReferenceFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.SystemOutFixture;
import io.github.mihkels.omb.arch.fixtures.violations.ViolationFixtures.TestLibraryFixture;
import java.net.URI;
import org.junit.jupiter.api.Test;

class CommonArchitectureRulesTest {

    private static final ClassFileImporter IMPORTER = new ClassFileImporter();

    @Test
    void cleanPackagePassesAllCommonRules() {
        JavaClasses clean = IMPORTER.importPackages("io.github.mihkels.omb.arch.fixtures.clean");

        CommonArchitectureRules.EVERY_PRODUCTION_PACKAGE_IS_NULL_MARKED.check(clean);
        CommonArchitectureRules.SLF4J_LOGGING_ONLY.check(clean);
        CommonArchitectureRules.JACKSON_3_ONLY.check(clean);
        CommonArchitectureRules.NO_CLASS_FOR_NAME_OUTSIDE_CONFIGURED_CLASSES.check(clean);
        CommonArchitectureRules.NO_TEST_LIBRARIES_IN_PRODUCTION_CODE.check(clean);
        CommonArchitectureRules.LOGGERS_SHOULD_BE_PRIVATE_STATIC_FINAL.check(clean);
        CommonArchitectureRules.NO_GUAVA_OR_APACHE_COMMONS.check(clean);
        CommonArchitectureRules.noConsoleOrProcessExit().check(clean);
    }

    @Test
    void unmarkedPackageFailsNullMarkedRule() {
        JavaClasses unmarked = IMPORTER.importPackages("io.github.mihkels.omb.arch.fixtures.unmarked");

        assertThatThrownBy(
                        () -> CommonArchitectureRules.EVERY_PRODUCTION_PACKAGE_IS_NULL_MARKED.check(unmarked))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("@NullMarked");
    }

    @Test
    void julLoggingFailsSlf4jOnlyRule() {
        JavaClasses classes = IMPORTER.importClasses(JulLoggerFixture.class);

        assertThatThrownBy(() -> CommonArchitectureRules.SLF4J_LOGGING_ONLY.check(classes))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("java.util.logging");
    }

    @Test
    void jackson2TypeOutsideAnnotationsFailsJackson3OnlyRule() {
        JavaClasses classes = IMPORTER.importClasses(Jackson2ModuleFixture.class);

        assertThatThrownBy(() -> CommonArchitectureRules.JACKSON_3_ONLY.check(classes))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("com.fasterxml.jackson.module");
    }

    @Test
    void classForNameCallOrReferenceFailsConfiguredClassesRule() {
        for (Class<?> fixture :
                new Class<?>[] {ClassForNameFixture.class, ClassForNameReferenceFixture.class}) {
            JavaClasses classes = IMPORTER.importClasses(fixture);

            assertThatThrownBy(() ->
                            CommonArchitectureRules.NO_CLASS_FOR_NAME_OUTSIDE_CONFIGURED_CLASSES.check(classes))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("ConfiguredClasses.load");
        }
    }

    @Test
    void testDependencyInProductionFailsRule() {
        JavaClasses classes = IMPORTER.importClasses(TestLibraryFixture.class);

        assertThatThrownBy(
                        () -> CommonArchitectureRules.NO_TEST_LIBRARIES_IN_PRODUCTION_CODE.check(classes))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("org.junit");
    }

    @Test
    void nonPrivateStaticFinalLoggersFailRule() {
        for (Class<?> fixture :
                new Class<?>[] {
                    PublicLoggerFixture.class, InstanceLoggerFixture.class, MutableLoggerFixture.class
                }) {
            JavaClasses classes = IMPORTER.importClasses(fixture);
            assertThatThrownBy(
                            () -> CommonArchitectureRules.LOGGERS_SHOULD_BE_PRIVATE_STATIC_FINAL.check(classes))
                    .isInstanceOf(AssertionError.class);
        }
    }

    @Test
    void consoleAndProcessExitFailWhenNotAllowlisted() {
        ArchRule rule = CommonArchitectureRules.noConsoleOrProcessExit();
        for (Class<?> fixture :
                new Class<?>[] {
                    SystemOutFixture.class,
                    SystemErrFixture.class,
                    PrintStackTraceFixture.class,
                    SystemExitFixture.class,
                    RuntimeExitFixture.class,
                    RuntimeHaltFixture.class,
                    SystemExitReferenceFixture.class,
                    RuntimeHaltReferenceFixture.class,
                    PrintStackTraceReferenceFixture.class
                }) {
            JavaClasses classes = IMPORTER.importClasses(fixture);
            assertThatThrownBy(() -> rule.check(classes)).isInstanceOf(AssertionError.class);
        }
    }

    @Test
    void allowlistedEntryPointAndNestedClassPassConsoleAndExitRule() {
        JavaClasses allowed =
                IMPORTER.importClasses(SystemExitFixture.class, SystemExitFixture.NestedExit.class);
        ArchRule rule =
                CommonArchitectureRules.noConsoleOrProcessExitOutside(SystemExitFixture.class.getName());

        rule.check(allowed);

        JavaClasses disallowed = IMPORTER.importClasses(SystemOutFixture.class);
        assertThatThrownBy(() -> rule.check(disallowed)).isInstanceOf(AssertionError.class);
    }

    @Test
    void doNotIncludeArchRulesFiltersOnlyArchPackage() {
        CommonArchitectureRules.DoNotIncludeArchRules filter =
                new CommonArchitectureRules.DoNotIncludeArchRules();
        Location archClass =
                Location.of(
                        URI.create(
                                "file:///target/classes/io/github/mihkels/omb/arch/CommonArchitectureRules.class"));
        Location frameworkClass =
                Location.of(URI.create("file:///target/classes/io/github/mihkels/omb/Benchmark.class"));

        assertThat(filter.includes(archClass)).isFalse();
        assertThat(filter.includes(frameworkClass)).isTrue();
    }
}
