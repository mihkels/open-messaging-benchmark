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
package io.github.mihkels.omb;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Module-boundary rules for {@code benchmark-framework}.
 *
 * <p>The framework talks to drivers only through {@code driver-api} and loads them in an isolated
 * driver process, so it must never depend on a concrete driver package.
 */
@AnalyzeClasses(
        packages = "io.github.mihkels.omb",
        importOptions = {
            ImportOption.DoNotIncludeTests.class,
            FrameworkArchitectureTest.DoNotIncludeShaded.class
        })
class FrameworkArchitectureTest {

    /**
     * Third-party code that metrics-otlp relocates under {@code io.github.mihkels.omb} is not ours to
     * check.
     */
    static final class DoNotIncludeShaded implements ImportOption {
        @Override
        public boolean includes(Location location) {
            return !location.contains("/metrics/otlp/shaded/");
        }
    }

    private static final String DRIVER_API_PACKAGE = "io.github.mihkels.omb.driver";

    @ArchTest
    static final ArchRule FRAMEWORK_DOES_NOT_DEPEND_ON_CONCRETE_DRIVERS =
            noClasses()
                    .that()
                    .resideOutsideOfPackage(DRIVER_API_PACKAGE + "..")
                    .should()
                    .dependOnClassesThat(
                            resideInAPackage(DRIVER_API_PACKAGE + "..")
                                    .and(not(resideInAPackage(DRIVER_API_PACKAGE))))
                    .because("drivers are loaded only through driver-api in an isolated driver process");

    @ArchTest
    static final ArchRule FRAMEWORK_HAS_NO_PACKAGE_CYCLES =
            slices().matching("io.github.mihkels.(**)").should().beFreeOfCycles();
}
