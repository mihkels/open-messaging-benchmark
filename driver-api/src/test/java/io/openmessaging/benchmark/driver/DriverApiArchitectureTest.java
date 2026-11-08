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
package io.openmessaging.benchmark.driver;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * Module-boundary rules for {@code driver-api}.
 *
 * <p>The driver API is the contract between the benchmark framework and every driver. It must stay
 * small: it may use the JDK, its own package and the libraries it exposes in its API (Jackson,
 * Micrometer and SLF4J). It must not reach into framework, worker or tool packages, concrete driver
 * packages or broker client libraries.
 */
@AnalyzeClasses(
        packages = "io.openmessaging.benchmark.driver",
        importOptions = ImportOption.DoNotIncludeTests.class)
class DriverApiArchitectureTest {

    @ArchTest
    static final ArchRule DRIVER_API_DEPENDS_ONLY_ON_JDK_AND_DECLARED_API_LIBRARIES =
            classes()
                    .that()
                    .resideInAPackage("io.openmessaging.benchmark.driver")
                    .should()
                    .onlyDependOnClassesThat()
                    .resideInAnyPackage(
                            "java..",
                            "io.openmessaging.benchmark.driver",
                            "com.fasterxml.jackson.annotation..",
                            "tools.jackson..",
                            "io.micrometer..",
                            "org.slf4j..")
                    .because(
                            "driver-api is the contract shared by the framework and every driver,"
                                    + " so it may only use the JDK and its declared API libraries");
}
