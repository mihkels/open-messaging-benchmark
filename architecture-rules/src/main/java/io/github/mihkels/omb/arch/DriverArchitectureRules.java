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

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaAccess.Predicates.target;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.INTERFACES;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.type;
import static com.tngtech.archunit.core.domain.properties.HasModifiers.Predicates.modifier;
import static com.tngtech.archunit.core.domain.properties.HasName.Predicates.name;
import static com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.Objects;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

/**
 * Reusable ArchUnit rules enforcing driver plugin isolation and SPI contracts across all {@code
 * driver-*} modules.
 *
 * <p>Each driver module imports only its own compiled classes via {@link
 * #importDriverModule(Class)} and runs {@link #verify(Class, String, String...)} so broker fat jars
 * are skipped, {@code driver-rocketmq} and {@code driver-rocketmq5} stay isolated even though they
 * share a package, and misplaced classes are still caught.
 */
public final class DriverArchitectureRules {

    private static final String ROOT_PACKAGE = "io.github.mihkels.omb";
    private static final String DRIVER_API_PACKAGE = ROOT_PACKAGE + ".driver";
    private static final String BENCHMARK_DRIVER_CLASS = DRIVER_API_PACKAGE + ".BenchmarkDriver";
    private static final String DRIVER_CONFIGS_CLASS = DRIVER_API_PACKAGE + ".DriverConfigs";

    private static final DescribedPredicate<JavaClass> CONCRETE_BENCHMARK_DRIVER =
            assignableTo(BENCHMARK_DRIVER_CLASS)
                    .and(not(INTERFACES))
                    .and(not(modifier(JavaModifier.ABSTRACT)));

    private DriverArchitectureRules() {}

    /** Drivers depend only on {@code driver-api}, never on benchmark-framework internals or tools. */
    public static final ArchRule NO_FRAMEWORK_DEPENDENCY =
            noClasses()
                    .should()
                    .dependOnClassesThat(
                            resideInAPackage(ROOT_PACKAGE + "..")
                                    .and(not(resideInAPackage(DRIVER_API_PACKAGE + ".."))))
                    .because(
                            "drivers run in an isolated process and may depend only on driver-api,"
                                    + " never on framework, worker, plugin, utils, metrics, or"
                                    + " tool classes");

    /**
     * Every concrete {@code BenchmarkDriver} must be public, top-level, with a public no-arg ctor.
     */
    public static final ArchRule BENCHMARK_DRIVERS_SATISFY_SPI_CONTRACT =
            classes()
                    .that(CONCRETE_BENCHMARK_DRIVER)
                    .should(satisfyDriverSpiInstantiationContract())
                    .because(
                            "LocalWorker instantiates drivers via Class.forName and a public"
                                    + " no-argument constructor");

    /** Drivers may use {@code io.micrometer.core.instrument..}, never Prometheus-specific types. */
    public static final ArchRule NO_PROMETHEUS_TYPES =
            noClasses()
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage("io.micrometer.prometheusmetrics..", "io.prometheus..")
                    .because(
                            "driver SPI v2 exposes only Micrometer MeterRegistry; Prometheus"
                                    + " exposition belongs to the benchmark host");

    /** Drivers must read YAML configs via {@code DriverConfigs.read}, not {@code YAMLMapper}. */
    public static final ArchRule NO_DIRECT_YAML_MAPPER =
            noClasses()
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage("tools.jackson.dataformat.yaml..")
                    .because("drivers must load configuration YAML through DriverConfigs.read");

    /**
     * Every concrete {@code BenchmarkDriver} must load configuration via {@code DriverConfigs.read}.
     */
    public static final ArchRule BENCHMARK_DRIVERS_USE_DRIVER_CONFIGS =
            classes()
                    .that(CONCRETE_BENCHMARK_DRIVER)
                    .should()
                    .accessTargetWhere(target(owner(name(DRIVER_CONFIGS_CLASS))).and(target(name("read"))))
                    .because(
                            "drivers must load configuration YAML through DriverConfigs.read"
                                    + " instead of instantiating YAMLMapper directly");

    /** Drivers must not spawn subprocesses or call {@code Runtime.exec}. */
    public static final ArchRule NO_PROCESS_EXECUTION =
            noClasses()
                    .should()
                    .dependOnClassesThat()
                    .haveFullyQualifiedName(ProcessBuilder.class.getName())
                    .orShould()
                    .accessTargetWhere(target(owner(type(Runtime.class))).and(target(name("exec"))))
                    .because("driver plugins must not spawn external processes");

    /**
     * Imports only the production classes of the module that owns {@code driverClass}.
     *
     * @param driverClass the module's {@code BenchmarkDriver} class
     * @return the imported {@link JavaClasses} from that module's classes directory or jar
     */
    public static JavaClasses importDriverModule(Class<?> driverClass) {
        CodeSource codeSource =
                Objects.requireNonNull(
                        driverClass.getProtectionDomain().getCodeSource(),
                        "Missing CodeSource for " + driverClass.getName());
        URL location =
                Objects.requireNonNull(
                        codeSource.getLocation(), "Missing CodeSource location for " + driverClass.getName());
        try {
            Path classesRoot = Path.of(location.toURI());
            return new ClassFileImporter().importPath(classesRoot);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(
                    "Invalid CodeSource URI for " + driverClass.getName() + ": " + location, e);
        }
    }

    /**
     * Returns dynamic tests enforcing all driver isolation, SPI, and shared invariants for the module
     * owning {@code driverClass}.
     *
     * @param driverClass the module's {@code BenchmarkDriver} class
     * @param driverPackage base package of the driver (for example {@code
     *     io.github.mihkels.omb.driver.kafka})
     * @param allowedSiblingDriverPackages optional sibling driver packages allowed for this driver
     * @return stream of dynamic JUnit tests
     */
    public static Stream<DynamicTest> verify(
            Class<?> driverClass, String driverPackage, String... allowedSiblingDriverPackages) {
        return verify(importDriverModule(driverClass), driverPackage, allowedSiblingDriverPackages);
    }

    /**
     * Returns dynamic tests enforcing all driver isolation, SPI, and shared invariants against the
     * provided {@link JavaClasses}.
     *
     * @param classes imported classes of a single driver module
     * @param driverPackage base package of the driver
     * @param allowedSiblingDriverPackages optional sibling driver packages allowed for this driver
     * @return stream of dynamic JUnit tests
     */
    public static Stream<DynamicTest> verify(
            JavaClasses classes, String driverPackage, String... allowedSiblingDriverPackages) {
        validateDriverPackage(driverPackage);
        return Stream.of(
                ruleTest("NO_FRAMEWORK_DEPENDENCY", NO_FRAMEWORK_DEPENDENCY, classes),
                ruleTest(
                        "NO_CROSS_DRIVER_DEPENDENCIES",
                        noCrossDriverDependencies(driverPackage, allowedSiblingDriverPackages),
                        classes),
                ruleTest("RESIDE_IN_DRIVER_PACKAGE", resideInDriverPackage(driverPackage), classes),
                ruleTest(
                        "BENCHMARK_DRIVERS_SATISFY_SPI_CONTRACT",
                        BENCHMARK_DRIVERS_SATISFY_SPI_CONTRACT,
                        classes),
                ruleTest("NO_PROMETHEUS_TYPES", NO_PROMETHEUS_TYPES, classes),
                ruleTest("NO_DIRECT_YAML_MAPPER", NO_DIRECT_YAML_MAPPER, classes),
                ruleTest(
                        "BENCHMARK_DRIVERS_USE_DRIVER_CONFIGS", BENCHMARK_DRIVERS_USE_DRIVER_CONFIGS, classes),
                ruleTest("NO_PROCESS_EXECUTION", NO_PROCESS_EXECUTION, classes),
                ruleTest("NO_INTERNAL_PACKAGE_CYCLES", noInternalPackageCycles(driverPackage), classes),
                ruleTest(
                        "EVERY_PRODUCTION_PACKAGE_IS_NULL_MARKED",
                        CommonArchitectureRules.EVERY_PRODUCTION_PACKAGE_IS_NULL_MARKED,
                        classes),
                ruleTest("SLF4J_LOGGING_ONLY", CommonArchitectureRules.SLF4J_LOGGING_ONLY, classes),
                ruleTest("JACKSON_3_ONLY", CommonArchitectureRules.JACKSON_3_ONLY, classes),
                ruleTest(
                        "NO_CLASS_FOR_NAME_OUTSIDE_CONFIGURED_CLASSES",
                        CommonArchitectureRules.NO_CLASS_FOR_NAME_OUTSIDE_CONFIGURED_CLASSES,
                        classes),
                ruleTest(
                        "NO_TEST_LIBRARIES_IN_PRODUCTION_CODE",
                        CommonArchitectureRules.NO_TEST_LIBRARIES_IN_PRODUCTION_CODE,
                        classes),
                ruleTest(
                        "LOGGERS_SHOULD_BE_PRIVATE_STATIC_FINAL",
                        CommonArchitectureRules.LOGGERS_SHOULD_BE_PRIVATE_STATIC_FINAL,
                        classes),
                ruleTest(
                        "NO_CONSOLE_OR_PROCESS_EXIT",
                        CommonArchitectureRules.noConsoleOrProcessExit(),
                        classes));
    }

    /**
     * Builds a rule forbidding dependencies on sibling driver packages outside {@code driver-api},
     * {@code driverPackage}, and any explicitly allowlisted sibling packages.
     *
     * @param driverPackage base package of the driver under test
     * @param allowedSiblingDriverPackages sibling driver packages explicitly allowed (such as for
     *     KoP)
     * @return the ArchUnit rule
     */
    public static ArchRule noCrossDriverDependencies(
            String driverPackage, String... allowedSiblingDriverPackages) {
        validateDriverPackage(driverPackage);
        DescribedPredicate<JavaClass> allowedDriverPackages =
                resideInAPackage(DRIVER_API_PACKAGE).or(resideInAPackage(driverPackage + ".."));
        for (String sibling : allowedSiblingDriverPackages) {
            validateDriverPackage(sibling);
            allowedDriverPackages = allowedDriverPackages.or(resideInAPackage(sibling + ".."));
        }
        return noClasses()
                .should()
                .dependOnClassesThat(
                        resideInAPackage(DRIVER_API_PACKAGE + "..").and(not(allowedDriverPackages)))
                .because(
                        "drivers must remain independent of other drivers unless explicitly"
                                + " allowlisted (such as driver-kop)");
    }

    /**
     * Builds a rule requiring all classes in the module to reside in {@code driverPackage} or its
     * subpackages, preventing split-package collisions with {@code driver-api}.
     *
     * @param driverPackage base package of the driver under test
     * @return the ArchUnit rule
     */
    public static ArchRule resideInDriverPackage(String driverPackage) {
        validateDriverPackage(driverPackage);
        return classes()
                .should()
                .resideInAPackage(driverPackage + "..")
                .because(
                        "driver classes must live in their own driver subpackage, never directly in"
                                + " io.github.mihkels.omb.driver");
    }

    /**
     * Builds a rule checking that packages inside the driver module are free of dependency cycles.
     *
     * @param driverPackage base package of the driver under test
     * @return the ArchUnit rule
     */
    public static ArchRule noInternalPackageCycles(String driverPackage) {
        validateDriverPackage(driverPackage);
        String parentPackage = driverPackage.substring(0, driverPackage.lastIndexOf('.'));
        return slices()
                .matching(parentPackage + ".(**)")
                .should()
                .beFreeOfCycles()
                .because("multi-package drivers must keep their subpackages cycle-free");
    }

    private static DynamicTest ruleTest(String name, ArchRule rule, JavaClasses classes) {
        return DynamicTest.dynamicTest(name, () -> rule.check(classes));
    }

    private static void validateDriverPackage(String driverPackage) {
        if (!driverPackage.startsWith(DRIVER_API_PACKAGE + ".")
                || driverPackage.length() <= DRIVER_API_PACKAGE.length() + 1) {
            throw new IllegalArgumentException(
                    "Driver package must be a subpackage of " + DRIVER_API_PACKAGE + ": " + driverPackage);
        }
    }

    private static ArchCondition<JavaClass> satisfyDriverSpiInstantiationContract() {
        return new ArchCondition<>("be public, top-level, with a public no-arg constructor") {
            @Override
            public void check(JavaClass item, ConditionEvents events) {
                boolean isPublic = item.getModifiers().contains(JavaModifier.PUBLIC);
                boolean isTopLevel = !item.isNestedClass();
                boolean hasPublicNoArgCtor =
                        item.getConstructors().stream()
                                .anyMatch(ctor ->
                                        ctor.getModifiers().contains(JavaModifier.PUBLIC)
                                                && ctor.getRawParameterTypes().isEmpty());
                if (!isPublic || !isTopLevel || !hasPublicNoArgCtor) {
                    events.add(
                            SimpleConditionEvent.violated(
                                    item,
                                    String.format(
                                            "Driver %s must be a public top-level class with a"
                                                    + " public no-argument constructor"
                                                    + " (public=%s, topLevel=%s,"
                                                    + " publicNoArgCtor=%s)",
                                            item.getName(), isPublic, isTopLevel, hasPublicNoArgCtor)));
                }
            }
        };
    }
}
