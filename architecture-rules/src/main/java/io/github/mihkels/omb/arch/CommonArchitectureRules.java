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
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.type;
import static com.tngtech.archunit.core.domain.properties.HasName.Predicates.name;
import static com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.fields;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.util.Set;
import org.jspecify.annotations.NullMarked;

/**
 * Reusable ArchUnit rules enforcing repository-wide invariants across production packages.
 *
 * <p>Modules pull the shared {@code @ArchTest} fields in via {@code
 * ArchTests.in(CommonArchitectureRules.class)} and selectively opt into the module-specific rules
 * ({@link #NO_GUAVA_OR_APACHE_COMMONS}, {@link #noConsoleOrProcessExitOutside(String...)}).
 */
public final class CommonArchitectureRules {

    private CommonArchitectureRules() {}

    /**
     * Excludes {@code io.github.mihkels.omb.arch} classes when a framework architecture test scans
     * {@code io.github.mihkels.omb} and has {@code architecture-rules} on its test classpath.
     */
    public static final class DoNotIncludeArchRules implements ImportOption {
        @Override
        public boolean includes(Location location) {
            return !location.contains("/io/github/mihkels/omb/arch/");
        }
    }

    /** Every production package must carry {@code @NullMarked} so NullAway checks it. */
    @ArchTest
    public static final ArchRule EVERY_PRODUCTION_PACKAGE_IS_NULL_MARKED =
            classes()
                    .should(haveNullMarkedPackage())
                    .because(
                            "NullAway runs with OnlyNullMarked=true, so any package missing"
                                    + " package-info.java with @NullMarked silently skips nullness"
                                    + " checks");

    /** Production code must log through SLF4J, never JUL or a logging implementation directly. */
    @ArchTest
    public static final ArchRule SLF4J_LOGGING_ONLY =
            noClasses()
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            "java.util.logging..", "org.apache.logging.log4j..", "ch.qos.logback..")
                    .because(
                            "production code must log through SLF4J rather than a logging"
                                    + " backend or java.util.logging");

    /**
     * Production code must use Jackson 3 ({@code tools.jackson..}); of Jackson 2 only the shared
     * {@code com.fasterxml.jackson.annotation..} package is allowed.
     */
    @ArchTest
    public static final ArchRule JACKSON_3_ONLY =
            noClasses()
                    .should()
                    .dependOnClassesThat(
                            resideInAPackage("com.fasterxml.jackson..")
                                    .and(not(resideInAPackage("com.fasterxml.jackson.annotation.."))))
                    .because(
                            "the codebase uses Jackson 3 (tools.jackson..); only"
                                    + " com.fasterxml.jackson.annotation.. is shared");

    /** Untrusted class loading must go through {@code ConfiguredClasses.load}. */
    @ArchTest
    public static final ArchRule NO_CLASS_FOR_NAME_OUTSIDE_CONFIGURED_CLASSES =
            noClasses()
                    .that()
                    .doNotHaveFullyQualifiedName("io.github.mihkels.omb.driver.ConfiguredClasses")
                    .should()
                    // accessTargetWhere also matches method references such as Class::forName
                    .accessTargetWhere(target(owner(type(Class.class))).and(target(name("forName"))))
                    .because(
                            "class names from YAML configs must go through ConfiguredClasses.load"
                                    + " so static initializers cannot run before type validation");

    /** Production code must never depend on test or architecture-checking libraries. */
    @ArchTest
    public static final ArchRule NO_TEST_LIBRARIES_IN_PRODUCTION_CODE =
            noClasses()
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            "org.junit..",
                            "org.mockito..",
                            "org.assertj..",
                            "org.hamcrest..",
                            "com.tngtech.archunit..")
                    .because("production code must not depend on test or ArchUnit libraries");

    /** SLF4J {@code Logger} fields must be {@code private static final}. */
    @ArchTest
    public static final ArchRule LOGGERS_SHOULD_BE_PRIVATE_STATIC_FINAL =
            fields()
                    .that()
                    .haveRawType("org.slf4j.Logger")
                    .should()
                    .bePrivate()
                    .andShould()
                    .beStatic()
                    .andShould()
                    .beFinal()
                    .allowEmptyShould(true)
                    .because("SLF4J Logger fields must be private static final");

    /** Core modules must prefer standard JDK APIs over Guava and Apache Commons. */
    public static final ArchRule NO_GUAVA_OR_APACHE_COMMONS =
            noClasses()
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage("com.google.common..", "org.apache.commons..")
                    .because(
                            "core modules must use standard JDK APIs rather than Guava or" + " Apache Commons");

    /**
     * Returns a rule forbidding console output, {@code printStackTrace}, and JVM termination in every
     * analyzed class.
     *
     * @return the ArchUnit rule
     */
    public static ArchRule noConsoleOrProcessExit() {
        return noConsoleOrProcessExitOutside();
    }

    /**
     * Returns a rule forbidding {@code System.out}, {@code System.err}, {@code
     * Throwable.printStackTrace}, {@code System.exit}, {@code Runtime.exit}, and {@code Runtime.halt}
     * outside the given CLI or process entry-point class names.
     *
     * @param allowedClassNames fully qualified names of entry-point classes allowed to use console
     *     streams or terminate the JVM (including their nested classes)
     * @return the ArchUnit rule
     */
    public static ArchRule noConsoleOrProcessExitOutside(String... allowedClassNames) {
        Set<String> allowed = Set.of(allowedClassNames);
        DescribedPredicate<JavaClass> notAllowedEntryPoint =
                new DescribedPredicate<>("not an allowed CLI or process entry point") {
                    @Override
                    public boolean test(JavaClass javaClass) {
                        String fullName = javaClass.getFullName();
                        for (String entryPoint : allowed) {
                            if (fullName.equals(entryPoint) || fullName.startsWith(entryPoint + "$")) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
        return noClasses()
                .that(notAllowedEntryPoint)
                .should()
                .accessField(System.class, "out")
                .orShould()
                .accessField(System.class, "err")
                .orShould()
                // accessTargetWhere also matches method references such as System::exit
                .accessTargetWhere(
                        target(owner(assignableTo(Throwable.class))).and(target(name("printStackTrace"))))
                .orShould()
                .accessTargetWhere(target(owner(type(System.class))).and(target(name("exit"))))
                .orShould()
                .accessTargetWhere(
                        target(owner(type(Runtime.class))).and(target(name("exit").or(name("halt")))))
                .allowEmptyShould(true)
                .because(
                        "only CLI and process entry points may write to System.out/err or"
                                + " terminate the JVM; use SLF4J logging and propagate failures"
                                + " instead");
    }

    private static ArchCondition<JavaClass> haveNullMarkedPackage() {
        return new ArchCondition<>("reside in a @NullMarked package") {
            @Override
            public void check(JavaClass item, ConditionEvents events) {
                if (!item.getPackage().isAnnotatedWith(NullMarked.class)) {
                    events.add(
                            SimpleConditionEvent.violated(
                                    item,
                                    String.format(
                                            "Class %s resides in package %s without @NullMarked"
                                                    + " on package-info.java",
                                            item.getName(), item.getPackageName())));
                }
            }
        };
    }
}
