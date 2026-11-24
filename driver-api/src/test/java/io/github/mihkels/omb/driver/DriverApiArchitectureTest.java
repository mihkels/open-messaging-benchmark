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
package io.github.mihkels.omb.driver;

import static com.tngtech.archunit.core.domain.JavaAccess.Predicates.target;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.type;
import static com.tngtech.archunit.core.domain.properties.HasName.Predicates.name;
import static com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.fields;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaConstructor;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaGenericArrayType;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.JavaParameterizedType;
import com.tngtech.archunit.core.domain.JavaType;
import com.tngtech.archunit.core.domain.JavaTypeVariable;
import com.tngtech.archunit.core.domain.JavaWildcardType;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.jspecify.annotations.NullMarked;

/**
 * Module-boundary rules for {@code driver-api}.
 *
 * <p>The driver API is the contract between the benchmark framework and every driver. It must stay
 * small and self-contained: it resides in {@code io.github.mihkels.omb.driver} with no subpackages,
 * exposes only JDK, SPI, {@code MeterRegistry}, and {@code DeserializationFeature} types in its
 * public signatures, and does not pull Prometheus, framework, or test dependencies into the SPI.
 */
@AnalyzeClasses(
        packages = "io.github.mihkels.omb.driver",
        importOptions = ImportOption.DoNotIncludeTests.class)
class DriverApiArchitectureTest {

    private static final String DRIVER_API_PACKAGE = "io.github.mihkels.omb.driver";

    @ArchTest
    static final ArchRule DRIVER_API_HAS_NO_SUBPACKAGES =
            classes()
                    .should()
                    .resideInAPackage(DRIVER_API_PACKAGE)
                    .because(
                            "subpackages under io.github.mihkels.omb.driver.* are reserved for"
                                    + " concrete driver plugins");

    @ArchTest
    static final ArchRule DRIVER_API_DEPENDS_ONLY_ON_JDK_AND_DECLARED_API_LIBRARIES =
            classes()
                    .should()
                    .onlyDependOnClassesThat()
                    .resideInAnyPackage(
                            "java..",
                            DRIVER_API_PACKAGE,
                            "com.fasterxml.jackson.annotation..",
                            "tools.jackson..",
                            "io.micrometer.core.instrument..",
                            "org.slf4j..",
                            "org.jspecify.annotations..")
                    .because(
                            "driver-api is the contract shared by the framework and every driver,"
                                    + " so it may only use the JDK, SLF4J, Jackson 3, JSpecify, and"
                                    + " Micrometer core instrument types");

    @ArchTest
    static final ArchRule PUBLIC_API_EXPOSES_ONLY_SPI_TYPES =
            classes()
                    .that()
                    .arePublic()
                    .and()
                    .doNotHaveSimpleName("package-info")
                    .should(exposeOnlyAllowedSpiTypes())
                    .because(
                            "public driver-api signatures must not leak internal helpers, SLF4J,"
                                    + " or Jackson mappers into the SPI");

    @ArchTest
    static final ArchRule SPI_RESOURCE_INTERFACES_EXTEND_AUTO_CLOSEABLE =
            classes()
                    .that()
                    .haveFullyQualifiedName(BenchmarkDriver.class.getName())
                    .or()
                    .haveFullyQualifiedName(BenchmarkProducer.class.getName())
                    .or()
                    .haveFullyQualifiedName(BenchmarkConsumer.class.getName())
                    .should()
                    .beInterfaces()
                    .andShould()
                    .beAssignableTo(AutoCloseable.class)
                    .because("the worker closes driver, producer, and consumer handles via AutoCloseable");

    @ArchTest
    static final ArchRule EVERY_DRIVER_API_PACKAGE_IS_NULL_MARKED =
            classes()
                    .should(haveNullMarkedPackage())
                    .because(
                            "NullAway runs with OnlyNullMarked=true, so every package in"
                                    + " driver-api must declare @NullMarked on package-info.java");

    @ArchTest
    static final ArchRule NO_CLASS_FOR_NAME_OUTSIDE_CONFIGURED_CLASSES =
            noClasses()
                    .that()
                    .doNotHaveFullyQualifiedName(ConfiguredClasses.class.getName())
                    .should()
                    // accessTargetWhere also matches method references such as Class::forName
                    .accessTargetWhere(target(owner(type(Class.class))).and(target(name("forName"))))
                    .because(
                            "untrusted class names must be loaded through ConfiguredClasses.load so"
                                    + " static initializers do not run before type checking");

    @ArchTest
    static final ArchRule NO_CONSOLE_OR_PROCESS_EXIT =
            noClasses()
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
                    .because("driver-api must report errors and logs through SLF4J and futures");

    @ArchTest
    static final ArchRule LOGGERS_SHOULD_BE_PRIVATE_STATIC_FINAL =
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

    private static ArchCondition<JavaClass> exposeOnlyAllowedSpiTypes() {
        return new ArchCondition<>("expose only allowed SPI types in public/protected signatures") {
            @Override
            public void check(JavaClass item, ConditionEvents events) {
                Set<JavaType> visited = Collections.newSetFromMap(new IdentityHashMap<>());
                for (JavaType typeParameter : item.getTypeParameters()) {
                    checkType(item, "type parameter", typeParameter, visited, events);
                }
                item.getSuperclass()
                        .ifPresent(superType -> checkType(item, "superclass", superType, visited, events));
                for (JavaType iface : item.getInterfaces()) {
                    checkType(item, "interface", iface, visited, events);
                }
                for (JavaField field : item.getFields()) {
                    if (isPublicOrProtected(field.getModifiers())
                            && !field.getModifiers().contains(JavaModifier.SYNTHETIC)) {
                        checkType(item, "field " + field.getName(), field.getType(), visited, events);
                    }
                }
                for (JavaConstructor constructor : item.getConstructors()) {
                    if (isPublicOrProtected(constructor.getModifiers())
                            && !constructor.getModifiers().contains(JavaModifier.SYNTHETIC)) {
                        for (JavaType typeParameter : constructor.getTypeParameters()) {
                            checkType(item, "constructor type parameter", typeParameter, visited, events);
                        }
                        for (JavaType param : constructor.getParameterTypes()) {
                            checkType(item, "constructor parameter", param, visited, events);
                        }
                        for (JavaClass thrown : constructor.getThrowsClause().getTypes()) {
                            checkType(item, "constructor throws", thrown, visited, events);
                        }
                    }
                }
                for (JavaMethod method : item.getMethods()) {
                    if (isPublicOrProtected(method.getModifiers())
                            && !method.getModifiers().contains(JavaModifier.SYNTHETIC)
                            && !method.getModifiers().contains(JavaModifier.BRIDGE)) {
                        String location = "method " + method.getName();
                        for (JavaType typeParameter : method.getTypeParameters()) {
                            checkType(item, location + " type parameter", typeParameter, visited, events);
                        }
                        checkType(item, location + " return type", method.getReturnType(), visited, events);
                        for (JavaType param : method.getParameterTypes()) {
                            checkType(item, location + " parameter", param, visited, events);
                        }
                        for (JavaClass thrown : method.getThrowsClause().getTypes()) {
                            checkType(item, location + " throws", thrown, visited, events);
                        }
                    }
                }
            }
        };
    }

    private static boolean isPublicOrProtected(Set<JavaModifier> modifiers) {
        return modifiers.contains(JavaModifier.PUBLIC) || modifiers.contains(JavaModifier.PROTECTED);
    }

    private static void checkType(
            JavaClass ownerClass,
            String memberLocation,
            JavaType type,
            Set<JavaType> visited,
            ConditionEvents events) {
        if (!visited.add(type)) {
            return;
        }
        switch (type) {
            case JavaClass javaClass -> {
                JavaClass base = javaClass.isArray() ? javaClass.getBaseComponentType() : javaClass;
                if (!isAllowedPublicSpiClass(base)) {
                    events.add(
                            SimpleConditionEvent.violated(
                                    ownerClass,
                                    String.format(
                                            "Public SPI class %s exposes disallowed type %s in %s",
                                            ownerClass.getName(), base.getName(), memberLocation)));
                }
            }
            case JavaParameterizedType parameterized -> {
                checkType(ownerClass, memberLocation, parameterized.toErasure(), visited, events);
                for (JavaType arg : parameterized.getActualTypeArguments()) {
                    checkType(ownerClass, memberLocation, arg, visited, events);
                }
            }
            case JavaWildcardType wildcard -> {
                for (JavaType bound : wildcard.getUpperBounds()) {
                    checkType(ownerClass, memberLocation, bound, visited, events);
                }
                for (JavaType bound : wildcard.getLowerBounds()) {
                    checkType(ownerClass, memberLocation, bound, visited, events);
                }
            }
            case JavaGenericArrayType genericArray ->
                    checkType(ownerClass, memberLocation, genericArray.getComponentType(), visited, events);
            case JavaTypeVariable<?> typeVariable -> {
                for (JavaType bound : typeVariable.getUpperBounds()) {
                    checkType(ownerClass, memberLocation, bound, visited, events);
                }
            }
            default ->
                    events.add(
                            SimpleConditionEvent.violated(
                                    ownerClass,
                                    String.format(
                                            "Public SPI class %s exposes unsupported JavaType %s in %s",
                                            ownerClass.getName(), type.getName(), memberLocation)));
        }
    }

    /**
     * Returns whether external drivers can name the type, which needs every enclosing class public.
     *
     * @param javaClass the type exposed in a public signature
     * @return {@code true} if the type and all its enclosing classes are public
     */
    private static boolean isPubliclyAccessible(JavaClass javaClass) {
        return javaClass.getModifiers().contains(JavaModifier.PUBLIC)
                && javaClass
                        .getEnclosingClass()
                        .map(DriverApiArchitectureTest::isPubliclyAccessible)
                        .orElse(true);
    }

    private static boolean isAllowedPublicSpiClass(JavaClass base) {
        if (base.isPrimitive()) {
            return true;
        }
        String packageName = base.getPackageName();
        if ("java".equals(packageName) || packageName.startsWith("java.")) {
            return true;
        }
        if (DRIVER_API_PACKAGE.equals(packageName)) {
            return isPubliclyAccessible(base);
        }
        String fullName = base.getName();
        return "io.micrometer.core.instrument.MeterRegistry".equals(fullName)
                || "tools.jackson.databind.DeserializationFeature".equals(fullName);
    }
}
