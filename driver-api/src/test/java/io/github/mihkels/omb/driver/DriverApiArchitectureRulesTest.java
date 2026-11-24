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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

/** Checks the driver-api rules against fixtures that would slip past a naive implementation. */
// public so the public nested fixtures form a fully public enclosing-class chain
public class DriverApiArchitectureRulesTest {

    private static final ClassFileImporter IMPORTER = new ClassFileImporter();

    @Test
    void forbiddenGenericBoundsFailPublicApiRule() {
        for (Class<?> fixture :
                new Class<?>[] {
                    LoggerBoundClass.class, LoggerBoundMethod.class, LoggerBoundConstructor.class
                }) {
            JavaClasses classes = IMPORTER.importClasses(fixture);

            AssertionError error =
                    assertThrows(
                            AssertionError.class,
                            () -> DriverApiArchitectureTest.PUBLIC_API_EXPOSES_ONLY_SPI_TYPES.check(classes));
            assertTrue(error.getMessage().contains("org.slf4j.Logger"));
        }
    }

    @Test
    void jdkGenericBoundPassesPublicApiRule() {
        DriverApiArchitectureTest.PUBLIC_API_EXPOSES_ONLY_SPI_TYPES.check(
                IMPORTER.importClasses(NumberBound.class));
    }

    @Test
    void publicNestedTypeOfHiddenClassFailsPublicApiRule() {
        JavaClasses classes = IMPORTER.importClasses(ExposesHiddenToken.class);

        AssertionError error =
                assertThrows(
                        AssertionError.class,
                        () -> DriverApiArchitectureTest.PUBLIC_API_EXPOSES_ONLY_SPI_TYPES.check(classes));
        assertTrue(error.getMessage().contains("Hidden$Token"));
    }

    @Test
    void publicNestedTypeOfPublicClassPassesPublicApiRule() {
        DriverApiArchitectureTest.PUBLIC_API_EXPOSES_ONLY_SPI_TYPES.check(
                IMPORTER.importClasses(ExposesVisibleToken.class));
    }

    @Test
    void methodReferencesFailClassForNameAndProcessExitRules() {
        JavaClasses forName = IMPORTER.importClasses(ClassForNameReference.class);
        assertThrows(
                AssertionError.class,
                () ->
                        DriverApiArchitectureTest.NO_CLASS_FOR_NAME_OUTSIDE_CONFIGURED_CLASSES.check(forName));

        JavaClasses exit = IMPORTER.importClasses(SystemExitReference.class);
        assertThrows(
                AssertionError.class,
                () -> DriverApiArchitectureTest.NO_CONSOLE_OR_PROCESS_EXIT.check(exit));
    }

    /**
     * Leaks SLF4J through a class type parameter bound.
     *
     * @param <T> bound to an SLF4J type
     */
    public static final class LoggerBoundClass<T extends Logger> {}

    /** Leaks SLF4J through a method type parameter bound. */
    public static final class LoggerBoundMethod {
        public <T extends Logger> void accept() {}
    }

    /** Leaks SLF4J through a protected constructor type parameter bound. */
    public static class LoggerBoundConstructor {
        protected <T extends Logger> LoggerBoundConstructor() {}
    }

    /**
     * JDK bounds stay allowed.
     *
     * @param <T> bound to a JDK type
     */
    public static final class NumberBound<T extends Number> {
        public <U extends Comparable<U>> void accept() {}
    }

    /** Package-private holder, so its public nested type cannot be named outside the package. */
    static final class Hidden {
        /** Public, but unreachable through {@link Hidden}. */
        public static final class Token {}
    }

    /** Exposes {@link Hidden.Token} in a public signature. */
    public static final class ExposesHiddenToken {
        public Hidden.Token token() {
            return new Hidden.Token();
        }
    }

    /** Public holder with a public nested type. */
    public static final class Visible {
        /** Reachable through {@link Visible}. */
        public static final class Token {}
    }

    /** Exposes {@link Visible.Token} in a public signature. */
    public static final class ExposesVisibleToken {
        public Visible.Token token() {
            return new Visible.Token();
        }
    }

    /** Loader shaped like Class.forName. */
    interface Loader {
        Class<?> load(String name) throws ClassNotFoundException;
    }

    /** Takes a method reference to Class.forName. */
    static final class ClassForNameReference {
        Loader loader() {
            return Class::forName;
        }
    }

    /** Takes a method reference to System.exit. */
    static final class SystemExitReference {
        IntConsumer terminate() {
            return System::exit;
        }
    }
}
