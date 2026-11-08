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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ConfiguredClassesTest {
    private static final ClassLoader LOADER = ConfiguredClassesTest.class.getClassLoader();

    @Test
    void loadsASubtypeOfTheExpectedType() throws Exception {
        Class<? extends Runnable> loaded =
                ConfiguredClasses.load(Accepted.class.getName(), Runnable.class, LOADER);

        assertSame(Accepted.class, loaded);
    }

    @Test
    void rejectsAClassOfAnotherTypeWithoutInitializingIt() {
        String name = ConfiguredClassesTest.class.getName() + "$Rejected";

        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ConfiguredClasses.load(name, Runnable.class, LOADER));

        assertEquals(
                "Class " + name + " configured as a java.lang.Runnable does not implement it",
                failure.getMessage());
        assertFalse(InitializationFlag.INITIALIZED.get(), "the rejected class must not be initialized");
    }

    @Test
    void doesNotInitializeAnAcceptedClass() throws Exception {
        String name = ConfiguredClassesTest.class.getName() + "$AcceptedWithInitializer";

        ConfiguredClasses.load(name, Runnable.class, LOADER);

        assertFalse(InitializationFlag.ACCEPTED_INITIALIZED.get());
        Class.forName(name, true, LOADER);
        assertTrue(InitializationFlag.ACCEPTED_INITIALIZED.get());
    }

    @Test
    void reportsAMissingClass() {
        assertThrows(
                ClassNotFoundException.class,
                () -> ConfiguredClasses.load("no.such.Type", Runnable.class, LOADER));
    }

    /** Lives outside the fixtures so that reading it does not initialize them. */
    static final class InitializationFlag {
        static final AtomicBoolean INITIALIZED = new AtomicBoolean();
        static final AtomicBoolean ACCEPTED_INITIALIZED = new AtomicBoolean();

        private InitializationFlag() {}
    }

    static final class Accepted implements Runnable {
        @Override
        public void run() {
            // fixture
        }
    }

    static final class AcceptedWithInitializer implements Runnable {
        static {
            InitializationFlag.ACCEPTED_INITIALIZED.set(true);
        }

        @Override
        public void run() {
            // fixture
        }
    }

    @SuppressWarnings("unused") // loaded by name only
    static final class Rejected {
        static {
            InitializationFlag.INITIALIZED.set(true);
        }
    }
}
