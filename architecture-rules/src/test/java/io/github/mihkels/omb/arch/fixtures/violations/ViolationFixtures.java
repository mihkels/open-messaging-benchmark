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
package io.github.mihkels.omb.arch.fixtures.violations;

import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fixture classes violating individual {@code CommonArchitectureRules} for unit testing. */
public final class ViolationFixtures {
    private ViolationFixtures() {}

    /** Uses java.util.logging instead of SLF4J. */
    public static final class JulLoggerFixture {
        public void log() {
            java.util.logging.Logger.getLogger("test").info("jul");
        }
    }

    /** Calls Class.forName directly. */
    public static final class ClassForNameFixture {
        public Class<?> load() throws ClassNotFoundException {
            return Class.forName("java.lang.String");
        }
    }

    /** References a JUnit test class. */
    public static final class TestLibraryFixture {
        public void check() {
            Assertions.assertTrue(true);
        }
    }

    /** Declares a public SLF4J logger. */
    public static final class PublicLoggerFixture {
        public static final Logger log = LoggerFactory.getLogger(PublicLoggerFixture.class);
    }

    /** Declares a non-static SLF4J logger. */
    public static final class InstanceLoggerFixture {
        private final Logger log = LoggerFactory.getLogger(InstanceLoggerFixture.class);

        public Logger logger() {
            return log;
        }
    }

    /** Declares a non-final SLF4J logger. */
    public static final class MutableLoggerFixture {
        private static Logger log = LoggerFactory.getLogger(MutableLoggerFixture.class);

        public static Logger logger() {
            return log;
        }
    }

    /** Writes to System.out. */
    public static final class SystemOutFixture {
        public void print() {
            System.out.println("out");
        }
    }

    /** Writes to System.err. */
    public static final class SystemErrFixture {
        public void print() {
            System.err.println("err");
        }
    }

    /** Calls Throwable.printStackTrace. */
    public static final class PrintStackTraceFixture {
        public void dump(Exception e) {
            e.printStackTrace();
        }
    }

    /** Calls System.exit and contains a nested caller. */
    public static final class SystemExitFixture {
        public void exit() {
            System.exit(1);
        }

        /** Nested class inside an allowed CLI entry point. */
        public static final class NestedExit {
            public void exit() {
                System.err.println("nested");
            }
        }
    }

    /** Calls Runtime.exit. */
    public static final class RuntimeExitFixture {
        public void exit() {
            Runtime.getRuntime().exit(1);
        }
    }

    /** Calls Runtime.halt. */
    public static final class RuntimeHaltFixture {
        public void halt() {
            Runtime.getRuntime().halt(1);
        }
    }
}
