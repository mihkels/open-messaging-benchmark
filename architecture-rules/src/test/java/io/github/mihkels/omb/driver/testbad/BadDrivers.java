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
package io.github.mihkels.omb.driver.testbad;

import io.github.mihkels.omb.driver.BenchmarkDriver;
import io.github.mihkels.omb.driver.testsibling.SiblingHelper;
import io.github.mihkels.omb.worker.StubWorker;
import io.prometheus.StubPrometheusMetric;
import java.io.IOException;
import tools.jackson.dataformat.yaml.YAMLMapper;

/** Violation fixtures for {@code DriverArchitectureRulesTest}. */
public final class BadDrivers {
    public static final Class<?> PACKAGE_PRIVATE_DRIVER = PackagePrivateDriver.class;

    private BadDrivers() {}

    /** Package-private driver violating public visibility. */
    static final class PackagePrivateDriver implements BenchmarkDriver {
        @Override
        public void close() {}
    }

    /** Public nested driver violating top-level requirement. */
    public static final class NestedDriver implements BenchmarkDriver {
        @Override
        public void close() {}
    }

    /** Abstract driver that should be ignored by concrete driver checks. */
    public abstract static class AbstractDriver implements BenchmarkDriver {}

    /** Non-driver helper class. */
    public static final class NotADriver {}

    /** Depends on framework internals. */
    public static final class FrameworkDependencyDriver {
        public Class<?> worker() {
            return StubWorker.class;
        }
    }

    /** Depends on a sibling driver package. */
    public static final class SiblingDependencyDriver {
        public Class<?> sibling() {
            return SiblingHelper.class;
        }
    }

    /** Depends on Prometheus types. */
    public static final class PrometheusDependencyDriver {
        public Class<?> metric() {
            return StubPrometheusMetric.class;
        }
    }

    /** Instantiates Jackson YAMLMapper directly. */
    public static final class DirectYamlMapperDriver {
        public YAMLMapper mapper() {
            return new YAMLMapper();
        }
    }

    /** Spawns a process via ProcessBuilder. */
    public static final class ProcessBuilderDriver {
        public ProcessBuilder builder() {
            return new ProcessBuilder("echo");
        }
    }

    /** Spawns a process via Runtime.exec. */
    public static final class RuntimeExecDriver {
        public Process run() throws IOException {
            return Runtime.getRuntime().exec(new String[] {"echo"});
        }
    }
}
