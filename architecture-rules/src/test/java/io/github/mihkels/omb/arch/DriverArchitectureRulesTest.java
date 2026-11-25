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
import io.github.mihkels.omb.driver.MisplacedDriverClass;
import io.github.mihkels.omb.driver.testbad.BadDrivers;
import io.github.mihkels.omb.driver.testbad.BadDrivers.AbstractDriver;
import io.github.mihkels.omb.driver.testbad.BadDrivers.DirectYamlMapperDriver;
import io.github.mihkels.omb.driver.testbad.BadDrivers.FrameworkDependencyDriver;
import io.github.mihkels.omb.driver.testbad.BadDrivers.NestedDriver;
import io.github.mihkels.omb.driver.testbad.BadDrivers.NotADriver;
import io.github.mihkels.omb.driver.testbad.BadDrivers.ProcessBuilderDriver;
import io.github.mihkels.omb.driver.testbad.BadDrivers.PrometheusDependencyDriver;
import io.github.mihkels.omb.driver.testbad.BadDrivers.RuntimeExecDriver;
import io.github.mihkels.omb.driver.testbad.BadDrivers.SiblingDependencyDriver;
import io.github.mihkels.omb.driver.testbad.NoDefaultCtorDriver;
import io.github.mihkels.omb.driver.testbad.PrivateCtorDriver;
import io.github.mihkels.omb.driver.testclean.CleanBenchmarkDriver;
import io.github.mihkels.omb.driver.testcycle.cyclea.CycleA;
import io.github.mihkels.omb.driver.testcycle.cycleb.CycleB;
import java.util.List;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;

class DriverArchitectureRulesTest {

    private static final ClassFileImporter IMPORTER = new ClassFileImporter();
    private static final String CLEAN_PACKAGE = "io.github.mihkels.omb.driver.testclean";

    @Test
    void cleanDriverPassesAllVerifyChecks() throws Throwable {
        JavaClasses clean = IMPORTER.importPackages(CLEAN_PACKAGE);

        List<DynamicTest> tests = DriverArchitectureRules.verify(clean, CLEAN_PACKAGE).toList();
        assertThat(tests).hasSize(16);
        for (DynamicTest test : tests) {
            test.getExecutable().execute();
        }
    }

    @Test
    void verifyByClassImportsModuleClasses() {
        JavaClasses imported = DriverArchitectureRules.importDriverModule(CleanBenchmarkDriver.class);
        assertThat(imported.contain(CleanBenchmarkDriver.class)).isTrue();
        assertThat(DriverArchitectureRules.verify(CleanBenchmarkDriver.class, CLEAN_PACKAGE))
                .hasSize(16);
    }

    @Test
    void frameworkDependencyFailsRule() {
        JavaClasses classes = IMPORTER.importClasses(FrameworkDependencyDriver.class);

        assertThatThrownBy(() -> DriverArchitectureRules.NO_FRAMEWORK_DEPENDENCY.check(classes))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("driver-api");
    }

    @Test
    void crossDriverDependencyFailsUnlessAllowlisted() {
        JavaClasses classes = IMPORTER.importClasses(SiblingDependencyDriver.class);

        assertThatThrownBy(() ->
                        DriverArchitectureRules.noCrossDriverDependencies(
                                        classes, "io.github.mihkels.omb.driver.testbad")
                                .check(classes))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("allowlisted");

        DriverArchitectureRules.noCrossDriverDependencies(
                        classes,
                        "io.github.mihkels.omb.driver.testbad",
                        "io.github.mihkels.omb.driver.testsibling")
                .check(classes);
    }

    @Test
    void sharedPackageClassOutsideModuleFailsCrossDriverRule() {
        String sharedPackage = "io.github.mihkels.omb.driver.testcycle";
        JavaClasses onlyA = IMPORTER.importClasses(CycleA.class);

        assertThatThrownBy(() ->
                        DriverArchitectureRules.noCrossDriverDependencies(onlyA, sharedPackage).check(onlyA))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(CycleB.class.getName());

        JavaClasses both = IMPORTER.importClasses(CycleA.class, CycleB.class);
        DriverArchitectureRules.noCrossDriverDependencies(both, sharedPackage).check(both);
    }

    @Test
    void misplacedClassFailsResideInDriverPackageRule() {
        JavaClasses classes = IMPORTER.importClasses(MisplacedDriverClass.class);

        assertThatThrownBy(
                        () -> DriverArchitectureRules.resideInDriverPackage(CLEAN_PACKAGE).check(classes))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void invalidDriverPackageThrowsIllegalArgumentException() {
        assertThatThrownBy(() -> DriverArchitectureRules.resideInDriverPackage("com.example"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () -> DriverArchitectureRules.resideInDriverPackage("io.github.mihkels.omb.driver."))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void spiInstantiationContractFailsForInvalidDriversAndEmptyModule() {
        for (Class<?> badDriver :
                new Class<?>[] {
                    BadDrivers.PACKAGE_PRIVATE_DRIVER,
                    NestedDriver.class,
                    NoDefaultCtorDriver.class,
                    PrivateCtorDriver.class
                }) {
            JavaClasses classes = IMPORTER.importClasses(badDriver);
            assertThatThrownBy(
                            () -> DriverArchitectureRules.BENCHMARK_DRIVERS_SATISFY_SPI_CONTRACT.check(classes))
                    .isInstanceOf(AssertionError.class);
        }

        JavaClasses withoutConcreteDriver =
                IMPORTER.importClasses(NotADriver.class, AbstractDriver.class);
        assertThatThrownBy(() ->
                        DriverArchitectureRules.BENCHMARK_DRIVERS_SATISFY_SPI_CONTRACT.check(
                                withoutConcreteDriver))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void prometheusDependencyFailsRule() {
        JavaClasses classes = IMPORTER.importClasses(PrometheusDependencyDriver.class);

        assertThatThrownBy(() -> DriverArchitectureRules.NO_PROMETHEUS_TYPES.check(classes))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void directYamlMapperAndMissingDriverConfigsFailRules() {
        JavaClasses directYaml = IMPORTER.importClasses(DirectYamlMapperDriver.class);
        assertThatThrownBy(() -> DriverArchitectureRules.NO_DIRECT_YAML_MAPPER.check(directYaml))
                .isInstanceOf(AssertionError.class);

        JavaClasses missingDriverConfigs = IMPORTER.importClasses(NoDefaultCtorDriver.class);
        assertThatThrownBy(() ->
                        DriverArchitectureRules.BENCHMARK_DRIVERS_USE_DRIVER_CONFIGS.check(
                                missingDriverConfigs))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void processExecutionFailsRule() {
        for (Class<?> fixture : new Class<?>[] {ProcessBuilderDriver.class, RuntimeExecDriver.class}) {
            JavaClasses classes = IMPORTER.importClasses(fixture);
            assertThatThrownBy(() -> DriverArchitectureRules.NO_PROCESS_EXECUTION.check(classes))
                    .isInstanceOf(AssertionError.class);
        }
    }

    @Test
    void internalPackageCycleFailsRule() {
        JavaClasses cycle = IMPORTER.importPackages("io.github.mihkels.omb.driver.testcycle");

        assertThatThrownBy(() ->
                        DriverArchitectureRules.noInternalPackageCycles(
                                        "io.github.mihkels.omb.driver.testcycle")
                                .check(cycle))
                .isInstanceOf(AssertionError.class);
    }
}
