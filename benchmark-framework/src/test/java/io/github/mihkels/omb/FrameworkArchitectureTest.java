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
import static com.tngtech.archunit.core.domain.JavaAccess.Predicates.target;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAnyPackage;
import static com.tngtech.archunit.core.domain.properties.HasName.Predicates.name;
import static com.tngtech.archunit.core.domain.properties.HasOwner.Predicates.With.owner;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import java.util.Set;

/**
 * Module-boundary, layering, 3-JVM classpath split, and library-confinement rules for {@code
 * benchmark-framework}.
 *
 * <p>The framework talks to drivers only through {@code driver-api} and loads them in an isolated
 * driver process whose classpath strips coordinator- and supervisor-only libraries.
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

    private static final String ROOT_PACKAGE = "io.github.mihkels.omb";
    private static final String DRIVER_API_PACKAGE = ROOT_PACKAGE + ".driver";
    private static final String METRICS_PACKAGE = ROOT_PACKAGE + ".metrics";
    private static final String PLUGIN_PACKAGE = ROOT_PACKAGE + ".plugin";
    private static final String UTILS_PACKAGE = ROOT_PACKAGE + ".utils";
    private static final String DISTRIBUTOR_PACKAGE = UTILS_PACKAGE + ".distributor";
    private static final String PAYLOAD_PACKAGE = UTILS_PACKAGE + ".payload";
    private static final String WORKER_PACKAGE = ROOT_PACKAGE + ".worker";
    private static final String COMMANDS_PACKAGE = WORKER_PACKAGE + ".commands";
    private static final String JACKSON_PACKAGE = WORKER_PACKAGE + ".jackson";

    private static final DescribedPredicate<JavaClass> DRIVER_JVM_WORKER_CLASSES =
            areAnyOf(
                    "driver-JVM worker classes",
                    WORKER_PACKAGE + ".BearerTokenAuth",
                    WORKER_PACKAGE + ".DeliveryStamp",
                    WORKER_PACKAGE + ".DeliveryVerifier",
                    WORKER_PACKAGE + ".DriverConfiguration",
                    WORKER_PACKAGE + ".DriverProcessMain",
                    WORKER_PACKAGE + ".DriverProcessServer",
                    WORKER_PACKAGE + ".JvmResourceMetrics",
                    WORKER_PACKAGE + ".LatencyMetric",
                    WORKER_PACKAGE + ".LocalWorker",
                    WORKER_PACKAGE + ".MessageProducer",
                    WORKER_PACKAGE + ".PartitionMetrics",
                    WORKER_PACKAGE + ".PayloadSampler",
                    WORKER_PACKAGE + ".RunCallbacks",
                    WORKER_PACKAGE + ".TrackedTask",
                    WORKER_PACKAGE + ".Worker",
                    WORKER_PACKAGE + ".WorkerHandler",
                    WORKER_PACKAGE + ".WorkerRejectedException",
                    WORKER_PACKAGE + ".WorkerStats",
                    WORKER_PACKAGE + ".WorkerTimeouts");

    private static final DescribedPredicate<JavaClass> DRIVER_JVM_PAYLOAD_CLASSES =
            areAnyOf(
                    "driver-JVM payload classes",
                    PAYLOAD_PACKAGE + ".MessageHeaders",
                    PAYLOAD_PACKAGE + ".MessageSizeDistribution",
                    PAYLOAD_PACKAGE + ".PayloadTemplates");

    private static final DescribedPredicate<JavaClass> DRIVER_JVM_CLASSES =
            DRIVER_JVM_WORKER_CLASSES
                    .or(DRIVER_JVM_PAYLOAD_CLASSES)
                    .or(
                            resideInAnyPackage(
                                    COMMANDS_PACKAGE + "..",
                                    JACKSON_PACKAGE + "..",
                                    UTILS_PACKAGE,
                                    DISTRIBUTOR_PACKAGE + ".."));

    private static final DescribedPredicate<JavaClass> SUPERVISOR_ONLY_WORKER_CLASSES =
            areAnyOf(
                    "supervisor-only worker classes",
                    WORKER_PACKAGE + ".BenchmarkWorker",
                    WORKER_PACKAGE + ".ProcessWorker",
                    WORKER_PACKAGE + ".DriverProcess",
                    WORKER_PACKAGE + ".HttpWorkerClient",
                    WORKER_PACKAGE + ".DistributedWorkersEnsemble",
                    WORKER_PACKAGE + ".WorkerTls",
                    WORKER_PACKAGE + ".WorkerWatchdog",
                    WORKER_PACKAGE + ".JvmOptions",
                    WORKER_PACKAGE + ".WorkerUnhealthyException");

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

    // --- Directed package layering ---

    @ArchTest
    static final ArchRule LEAF_PACKAGES_DO_NOT_DEPEND_ON_HIGHER_LAYERS =
            noClasses()
                    .that()
                    .resideInAnyPackage(UTILS_PACKAGE, DISTRIBUTOR_PACKAGE, PAYLOAD_PACKAGE, JACKSON_PACKAGE)
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            ROOT_PACKAGE,
                            WORKER_PACKAGE,
                            COMMANDS_PACKAGE + "..",
                            PLUGIN_PACKAGE + "..",
                            DRIVER_API_PACKAGE + "..",
                            METRICS_PACKAGE + "..")
                    .because(
                            "utils, distributor, payload, and worker.jackson are leaf layers"
                                    + " with no upward dependencies");

    @ArchTest
    static final ArchRule WORKER_COMMANDS_DEPEND_ONLY_ON_DTO_DEPENDENCIES =
            noClasses()
                    .that()
                    .resideInAPackage(COMMANDS_PACKAGE + "..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            ROOT_PACKAGE,
                            WORKER_PACKAGE,
                            JACKSON_PACKAGE + "..",
                            PLUGIN_PACKAGE + "..",
                            METRICS_PACKAGE + "..",
                            "io.javalin..",
                            "org.eclipse.jetty..",
                            "org.asynchttpclient..",
                            "io.netty..")
                    .because(
                            "worker.commands are wire DTOs and must not depend on worker"
                                    + " implementations, plugins, HTTP servers, or HTTP clients");

    @ArchTest
    static final ArchRule PLUGIN_PACKAGE_DEPENDS_ONLY_ON_PLUGIN_AND_DRIVER_API =
            noClasses()
                    .that()
                    .resideInAPackage(PLUGIN_PACKAGE + "..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            ROOT_PACKAGE, WORKER_PACKAGE + "..", UTILS_PACKAGE + "..", METRICS_PACKAGE + "..")
                    .because(
                            "plugin discovery and classpath resolution depend only on"
                                    + " plugin and driver-api");

    @ArchTest
    static final ArchRule WORKER_DOES_NOT_DEPEND_ON_COORDINATOR =
            noClasses()
                    .that()
                    .resideInAPackage(WORKER_PACKAGE + "..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAPackage(ROOT_PACKAGE)
                    .because("worker runtime packages must never depend upward on the coordinator");

    @ArchTest
    static final ArchRule COORDINATOR_IS_ISOLATED_FROM_PLUGINS_OTLP_AND_DRIVER_INTERNALS =
            noClasses()
                    .that()
                    .resideInAPackage(ROOT_PACKAGE)
                    .should()
                    .dependOnClassesThat(
                            resideInAnyPackage(PLUGIN_PACKAGE + "..", METRICS_PACKAGE + "..")
                                    .or(
                                            areAnyOf(
                                                    "worker/driver internals hidden from coordinator",
                                                    WORKER_PACKAGE + ".LocalWorker",
                                                    WORKER_PACKAGE + ".DriverProcessMain",
                                                    WORKER_PACKAGE + ".DriverProcess",
                                                    WORKER_PACKAGE + ".DriverProcessServer",
                                                    WORKER_PACKAGE + ".WorkerHandler",
                                                    WORKER_PACKAGE + ".BenchmarkWorker",
                                                    DRIVER_API_PACKAGE + ".BenchmarkDriver",
                                                    DRIVER_API_PACKAGE + ".BenchmarkProducer",
                                                    DRIVER_API_PACKAGE + ".BenchmarkConsumer",
                                                    DRIVER_API_PACKAGE + ".ConsumerCallback")))
                    .because(
                            "the coordinator talks to workers only through Worker,"
                                    + " ProcessWorker, HttpWorkerClient, and"
                                    + " DistributedWorkersEnsemble");

    // --- Isolated driver-JVM classpath safety ---

    @ArchTest
    static final ArchRule DRIVER_JVM_DOES_NOT_DEPEND_ON_EXCLUDED_LIBRARIES =
            noClasses()
                    .that(
                            DRIVER_JVM_CLASSES.or(
                                    areAnyOf(
                                            "WorkerClock and DistributedWorkersEnsemble",
                                            WORKER_PACKAGE + ".WorkerClock",
                                            WORKER_PACKAGE + ".DistributedWorkersEnsemble")))
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            "org.asynchttpclient..",
                            "io.netty..",
                            "com.networknt..",
                            "com.github.luben.zstd..",
                            "com.beust.jcommander..",
                            "com.google.common..",
                            "org.apache.commons..")
                    .because(
                            "worker-runtime-artifacts.txt strips coordinator/supervisor"
                                    + " libraries from the isolated driver-JVM classpath");

    private static final DescribedPredicate<JavaClass> PLUGIN_EXCEPTION =
            areAnyOf("PluginException", PLUGIN_PACKAGE + ".PluginException");

    @ArchTest
    static final ArchRule DRIVER_JVM_DOES_NOT_DEPEND_ON_SUPERVISOR_ONLY_CLASSES =
            noClasses()
                    .that(DRIVER_JVM_CLASSES)
                    .should()
                    .dependOnClassesThat(
                            SUPERVISOR_ONLY_WORKER_CLASSES.or(
                                    resideInAPackage(PLUGIN_PACKAGE + "..").and(not(PLUGIN_EXCEPTION))))
                    .because(
                            "driver-JVM classes must not reach into supervisor-only worker"
                                    + " orchestration or plugin-loading internals");

    // --- Third-party library & process confinement ---

    @ArchTest
    static final ArchRule JAVALIN_CONFINED_TO_HTTP_SERVER_CLASSES =
            noClasses()
                    .that(
                            not(
                                    areAnyOf(
                                            "HTTP server classes",
                                            WORKER_PACKAGE + ".BearerTokenAuth",
                                            WORKER_PACKAGE + ".BenchmarkWorker",
                                            WORKER_PACKAGE + ".DriverProcessMain",
                                            WORKER_PACKAGE + ".DriverProcessServer",
                                            WORKER_PACKAGE + ".WorkerHandler")))
                    .should()
                    .dependOnClassesThat()
                    .resideInAPackage("io.javalin..")
                    .because("Javalin HTTP server wiring belongs only in worker server classes");

    @ArchTest
    static final ArchRule JETTY_CONFINED_TO_WORKER_HTTP_SERVER =
            noClasses()
                    .that(
                            not(
                                    areAnyOf(
                                            "BenchmarkWorker, WorkerTls, and WorkerHandler",
                                            WORKER_PACKAGE + ".BenchmarkWorker",
                                            WORKER_PACKAGE + ".WorkerTls",
                                            WORKER_PACKAGE + ".WorkerHandler")))
                    .should()
                    .dependOnClassesThat()
                    .resideInAPackage("org.eclipse.jetty..")
                    .because(
                            "Jetty server, TLS, and HTTP configuration belongs only in"
                                    + " BenchmarkWorker, WorkerTls, and WorkerHandler");

    @ArchTest
    static final ArchRule ASYNC_HTTP_CLIENT_AND_NETTY_CONFINED_TO_HTTP_WORKER_CLIENT =
            noClasses()
                    .that(not(areAnyOf("HttpWorkerClient", WORKER_PACKAGE + ".HttpWorkerClient")))
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage("org.asynchttpclient..", "io.netty..")
                    .because(
                            "AsyncHttpClient and Netty are excluded from the driver JVM and"
                                    + " confined to HttpWorkerClient");

    @ArchTest
    static final ArchRule JCOMMANDER_CONFINED_TO_CLI_ENTRY_POINTS =
            noClasses()
                    .that(
                            not(
                                    areAnyOf(
                                            "CLI entry points",
                                            ROOT_PACKAGE + ".Benchmark",
                                            WORKER_PACKAGE + ".BenchmarkWorker")))
                    .should()
                    .dependOnClassesThat()
                    .resideInAPackage("com.beust.jcommander..")
                    .because("JCommander argument parsing belongs only in Benchmark and BenchmarkWorker");

    @ArchTest
    static final ArchRule JSON_SCHEMA_VALIDATOR_CONFINED_TO_YAML_SCHEMAS =
            noClasses()
                    .that(not(areAnyOf("YamlSchemas", ROOT_PACKAGE + ".YamlSchemas")))
                    .should()
                    .dependOnClassesThat()
                    .resideInAPackage("com.networknt..")
                    .because("JSON schema validation is coordinator-only and confined to YamlSchemas");

    @ArchTest
    static final ArchRule ZSTD_CONFINED_TO_COMPRESSIBLE_PAYLOADS =
            noClasses()
                    .that(not(areAnyOf("CompressiblePayloads", PAYLOAD_PACKAGE + ".CompressiblePayloads")))
                    .should()
                    .dependOnClassesThat()
                    .resideInAPackage("com.github.luben.zstd..")
                    .because(
                            "zstd-jni payload calibration is coordinator-only and confined to CompressiblePayloads");

    @ArchTest
    static final ArchRule MAVEN_ARTIFACT_CONFINED_TO_RUNTIME_CLASSPATH =
            noClasses()
                    .that(not(areAnyOf("RuntimeClasspath", PLUGIN_PACKAGE + ".RuntimeClasspath")))
                    .should()
                    .dependOnClassesThat()
                    .resideInAPackage("org.apache.maven..")
                    .because("Maven coordinate version comparison is confined to RuntimeClasspath");

    @ArchTest
    static final ArchRule PROCESS_EXECUTION_CONFINED_TO_DRIVER_PROCESS_AND_RESULT_METADATA =
            noClasses()
                    .that(
                            not(
                                    areAnyOf(
                                            "DriverProcess and ResultMetadata",
                                            WORKER_PACKAGE + ".DriverProcess",
                                            ROOT_PACKAGE + ".ResultMetadata")))
                    .should()
                    .dependOnClassesThat()
                    .haveFullyQualifiedName(ProcessBuilder.class.getName())
                    .orShould()
                    .callMethodWhere(target(owner(assignableTo(Runtime.class))).and(target(name("exec"))))
                    .because(
                            "subprocess execution is restricted to DriverProcess (child JVM) and"
                                    + " ResultMetadata (git lookup)");

    private static DescribedPredicate<JavaClass> areAnyOf(String description, String... classNames) {
        Set<String> names = Set.of(classNames);
        return new DescribedPredicate<>(description) {
            @Override
            public boolean test(JavaClass javaClass) {
                String fullName = javaClass.getFullName();
                for (String name : names) {
                    if (fullName.equals(name) || fullName.startsWith(name + "$")) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
