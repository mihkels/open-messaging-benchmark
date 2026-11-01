<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Packaging a plugin

Drivers are not on the benchmark's classpath. Each driver ships as a **plugin bundle**: a directory with
a `plugin.yaml` descriptor, the driver jar and its dependencies. The worker loads exactly one bundle (plus
any bundles it references) into an isolated driver JVM (see [Architecture](architecture.md)). This page
covers how to build a bundle, the rules it must follow, and how to install it. The runtime settings are
summarized in [Runtime and plugins](../reference/runtime-and-plugins.md).

## Bundle layout

```text
<bundle>/
  plugin.yaml          descriptor
  <driver>.jar         the driver classes
  lib/*.jar            runtime dependencies of the driver
```

Paths inside a bundle are relative to the bundle directory. The directory name does not matter; the
bundle is identified by the `id` in `plugin.yaml`.

## `plugin.yaml`

The descriptor of the example loopback driver,
[`examples/external-driver-plugin/src/main/plugin/plugin.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/examples/external-driver-plugin/src/main/plugin/plugin.yaml),
before Maven resource filtering:

```yaml
descriptorVersion: 1
id: loopback
version: ${project.version}
driverApiVersion: 1
javaVersion: ${maven.compiler.release}
description: In-memory loopback driver delivering every message straight to its consumers
drivers:
  - com.example.omb.loopback.LoopbackBenchmarkDriver
classpath:
  - ${project.build.finalName}.jar
  - lib/*.jar
```

The fields, as defined by
[`PluginDescriptor`](https://github.com/mihkels/open-messaging-benchmark/blob/main/benchmark-framework/src/main/java/io/openmessaging/benchmark/plugin/PluginDescriptor.java):

|           Field            | Required |                                                     Rules                                                     |
|----------------------------|----------|---------------------------------------------------------------------------------------------------------------|
| `descriptorVersion`        | yes      | Must be `1`                                                                                                   |
| `id`                       | yes      | Letters, digits, `.`, `_` and `-` only. Must be unique among installed bundles                                |
| `version`                  | no       | Informational, shown in logs and error messages                                                               |
| `driverApiVersion`         | yes      | Must equal the benchmark's driver API version, currently `1`                                                  |
| `javaVersion`              | no       | Minimum Java feature release. The bundle is rejected on an older runtime                                      |
| `drivers`                  | yes      | At least one fully qualified `BenchmarkDriver` class name. A driver class must be provided by only one bundle |
| `classpath`                | yes      | At least one entry. Entries without wildcards must exist; wildcard entries may match nothing                  |
| `optionalClasspath`        | no       | Same syntax as `classpath`, but entries may be absent, for example user-supplied extension jars               |
| `classReferenceProperties` | no       | Names of driver YAML properties whose values are driver class names whose bundles must also be loaded         |
| `description`              | no       | Free text                                                                                                     |

Additional rules:

- Unknown fields are an error, so a typo fails the bundle instead of being ignored.
- Classpath entries must be relative paths and must not escape the bundle directory. `*` and `?`
  wildcards are allowed in the file name only, as in `lib/*.jar`.
- The combined classpath must not be empty.

An invalid bundle is logged as a warning when bundles are discovered and does not affect other bundles.
Selecting one of its drivers fails, and the error includes the reasons the bundle was rejected.

### `optionalClasspath` and `classReferenceProperties`

The JMS driver uses both:

```yaml
classpath:
  - ${project.build.finalName}.jar
  - lib/*.jar
optionalClasspath:
  - ext/*.jar
classReferenceProperties:
  - delegateForAdminOperationsClassName
```

- `ext/*.jar` lets users drop a JMS provider jar into the bundle's `ext/` directory without changing the
  descriptor.
- If the driver YAML sets `delegateForAdminOperationsClassName` to another driver class, the bundle that
  provides that class is loaded into the same driver JVM. References are followed transitively, and a
  reference to an unknown class fails with the list of known driver classes.

## In-tree drivers: the `omb-driver-bundle` profile

Driver modules in this repository do not configure bundling themselves. The root
[`pom.xml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/pom.xml) has a profile
`omb-driver-bundle` that is activated in every module where `src/main/plugin/plugin.yaml` exists. It
builds `target/omb-plugin`:

|       Phase       |                                             Action                                             |
|-------------------|------------------------------------------------------------------------------------------------|
| `prepare-package` | Delete `target/omb-plugin/lib` and `target/omb-plugin/*.jar` from earlier builds               |
| `prepare-package` | Copy runtime-scope dependencies to `target/omb-plugin/lib`                                     |
| `prepare-package` | Copy `src/main/plugin` to `target/omb-plugin` with resource filtering, so `${...}` is replaced |
| `package`         | Copy `${project.build.finalName}.jar` to `target/omb-plugin`                                   |

To add an in-tree driver module:

1. Create the module (`driver-<name>`) and add it to the `<modules>` list of the root POM.
2. Depend on `driver-api` with `provided` scope, so it is not copied into `lib/`. The worker supplies it.
3. Add `src/main/plugin/plugin.yaml` like the one above.
4. Build the reactor. The bundle appears in `driver-<name>/target/omb-plugin`, and the distribution
   copies it to `drivers/driver-<name>/`.

Dependencies that the benchmark core already provides, such as Micrometer, can also be `provided`. If
they are bundled, the [compatibility rules](#compatibility-rules) decide what happens to them.

## External drivers

[`examples/external-driver-plugin`](https://github.com/mihkels/open-messaging-benchmark/tree/main/examples/external-driver-plugin)
is a standalone Maven project (`com.example.omb:omb-driver-loopback:1.0.0`) that builds the same bundle
layout without the benchmark's parent POM. It:

- depends on `io.openmessaging.benchmark:driver-api` (version property `omb.version`, currently
  `0.0.6-SNAPSHOT`) with `provided` scope;
- bundles `commons-lang3` in `lib/`, to show a third-party dependency;
- repeats the clean, copy-dependencies and filtered resource steps of the `omb-driver-bundle` profile.

Build and run it from the repository root:

```bash
mvn -q install -DskipTests                    # full reactor; installs driver-api locally
mvn -q -f examples/external-driver-plugin/pom.xml package
OMB_PLUGIN_PATH="$PWD/examples/external-driver-plugin/target/omb-plugin" \
  bin/benchmark -d examples/external-driver-plugin/loopback.yaml \
  -o loopback-result.json e2e-tests/compose/workloads/contract.yaml
```

The first command installs `driver-api` and the rest of the reactor into the local Maven repository, so
the standalone project can resolve it. `loopback.yaml` is the driver configuration:

```yaml
name: Loopback
driverClass: com.example.omb.loopback.LoopbackBenchmarkDriver
```

## Compatibility rules

The switch from `PrometheusMeterRegistry` to `MeterRegistry` in `BenchmarkDriver.initialize` is a
one-time binary-incompatible change that retains `driverApiVersion: 1`. An older version-1 bundle's
descriptor still matches, so discovery cannot detect that the bundle is stale. Rebuild the host and
every plugin bundle, including delegates and the loopback example, together against the changed
`driver-api`, then install the rebuilt bundles on every worker. Matching descriptor or artifact
versions alone are not sufficient.

All bundles run on top of the benchmark core in the driver JVM.
[`RuntimeClasspath`](https://github.com/mihkels/open-messaging-benchmark/blob/main/benchmark-framework/src/main/java/io/openmessaging/benchmark/plugin/RuntimeClasspath.java)
builds the classpath with the core first and checks every bundle jar that contains Maven metadata
(`META-INF/maven/**/pom.properties`). Jars without that metadata are added as they are.

|                                     Situation                                      |                Result                |
|------------------------------------------------------------------------------------|--------------------------------------|
| Bundle contains a core artifact with the same version                              | Bundle copy is silently dropped      |
| Bundle contains a different version of a boundary artifact (list below)            | Error: the driver does not start     |
| Bundle contains a different version of another core artifact                       | Warning; the core version is used    |
| Two bundles in one process contain different versions of the same artifact         | Error                                |
| Two bundles contain different versions of an annotation-only artifact (list below) | Warning; the highest version is used |
| Same artifact with a different classifier (for example native libraries)           | Both are kept                        |

Boundary artifacts are the types shared between the benchmark and the driver:

- `io.openmessaging.benchmark:driver-api`
- `io.micrometer:micrometer-core`, `micrometer-commons`, `micrometer-observation`,
  `micrometer-registry-prometheus`
- `io.prometheus:prometheus-metrics-core`, `prometheus-metrics-model`

Annotation-only artifacts are those whose classes are not needed at run time:
`com.google.errorprone:error_prone_annotations`, `com.google.code.findbugs:jsr305`,
`com.google.j2objc:j2objc-annotations`, `org.checkerframework:checker-qual`,
`org.codehaus.mojo:animal-sniffer-annotations`, `com.github.spotbugs:spotbugs-annotations`,
`org.jetbrains:annotations` and `org.jspecify:jspecify`.

The core list is recorded at build time in `META-INF/omb/worker-runtime-artifacts.txt` of
`benchmark-framework`. Build drivers against the same benchmark version that the workers run, and keep
`driver-api` and Micrometer `provided`.

## Discovery

Each time a driver is initialized, the worker searches for bundles in this order:

|                      Location                      |               Typical use                |
|----------------------------------------------------|------------------------------------------|
| `$OMB_HOME/drivers/*`                              | Binary distribution and container images |
| `$OMB_HOME/*/target/omb-plugin`                    | Running from a built source checkout     |
| Entries of `OMB_PLUGIN_PATH` / `-Domb.plugin.path` | External bundles anywhere on disk        |

- `OMB_HOME` is the `omb.home` system property, then the `OMB_HOME` environment variable, then the
  working directory. The `bin/` scripts set `omb.home` to `OMB_HOME` or to their installation directory.
- `OMB_PLUGIN_PATH` and `omb.plugin.path` are lists separated by the platform path separator (`:` on
  Linux and macOS). Relative entries are resolved against `OMB_HOME`. Each entry is either a bundle
  directory or a directory that contains bundle directories.
- The driver YAML's `driverClass` selects the bundle. If no bundle provides it, the error lists the known
  driver classes and the searched locations.

## Installing on workers

Bundles are never downloaded or copied between hosts. Install the same bundle:

- on the coordinator host for local runs, because the coordinator starts the driver JVM;
- on **every** worker host for distributed runs, because each worker starts its own driver JVM. The
  coordinator only sends the driver YAML.

For a binary distribution, copy the bundle to `drivers/<name>/` under the installation directory (in the
container images, `/benchmark/drivers/<name>/`), or point `OMB_PLUGIN_PATH` at it. Workers discover
bundles on every driver initialization, so no restart is needed. See
[Distributed runs](../getting-started/distributed-runs.md) and
[Containers and Kubernetes](../guides/containers-and-kubernetes.md).

## `--legacy-driver-name`

The coordinator option `--legacy-driver-name <path>` loads the driver from a given location instead of
from discovery. It is applied in **local mode only**; with `--workers` or `--workers-file` it is ignored
and a warning is logged.

|            Location             |                                  What is loaded                                   |
|---------------------------------|-----------------------------------------------------------------------------------|
| Directory with `plugin.yaml`    | That bundle. Its `drivers` list must contain the `driverClass` of the driver YAML |
| Directory without `plugin.yaml` | `target/classes`, `*.jar` and `lib/*.jar` in that directory, whichever exist      |
| A `.jar` file                   | Only that jar                                                                     |

Bundles named through `classReferenceProperties` are still taken from discovery. This option is meant for
trying out a driver during development. Use a regular bundle for real runs.
