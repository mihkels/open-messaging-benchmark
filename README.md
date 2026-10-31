# OpenMessaging Benchmark (Fork)

[![Build](https://github.com/openmessaging/benchmark/actions/workflows/pr-build-and-test.yml/badge.svg)](https://github.com/openmessaging/benchmark/actions/workflows/pr-build-and-test.yml)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

This is a maintained fork of the OpenMessaging Benchmark framework, keeping its dependencies and
Java 27 container images up to date while tracking upstream functionality.

Notice: We do not consider or plan to release any unilateral test results based on this standard. For reference, you can purchase server tests on the cloud by yourself.

## Overview

OpenMessaging Benchmark (OMB) is a user-friendly, cloud-ready benchmarking suite for popular messaging platforms. It provides a coordinator/worker architecture to run standardized workloads against different drivers.

Supported platforms (drivers) include:
- Apache ActiveMQ Artemis
- Apache BookKeeper
- Apache Kafka
- Apache Pulsar (and KoP: Kafka-on-Pulsar)
- Apache RocketMQ
- Generic JMS
- NATS (JetStream and legacy Streaming/STAN)
- NSQ
- Pravega
- RabbitMQ
- Redis

More details can be found in the upstream documentation: http://openmessaging.cloud/docs/benchmarks/

## Technology stack

- Language: Java
- Build tool / package manager: Apache Maven (multi-module project)
- Java version: 27 (as enforced by the build) — see mise.toml and pom.xml
- Tests: JUnit 6 (Jupiter), Mockito; integration/e2e tests under e2e-tests
- Static analysis/formatting: Checkstyle, Spotless, SpotBugs, JaCoCo coverage
- Metrics: Micrometer and Prometheus client libraries
- Containers: Docker (with Dockerfiles in docker/), multi-arch builds supported

Builds and benchmark runtimes require Java 27. The application Dockerfiles use Amazon Corretto 27;
the Compose regression runner uses Eclipse Temurin 27.

## Requirements

- JDK 27
- Maven 3.9.0+ (3.10.0 recommended)
- [mise](https://mise.jdx.dev/) (recommended for the Java, Maven, and Python versions in `mise.toml`)
- Docker (optional; required for image builds or local containerized runs)
- GNU make or bash (for helper scripts)

## Project structure (high level)

- benchmark-framework: Core coordinator and worker implementation, common logic
- driver-api: Driver SPI
- driver-*: Implementations for specific brokers (Kafka, Pulsar, RabbitMQ, Redis, NATS, NSQ, RocketMQ, Artemis, Pravega, JMS, KoP), each packaged as a driver plugin bundle (see [Driver plugins](#driver-plugins))
- e2e-tests: Testcontainers tests for selected drivers and a Compose regression suite for all plugins
- package: Assembles a binary distribution (tar.gz) including bin/, lib/, workloads/, payload/
- docker: Dockerfiles and instructions for building runnable images
- deployment: Kubernetes/Helm and other deployment manifests
- bin: Launch scripts used by the distribution and for in-repo runs
- workloads, payload: Example workloads and message payloads
- tool: Auxiliary tools/utilities
- examples/external-driver-plugin: A standalone driver plugin built outside this repository

## Entry points and scripts

The packaged distribution and this repository include two main shell entry points in bin/:

- `bin/benchmark` — runs the benchmark coordinator.
  - Main class: `io.openmessaging.benchmark.Benchmark`
  - Important env vars: see [Environment variables](#environment-variables).
  - Example: bin/benchmark --drivers driver-kafka/kafka.yaml --workers <worker-urls> workloads/1-topic-16-partitions-1kb.yaml
- `bin/benchmark-worker` — runs a worker process that executes workload operations.
  - Main class: `io.openmessaging.benchmark.worker.BenchmarkWorker`
  - Important env vars: see [Environment variables](#environment-variables).
  - Common flag: `-p <port>` (HTTP server port; `/metrics` includes the driver process metrics).
    `-sp` is deprecated and does not start a separate metrics listener.
  - Example: `bin/benchmark-worker -p 8080`

`local-build.sh` builds multi-architecture images with buildx. Its arguments are the version
(default `latest`), platforms (for example `linux/arm64,linux/amd64`), and optionally `push` or
`load`. `DOCKER_PUSH=1` also enables pushing. The repository and Kafka/Java tag components are set
in the script. It formats license headers and source files, and exports a BuildKit cache to
`mihkels/open-messaging-benchmark:cache` even when the image itself is not pushed.

## Build

Run `mise install` to install the project tools. Activate mise in your shell, or prefix the commands
below with `mise exec --` to use the configured versions.

Common build actions (from project root):
- Full build and unit test: `mvn clean install`
- Skip tests: `mvn clean install -DskipTests`
- Skip JaCoCo coverage: `mvn clean verify -Djacoco.skip`
- Skip Checkstyle: `mvn clean verify -Dcheckstyle.skip`
- Skip Spotless check: `mvn clean verify -Dspotless.check.skip`
- Format code: `mvn spotless:apply`
- Generate license headers: `mvn license:format`

In some Git worktrees, the license plugin can fail with "Bare Repository has neither a working
tree, nor an index". As a workaround, append `-Dlicense.skip` to a build command; this bypasses
the license check.

## Running benchmarks (local)

Build from the repository root with the full reactor. The distribution assembly includes plugin
bundles from the reactor, so building only the `package` module can omit drivers.

1) Build classes and plugin bundles if running from the source tree:
- `mvn -q -DskipTests install`
2) Start one or more workers:
- `bin/benchmark-worker -p 8080`
- Repeat on other machines/ports as needed; ensure network connectivity from coordinator to workers.
3) Run the coordinator pointing to drivers and workloads:
- bin/benchmark --drivers driver-kafka/kafka.yaml --workers http://host1:8080,http://host2:8080 workloads/1-topic-16-partitions-1kb.yaml

Notes:
- Each driver has its own configuration YAML (see `driver-*/*.yaml`), describing broker endpoints and client parameters.
- Workloads live under workloads/ and payload/; feel free to copy and customize.

## Driver plugins

Drivers are not on the benchmark classpath. Each driver is a self-contained plugin bundle, and every
benchmark run starts the selected driver in its own child JVM whose classpath contains only the
benchmark runtime, that driver and its own libraries. Driver-specific dependencies are isolated
from other drivers; the shared-runtime and combined-plugin rules below still apply.

How it works:

- The `driverClass` in the driver YAML selects the plugin, as before.
- `bin/benchmark` (local mode) and every `bin/benchmark-worker` act as supervisors: when a driver is
  initialized they start a driver process (`DriverProcessMain`) on a loopback port and proxy all
  worker calls to it. Running a different driver replaces the process. The process is stopped
  when the supervisor exits.
- Some drivers depend on other plugins: the JMS driver loads the plugin of the class named by
  `delegateForAdminOperationsClassName` (for example the Kafka or Pulsar plugin), and KoP bundles
  both Kafka and Pulsar clients. Only these combinations share a driver process.
  If bundles that share a process contain different versions of the same library, the driver fails
  to start. Annotation-only jars, such as `error_prone_annotations` or `jsr305`, are the exception: the
  highest version is used, and a warning is logged.

### Bundle layout

The distribution contains one directory per plugin under `drivers/`:

```
drivers/driver-kafka/
├── plugin.yaml
├── driver-kafka-<version>.jar
└── lib/            # the driver's runtime dependencies
```

`plugin.yaml` describes the bundle:

```yaml
descriptorVersion: 1          # descriptor format, currently 1
id: kafka                     # unique plugin id
version: 0.0.6-SNAPSHOT
driverApiVersion: 1           # must equal the benchmark's DriverApiVersion.CURRENT
javaVersion: 27               # minimum Java feature version
drivers:                      # BenchmarkDriver implementations in this bundle
  - io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver
classpath:                    # relative to the bundle; plain entries must exist, globs ('*' in the file name) may match nothing
  - driver-kafka-0.0.6-SNAPSHOT.jar
  - lib/*.jar
```

Bundles can also declare optional entries. For example, the JMS descriptor includes:

```yaml
optionalClasspath:            # entries that may be missing, e.g. user supplied jars
  - ext/*.jar
classReferenceProperties:     # driver YAML properties naming classes of other plugins
  - delegateForAdminOperationsClassName
```

Plugins are discovered in `$OMB_HOME/drivers/*`, in `*/target/omb-plugin` of a source checkout,
and in the directories listed in `OMB_PLUGIN_PATH` (or the `omb.plugin.path` system property),
separated by the platform path separator. Each entry may be a bundle or a directory of bundles.
Invalid bundles are reported as warnings and do not block unrelated drivers. Selecting a driver with
an ambiguous plugin id or driver class fails with a description of the conflicting bundles.

### Compatibility rules

- Plugins compile against `driver-api` with `provided` scope and must not bundle it.
- `driver-api`, Micrometer and the Prometheus metrics libraries are shared with the benchmark
  runtime. A bundle containing a different version of these is rejected.
- Any other library that is also part of the benchmark runtime (for example Jackson, HdrHistogram
  or Log4j) is loaded from the runtime; the bundle's copy is ignored with a warning.
- Two bundles loaded into the same process (JMS plus its delegate) must not contain different
  versions of the same library and classifier, except for the annotation-only artifacts listed by
  `RuntimeClasspath`, where the highest Maven version is selected with a warning. Distinct
  classifiers, such as native libraries for different platforms, remain on the classpath.
- Bundles are never downloaded: install them on the coordinator (local mode) and on every worker.

The NSQ driver also needs access to the nsqd HTTP API to create topics. `nsqdHttpPort` in
`driver-nsq/nsq.yaml` defaults to 4151; producer TCP port 4150 and lookup HTTP port 4161 are fixed.

### Writing an external driver plugin

[`examples/external-driver-plugin`](examples/external-driver-plugin) is a complete, standalone
Maven project for an in-memory loopback driver:

```bash
mvn -q install -DskipTests                    # full reactor; installs driver-api locally
mvn -q -f examples/external-driver-plugin/pom.xml package
OMB_PLUGIN_PATH="$PWD/examples/external-driver-plugin/target/omb-plugin" \
  bin/benchmark -d examples/external-driver-plugin/loopback.yaml \
  -o loopback-result.json e2e-tests/compose/workloads/contract.yaml
```

For a packaged distribution, copy `examples/external-driver-plugin/target/omb-plugin` into its
`drivers/loopback` directory instead. Install the same bundle on every remote worker.

Plugins inside this repository get the bundle layout from the `omb-driver-bundle` profile of the root
POM, which activates for every module that has `src/main/plugin/plugin.yaml`.

## Docker images

See [docker/README.md](docker/README.md) for details. Both Dockerfiles use an Amazon Corretto 27
runtime. `docker/Dockerfile` consumes a prebuilt distribution; `docker/Dockerfile.build` builds the
full project with Maven in Docker.

Examples:
- Build with prebuilt tarball:
- `mvn -q -DskipTests install`
- `export BENCHMARK_TARBALL=package/target/openmessaging-benchmark-0.0.6-SNAPSHOT-bin.tar.gz`
- `docker build -t openmessaging-benchmark:latest --build-arg "BENCHMARK_TARBALL=$BENCHMARK_TARBALL" -f docker/Dockerfile .`
- Build everything inside Docker (no local Maven required):
- docker build -t openmessaging-benchmark:latest . -f docker/Dockerfile.build
- Multi-arch buildx helper:
- ./local-build.sh <version> [platforms] [push|load]

The `local-test/docker-compose.yaml` example uses `openmessaging-benchmark:local` by default.
Build that tag locally or set `OMB_IMAGE` to an existing Java 27 benchmark image before starting it.

## Environment variables

- HEAP_OPTS: JVM heap of the driver process, where the workload runs. Default "-Xms4G -Xmx4G".
- JVM_OPTS: Extra JVM flags for the driver process. Quote values containing spaces.
- OMB_DRIVER_JVM_OPTS: Replaces the complete driver process JVM options (heap, GC and `--add-opens` flags). By default the scripts build it from HEAP_OPTS and JVM_OPTS. Also available as the `omb.driver.jvmOpts` system property.
- Driver-specific system properties can be passed in `JVM_OPTS` or `OMB_DRIVER_JVM_OPTS`; the
  supervisor's `zone.id` and Log4j configuration properties are forwarded by default.
- SUPERVISOR_HEAP_OPTS: JVM heap of the coordinator/worker supervisor process. Default "-Xmx1G".
- OMB_HOME: Benchmark installation directory used to find `drivers/`. Defaults to the scripts'
  installation directory.
- OMB_PLUGIN_PATH: Additional plugin bundle locations (see [Driver plugins](#driver-plugins)).
- The `omb.driver.startupTimeoutSeconds` system property limits the driver process start time (default 120).
- DOCKER_PUSH: enables pushing images from `local-build.sh`.

## Tests

- Unit tests run with `mvn clean verify`. Testcontainers integration tests are skipped by default;
  run them with Docker using `mvn -pl e2e-tests -am -De2e verify`. They use Maven Failsafe, so the
  `verify` phase is required.
- Some drivers include README.md files with instructions to spin up the corresponding broker and run sample benchmarks.

### Driver plugin regression suite (Docker Compose)

`e2e-tests/compose/run-e2e.sh` runs every driver plugin end to end against its messaging service. The brokers, two
workers and the coordinator all run in Docker Compose. Each case checks that messages are published and consumed
correctly. The suite writes an HTML report, a Markdown summary, JUnit XML and JSON to
`e2e-tests/target/e2e-reports/<run-id>/`.

```bash
e2e-tests/compose/run-e2e.sh                 # all plugins
e2e-tests/compose/run-e2e.sh --stacks kafka  # one messaging service
```

See [e2e-tests/compose/README.md](e2e-tests/compose/README.md) for the case matrix, platform notes and the JMS
providers.

## Kubernetes/Helm and deployments

- Manifests and Helm charts are under `deployment/`. See
  [deployment/kubernetes/helm/README.md](deployment/kubernetes/helm/README.md) for Helm 3 examples.
- The legacy Terraform/Ansible deployments under the driver directories still reference older
  distribution names, Java versions and launcher settings. Adapt them for Java 27, the current
  tarball, and the complete `drivers/` plugin bundles before using them with this fork.

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
