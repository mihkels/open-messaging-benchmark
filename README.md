# OpenMessaging Benchmark (Fork)

[![Build](https://github.com/openmessaging/benchmark/actions/workflows/pr-build-and-test.yml/badge.svg)](https://github.com/openmessaging/benchmark/actions/workflows/pr-build-and-test.yml)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

This is a maintained fork of the OpenMessaging Benchmark framework. The goal of this fork is to keep the Docker images and dependencies up to date (notably updating the official Docker image to a Java 17 base) while tracking upstream functionality.

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
- Java version: 25 (as enforced by the build) — see mise.toml and pom.xml
- Tests: JUnit 5 (Jupiter), Mockito; integration/e2e tests under e2e-tests
- Static analysis/formatting: Checkstyle, Spotless, SpotBugs, JaCoCo coverage
- Metrics: Micrometer and Prometheus client libraries
- Containers: Docker (with Dockerfiles in docker/), multi-arch builds supported

Notes on Java versions:
- Build requires JDK 25 (pom.xml sets maven.compiler.release=25 and mise.toml sets java = "25").
- Docker runtime images are based on Java 17 (Temurin 17) per docker/README.md. This is acceptable because the runtime only needs to execute the assembled distribution. If you build locally, use JDK 25.

## Requirements

- JDK 25
- Maven 3.8.6+ (3.9.x recommended)
- Docker (optional; required for image builds or local containerized runs)
- GNU make or bash (for helper scripts)

## Project structure (high level)

- benchmark-framework: Core coordinator and worker implementation, common logic
- driver-api: Driver SPI
- driver-*: Implementations for specific brokers (Kafka, Pulsar, RabbitMQ, Redis, NATS, NSQ, RocketMQ, Artemis, Pravega, JMS, KoP)
- e2e-tests: End-to-end tests for selected drivers
- package: Assembles a binary distribution (tar.gz) including bin/, lib/, workloads/, payload/
- docker: Dockerfiles and instructions for building runnable images
- deployment: Kubernetes/Helm and other deployment manifests
- bin: Launch scripts used by the distribution and for in-repo runs
- workloads, payload: Example workloads and message payloads
- tool: Auxiliary tools/utilities

## Entry points and scripts

The packaged distribution and this repository include two main shell entry points in bin/:

- `bin/benchmark` — runs the benchmark coordinator.
  - Main class: `io.openmessaging.benchmark.Benchmark`
  - Important env vars:
    - `HEAP_OPTS`: JVM heap settings (default "-Xms4G -Xmx4G").
  - Example: bin/benchmark --drivers driver-kafka/kafka.yaml --workers <worker-urls> workloads/1-topic-16-partitions-1kb.yaml
- `bin/benchmark-worker` — runs a worker process that executes workload operations.
  - Main class: `io.openmessaging.benchmark.worker.BenchmarkWorker`
  - Important env vars:
    - HEAP_OPTS: JVM heap settings (default "-Xms4G -Xmx4G").
    - JVM_OPTS: additional JVM flags; script sets required --add-opens flags and Netty/Pulsar reflective access options.
  - Common flags: -p <port> (HTTP server port), -sp <prometheus-port> (if supported by build; see help)
  - Example: bin/benchmark-worker -p 8080 -sp 8081

Other helpful files:
- local-build.sh — convenience script to build multi-arch Docker images with buildx.
- Variables/args:
- VERSION (arg1, default: latest)
- PLATFORMS (arg2 or auto-detected; e.g., linux/arm64,linux/amd64)
- PUSH_ARG (arg3: push|--push|true|1 to push; load|--load to docker load)
- Env: DOCKER_PUSH=1 to push; REPOSITORY (default mihkels/open-messaging-benchmark); KAFKA_VERSION and JAVA_VERSION tags baked into image name.

## Build

Common build actions (from project root):
- Full build and unit test: `mvn clean install`
- Skip tests: mvn `clean install -DskipTests`
- Skip JaCoCo coverage: mvn clean verify -Djacoco.skip
- Skip Checkstyle: mvn clean verify -Dcheckstyle.skip
- Skip Spotless check: mvn clean verify -Dspotless.check.skip
- Format code: `mvn spotless:apply`
- Generate license headers: `mvn license:format`

## Running benchmarks (local)

1) Build classes if running from source tree (non-packaged):
- `mvn -q -DskipTests package`
2) Start one or more workers:
- `bin/benchmark-worker -p 8080 -sp 8081`
- Repeat on other machines/ports as needed; ensure network connectivity from coordinator to workers.
3) Run the coordinator pointing to drivers and workloads:
- bin/benchmark --drivers driver-kafka/kafka.yaml --workers http://host1:8080,http://host2:8080 workloads/1-topic-16-partitions-1kb.yaml

Notes:
- Each driver has its own configuration YAML (see `driver-*/ *.yaml`), describing broker endpoints and client parameters.
- Workloads live under workloads/ and payload/; feel free to copy and customize.

## Docker images

See docker/README.md for details. Summary:
- docker/Dockerfile: runtime image based on eclipse-temurin:17 that expects a prebuilt distribution tarball (BENCHMARK_TARBALL build-arg).
- docker/Dockerfile.build: builds the project (using a Maven builder) and produces a runtime image on eclipse-temurin:17.

Examples:
- Build with prebuilt tarball:
- mvn -q -DskipTests -pl package -am package
- export BENCHMARK_TARBALL=package/target/openmessaging-benchmark-<VERSION>-SNAPSHOT-bin.tar.gz
- docker build -t openmessaging-benchmark:latest --build-arg BENCHMARK_TARBALL . -f docker/Dockerfile
- Build everything inside Docker (no local Maven required):
- docker build -t openmessaging-benchmark:latest . -f docker/Dockerfile.build
- Multi-arch buildx helper:
- ./local-build.sh <version> [platforms] [push|load]

## Environment variables

- HEAP_OPTS: JVM heap for coordinator/worker scripts. Default "-Xms4G -Xmx4G".
- JVM_OPTS: Extra JVM flags passed by benchmark-worker script (add your own as needed).
- DOCKER_PUSH, REPOSITORY, VERSION, PLATFORMS: used by local-build.sh for image builds.
- TODO: Document any driver-specific environment variables if/when added (search driver-*/ docs).

## Tests

- Unit and integration tests run with: mvn clean verify
- End-to-end tests are under e2e-tests and target real brokers (Kafka, RabbitMQ, Redis, etc.). Running them typically requires services available and may use Testcontainers or external endpoints. To run just e2e tests:
  - mvn -pl e2e-tests -am -DskipTests=false test
- Some drivers include README.md files with instructions to spin up the corresponding broker and run sample benchmarks.

## Kubernetes/Helm and deployments

- Manifests and Helm charts are under deployment/. These install workers and run the coordinator with provided values.
- See deployment/kubernetes/helm/README.md for example commands.
- TODO: Provide an end-to-end example for a chosen driver in Kubernetes in this fork.

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
