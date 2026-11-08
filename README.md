# OpenMessaging Benchmark (Fork)

[![Build](https://github.com/mihkels/open-messaging-benchmark/actions/workflows/pr-build-and-test.yml/badge.svg)](https://github.com/mihkels/open-messaging-benchmark/actions/workflows/pr-build-and-test.yml)
[![Docs](https://github.com/mihkels/open-messaging-benchmark/actions/workflows/docs.yml/badge.svg)](https://mihkels.github.io/open-messaging-benchmark/)
[![Coverage](https://raw.githubusercontent.com/mihkels/open-messaging-benchmark/badges/jacoco.svg)](docs/development/building-the-project.md#coverage-thresholds)
[![Branches](https://raw.githubusercontent.com/mihkels/open-messaging-benchmark/badges/branches.svg)](docs/development/building-the-project.md#coverage-thresholds)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=mihkels_open-messaging-benchmark&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=mihkels_open-messaging-benchmark)
[![SonarCloud Coverage](https://sonarcloud.io/api/project_badges/measure?project=mihkels_open-messaging-benchmark&metric=coverage)](https://sonarcloud.io/summary/new_code?id=mihkels_open-messaging-benchmark)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

A maintained fork of the [OpenMessaging Benchmark](https://github.com/openmessaging/benchmark)
(OMB) framework. OMB runs a workload defined in YAML against a messaging system and records publish
and consume rate, backlog, and publish, publish-delay and end-to-end latency percentiles. This fork
runs on Java 27, keeps client libraries current, runs every driver as an isolated plugin, and tests
each plugin end to end against a real broker.

**Documentation: <https://mihkels.github.io/open-messaging-benchmark/>**

> The project does not publish benchmark results and does not plan to publish comparisons between
> systems. Run the benchmark on hardware and configurations that match your own use case.

## Supported systems

Apache ActiveMQ Artemis, Apache BookKeeper, Apache Kafka, Kafka-on-Pulsar, MQTT 5, generic JMS, NATS
JetStream, NATS Streaming, NSQ, Pravega, Apache Pulsar, RabbitMQ, Redis Streams, Apache RocketMQ and
RocketMQ 5. See [Supported drivers](https://mihkels.github.io/open-messaging-benchmark/drivers/).

## Quickstart

Run a first benchmark against a single Kafka broker with Docker only:

```bash
git clone https://github.com/mihkels/open-messaging-benchmark.git
cd open-messaging-benchmark
docker buildx bake                # or pull docker.io/mihkels/open-messaging-benchmark:latest
cd local-test && docker compose up -d
docker exec -it omb-driver bash -lc \
  'bin/benchmark --drivers /drivers/kafka.yaml \
     --workers http://omb-worker-1:8080,http://omb-worker-2:8080 \
     -o /results/first-result.json /workloads/simple.yaml'
```

The [Quickstart](https://mihkels.github.io/open-messaging-benchmark/getting-started/quickstart/)
explains each step, how to check that the environment is ready, and how to read the result.
To run any driver against its broker on a local kind cluster with the Helm chart, see
[Local Kubernetes](https://mihkels.github.io/open-messaging-benchmark/guides/kubernetes-local/).

### From the binary distribution

This README is also shipped in the distribution tarball. From its directory, with Java 27 and a
reachable broker:

```bash
bin/benchmark-worker -p 8080                  # on each worker machine, or skip for local mode
bin/benchmark --drivers driver-kafka/kafka-latency.yaml \
  --workers http://host1:8080,http://host2:8080 \
  workloads/1-topic-16-partitions-1kb.yaml
```

Driver YAML files are in `driver-*/`, plugin bundles in `drivers/`, and example workloads in
`workloads/`. See [Using your broker](https://mihkels.github.io/open-messaging-benchmark/getting-started/using-your-broker/)
and the [CLI reference](https://mihkels.github.io/open-messaging-benchmark/reference/cli/).

## Documentation

|                                              Section                                              |                                 Covers                                 |
|---------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| [Getting started](https://mihkels.github.io/open-messaging-benchmark/getting-started/quickstart/) | First run, using your own broker, reading output, distributed runs     |
| [Running benchmarks](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/)        | Workloads, payloads, results, monitoring, containers and Kubernetes    |
| [Drivers](https://mihkels.github.io/open-messaging-benchmark/drivers/)                            | Setup, configuration and semantics for every driver                    |
| [Methodology](https://mihkels.github.io/open-messaging-benchmark/methodology/benchmark-design/)   | Designing experiments and comparing systems fairly                     |
| [Reference](https://mihkels.github.io/open-messaging-benchmark/reference/cli/)                    | CLI, workload schema, runtime settings and environment, results schema |
| [Development](https://mihkels.github.io/open-messaging-benchmark/development/architecture/)       | Architecture, building, writing, testing and packaging driver plugins  |

The site is built from [`docs/`](docs/); see
[Documentation](https://mihkels.github.io/open-messaging-benchmark/development/documentation/) to
preview or change it. The [upstream documentation](http://openmessaging.cloud/docs/benchmarks/)
describes the original project and does not cover this fork's plugin runtime, drivers or tooling.

## Building

Builds need JDK 27 and Maven 3.9 or later. [mise](https://mise.jdx.dev/) installs the versions in
`mise.toml`:

```bash
mise install
mvn clean install            # build, unit tests, plugin bundles and the distribution
mvn clean install -DskipTests
mvn spotless:apply           # format sources and Markdown
```

[Building the project](https://mihkels.github.io/open-messaging-benchmark/development/building-the-project/)
covers the build profiles, Docker images (published to `docker.io/mihkels/open-messaging-benchmark`), the Testcontainers and Docker Compose regression suites,
and known issues such as running the license check in a Git worktree. To contribute a change, see
[`CONTRIBUTING.md`](CONTRIBUTING.md).

## Repository layout

|                Path                 |                                Contents                                |
|-------------------------------------|------------------------------------------------------------------------|
| `benchmark-framework/`              | Coordinator, worker and plugin runtime                                 |
| `driver-api/`                       | Driver SPI that plugins compile against                                |
| `driver-*/`                         | One driver plugin per messaging system, with example driver YAML files |
| `workloads/`, `payload/`            | Example workloads and message payloads                                 |
| `bin/`                              | `benchmark` (coordinator) and `benchmark-worker` launch scripts        |
| `package/`                          | Binary distribution (`tar.gz`) with all plugin bundles                 |
| `docker/`, `deployment/`            | Dockerfiles, Kubernetes manifests and Helm chart                       |
| `e2e-tests/`                        | Testcontainers contracts and the Docker Compose regression suite       |
| `examples/external-driver-plugin/`  | A driver plugin built outside this repository                          |
| `tool/`                             | Workload generator and other utilities                                 |
| `docs/`, `mkdocs.yml`, `overrides/` | Documentation site                                                     |

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
