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

# Supported drivers

Every driver is a plugin bundle installed in `drivers/` of the distribution and the Docker image. A
driver configuration file selects the plugin through its `driverClass`. See
[Runtime and plugins](../reference/runtime-and-plugins.md) for how plugins are loaded.

## Drivers

|                  Driver                  |    Plugin ID     |                                 `driverClass`                                  |                        Shipped configurations                         |
|------------------------------------------|------------------|--------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| [Apache Kafka](kafka.md)                 | `kafka`          | `io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver`                 | `driver-kafka/kafka-*.yaml` (14 files)                                |
| [Kafka on Pulsar (KoP)](kop.md)          | `kop`            | `io.openmessaging.benchmark.driver.kop.KopBenchmarkDriver`                     | `kafka_to_kafka.yaml`, `kafka_to_pulsar.yaml`, `pulsar_to_kafka.yaml` |
| [Apache Pulsar](pulsar.md)               | `pulsar`         | `io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkDriver`               | `pulsar.yaml`, `pulsar-effectively-once.yaml`                         |
| [Apache BookKeeper](bookkeeper.md)       | `bookkeeper`     | `io.openmessaging.benchmark.driver.bookkeeper.DlogBenchmarkDriver`             | `bookkeeper.yaml`                                                     |
| [Pravega](pravega.md)                    | `pravega`        | `io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver`             | `pravega.yaml`, `pravega-exactly-once.yaml`                           |
| [RabbitMQ](rabbitmq.md)                  | `rabbitmq`       | `io.openmessaging.benchmark.driver.rabbitmq.RabbitMqBenchmarkDriver`           | `rabbitmq.yaml`                                                       |
| [Apache ActiveMQ Artemis](artemis.md)    | `artemis`        | `io.openmessaging.benchmark.driver.artemis.ArtemisBenchmarkDriver`             | `artemis.yaml`                                                        |
| [JMS](jms.md)                            | `jms`            | `io.openmessaging.benchmark.driver.jms.JMSBenchmarkDriver`                     | `artemis-jms*.yaml`, `pulsar-jms*.yaml`, `kafka-jms.yaml`             |
| [NATS JetStream](nats.md)                | `nats`           | `io.openmessaging.benchmark.driver.nats.NatsBenchmarkDriver`                   | `nats.yaml`                                                           |
| [NATS Streaming](nats-streaming.md)      | `nats-streaming` | `io.openmessaging.benchmark.driver.natsstreaming.NatsStreamingBenchmarkDriver` | `nats-streaming.yaml`                                                 |
| [NSQ](nsq.md)                            | `nsq`            | `io.openmessaging.benchmark.driver.nsq.NsqBenchmarkDriver`                     | `nsq.yaml`                                                            |
| [Redis Streams](redis.md)                | `redis`          | `io.openmessaging.benchmark.driver.redis.RedisBenchmarkDriver`                 | `redis.yaml`                                                          |
| [Apache RocketMQ](rocketmq.md)           | `rocketmq`       | `io.openmessaging.benchmark.driver.rocketmq.RocketMQBenchmarkDriver`           | `driver-rocketmq/rocketmq.yaml`                                       |
| [Apache RocketMQ 5 (gRPC)](rocketmq5.md) | `rocketmq5`      | `io.openmessaging.benchmark.driver.rocketmq.RocketMQ5BenchmarkDriver`          | `driver-rocketmq5/rocketmq.yaml`                                      |
| [MQTT 5](mqtt5.md)                       | `mqtt5`          | `io.openmessaging.benchmark.driver.mqtt5.MqttBenchmarkDriver`                  | `mqtt5.yaml`                                                          |

Shipped configuration files are in the driver's `driver-<id>/` directory. They are examples: their
broker addresses, replication settings and cluster names usually need changing for your
environment, and several of them share the same `name`. Each driver page explains every setting.
Each module's `driver-<id>/README.md` summarises the broker prerequisites and a usage example; some
also show how to start the broker.

> **Note:** The JMS driver needs a JMS provider's client jars, which are not shipped. Copy them into
> `drivers/driver-jms/ext/`; see [JMS](jms.md).
>
> **Note:** NATS Streaming is deprecated by its maintainers. Prefer the NATS JetStream driver for new
> work.

## Regression suite versions

The opt-in [Testcontainers suite](../development/testing-a-driver.md#testcontainers-end-to-end-tests)
defines a contract for each shipped plugin and records which contracts actually ran. It tests the local
isolated-driver process; its results are separate from the distributed Compose cases below.

The [Compose regression suite](../development/testing-a-driver.md) registers cases for every driver
against the broker versions below. Registration is not a guarantee that every case passes in a
particular environment, nor evidence of compatibility with other versions. These are functional
checks, not performance comparisons.

|      Stack       |                               Broker                                |                                     Cases                                     |
|------------------|---------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `kafka`          | Apache Kafka 4.4.0 (KRaft)                                          | Contract, LZ4 with keys and partitions, idempotent fan-out, size distribution |
| `pulsar`         | Apache Pulsar 4.0.6 standalone                                      | Contract, Shared without batching, Key_Shared with deduplication              |
| `rabbitmq`       | RabbitMQ 4.1                                                        | Quorum queues, classic queues with fan-out, streams                           |
| `redis`          | Redis 7.4                                                           | Contract, fan-out, random payloads                                            |
| `artemis`        | Apache Artemis 2.57.0                                               | Contract, fan-out                                                             |
| `nats`           | NATS 2.11 with JetStream                                            | File storage, memory storage with fan-out                                     |
| `nats-streaming` | NATS Streaming 0.25.6                                               | Contract, fan-out                                                             |
| `nsq`            | NSQ 1.3.0                                                           | Contract, fan-out                                                             |
| `rocketmq`       | Apache RocketMQ 5.3.2                                               | Contract, partitioned with keys                                               |
| `rocketmq5`      | Apache RocketMQ 5.3.2 with gRPC proxy                               | Contract                                                                      |
| `mqtt5`          | Eclipse Mosquitto                                                   | Contract                                                                      |
| `bookkeeper`     | BookKeeper from the Pulsar 4.0.6 image, DistributedLog              | Contract                                                                      |
| `pravega`        | Pravega 0.13.0 standalone (amd64, emulated on arm64)                | Contract, transactions                                                        |
| `kop`            | Pulsar 2.10.4 with KoP 2.10.4.6 (amd64, emulated on arm64)          | Kafka to Kafka, Kafka to Pulsar, Pulsar to Kafka                              |
| `jms-artemis`    | Artemis JMS client 2.57.0 with Artemis                              | JMS 2.0 contract, JMS 1.1 transactions                                        |
| `jms-pulsar`     | DataStax Pulsar JMS 6.0.11 with Pulsar                              | Contract, selectors, transactions                                             |
| `jms-kafka`      | Confluent Kafka JMS client with Kafka (client supplied by the user) | Contract                                                                      |

## Choosing a configuration

1. Start from the shipped file closest to what you want to measure, copy it, and give it a
   distinct `name`.
2. Point it at your broker; see [Using your broker](../getting-started/using-your-broker.md).
3. Read the driver page's semantics section to learn how topics, partitions and subscriptions map to
   the system.
4. Before comparing systems, align durability and batching; see
   [Fair comparisons](../methodology/fair-comparisons.md).

## Local quickstarts

Every driver page has **Local Docker Compose** and **Local kind cluster** sections. Both paths use
the registered contract cases: one topic, one partition, one producer, one subscription and one
consumer, at 200 messages/s for one minute without warm-up. Brokers, two workers and the coordinator
run in containers; startup and the first build take additional time. These are functional smoke
checks, not representative performance results.

|        Driver         |                    Docker Compose                    |                        kind                        |
|-----------------------|------------------------------------------------------|----------------------------------------------------|
| Kafka                 | [Quickstart](kafka.md#local-docker-compose)          | [Quickstart](kafka.md#local-kind-cluster)          |
| KoP                   | [Quickstart](kop.md#local-docker-compose)            | [Quickstart](kop.md#local-kind-cluster)            |
| Pulsar                | [Quickstart](pulsar.md#local-docker-compose)         | [Quickstart](pulsar.md#local-kind-cluster)         |
| BookKeeper            | [Quickstart](bookkeeper.md#local-docker-compose)     | [Quickstart](bookkeeper.md#local-kind-cluster)     |
| Pravega               | [Quickstart](pravega.md#local-docker-compose)        | [Quickstart](pravega.md#local-kind-cluster)        |
| RabbitMQ              | [Quickstart](rabbitmq.md#local-docker-compose)       | [Quickstart](rabbitmq.md#local-kind-cluster)       |
| Artemis               | [Quickstart](artemis.md#local-docker-compose)        | [Quickstart](artemis.md#local-kind-cluster)        |
| JMS (three providers) | [Quickstart](jms.md#local-docker-compose)            | [Quickstart](jms.md#local-kind-cluster)            |
| NATS JetStream        | [Quickstart](nats.md#local-docker-compose)           | [Quickstart](nats.md#local-kind-cluster)           |
| NATS Streaming        | [Quickstart](nats-streaming.md#local-docker-compose) | [Quickstart](nats-streaming.md#local-kind-cluster) |
| NSQ                   | [Quickstart](nsq.md#local-docker-compose)            | [Quickstart](nsq.md#local-kind-cluster)            |
| Redis                 | [Quickstart](redis.md#local-docker-compose)          | [Quickstart](redis.md#local-kind-cluster)          |
| RocketMQ              | [Quickstart](rocketmq.md#local-docker-compose)       | [Quickstart](rocketmq.md#local-kind-cluster)       |
| RocketMQ 5            | [Quickstart](rocketmq5.md#local-docker-compose)      | [Quickstart](rocketmq5.md#local-kind-cluster)      |
| MQTT 5                | [Quickstart](mqtt5.md#local-docker-compose)          | [Quickstart](mqtt5.md#local-kind-cluster)          |

### Shared prerequisites

Run commands from a clone of the repository, not from an extracted binary distribution. Docker
must be running, with Compose v2, buildx, network access to broker images and pinned downloads, and
about 8 GB of available memory. Run stacks one at a time; stop other brokers before starting kind.
Compose builds the distribution in Docker if it is missing; no host Java or Maven is needed.
After code changes, pass `--build docker` to rebuild it.

For kind, first follow [Local Kubernetes setup](../guides/kubernetes-local.md#setup):

```bash
./setup.sh
```

Activate mise in your shell as described there before running the driver page's commands. The
pinned tools and repository-local `.kube/config` keep this separate from other clusters. Leave
Docker Desktop's own Kubernetes off. `k8s:run` builds and loads the driver image, deploys the broker,
waits for readiness and runs the coordinator Job. Use `OMB_K8S_REBUILD=1` after code changes.

### Results, troubleshooting and cleanup

Compose waits for broker health and worker readiness, runs the benchmark and checks message counts,
rates, errors and latency. It exits zero only when the selected cases and cleanup pass. Open
`e2e-tests/target/e2e-reports/latest/index.html`; the result for a case is
`cases/<case>/result.json` below that directory, beside its coordinator and worker logs.
The runner automatically removes its stack containers and volumes, including broker data.
See the [Compose runner README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md)
for report statuses and rerun options.

kind writes `result.json`, `coordinator.log` and worker logs to
`e2e-tests/target/k8s/<case>/`. A `PASSED <case>` message means the coordinator succeeded and emitted
parseable JSON; unlike Compose, the kind runner does not apply the full report validation checks.
Run `OMB_K8S_KEEP=1 mise run k8s:run <stack> <case>` to retain the namespace for inspection, then
`mise run k8s:logs <stack>` for pods and recent logs. By default a completed run deletes its
namespace and broker data, but leaves the cluster. After inspection:

```bash
mise run k8s:down <stack>  # delete a retained stack namespace and its broker data
mise run k8s:down          # delete the local omb kind cluster when finished
```

If startup fails, check Docker memory, image downloads and broker readiness. For kind scheduling,
context or architecture failures, see [Local Kubernetes troubleshooting](../guides/kubernetes-local.md#troubleshooting).
For driver-specific connection and configuration errors, use the troubleshooting section on the
driver page. Pravega and KoP need amd64 hosts for kind; Compose can emulate them on arm64.
JMS Kafka needs a user-supplied provider jar on both paths.

## Writing a new driver

See [Implementing a driver](../development/implementing-a-driver.md).
