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

# OpenMessaging Benchmark (fork)

This site documents the maintained fork of the OpenMessaging Benchmark (OMB) framework in
[mihkels/open-messaging-benchmark](https://github.com/mihkels/open-messaging-benchmark). The fork
keeps the framework on Java 27, runs every messaging driver as an isolated plugin, and ships a
Docker Compose regression suite that exercises every plugin against a real broker.

OMB sends a configurable workload of messages through a messaging system and records publish rate,
consume rate, backlog, and publish, publish-delay and end-to-end latency. The same workload file can
be run against any supported driver, which makes it possible to compare systems under matching
conditions, provided the drivers are configured with equivalent guarantees (see
[Fair comparisons](methodology/fair-comparisons.md)).

> **Note:** The project does not publish benchmark results and does not plan to publish unilateral
> comparisons. Run the benchmark on hardware and configurations that match your own use case.

## How a run works

```text
          driver YAML + workload YAML
                     |
              bin/benchmark  (coordinator)
              /             \
local mode: one             distributed mode: HTTP to two or more
driver child JVM            bin/benchmark-worker supervisors,
                            each with its own driver child JVM
                     |
              messaging system under test
```

- A **driver configuration** (`driver-*/*.yaml`) selects a plugin through `driverClass` and holds
  broker endpoints and client settings.
- A **workload** (`workloads/*.yaml`) defines topics, partitions, producers, consumers, message
  size, rate and duration.
- The **coordinator** (`bin/benchmark`) creates topics, producers and consumers, drives the load,
  collects statistics every 10 seconds and writes a JSON result.
- **Workers** (`bin/benchmark-worker`) run producers and consumers on other machines. Each worker
  and the local coordinator start the selected driver in a separate JVM containing its plugin
  bundle and any required delegate bundles.

## Where to start

|              You want to              |                                                          Read                                                           |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| Get a first result in a few commands  | [Quickstart](getting-started/quickstart.md)                                                                             |
| Benchmark a broker you already run    | [Using your broker](getting-started/using-your-broker.md)                                                               |
| Understand what the output means      | [Understanding your first run](getting-started/understanding-your-first-run.md)                                         |
| Spread load across machines           | [Distributed runs](getting-started/distributed-runs.md)                                                                 |
| Pick and configure a driver           | [Drivers](drivers/index.md)                                                                                             |
| Design a meaningful experiment        | [Benchmark design](methodology/benchmark-design.md)                                                                     |
| Look up a flag, field or result value | [CLI](reference/cli.md), [Workload schema](reference/workload-schema.md), [Results schema](reference/results-schema.md) |
| Write or package a new driver         | [Driver development](development/architecture.md)                                                                       |

## Supported drivers

|                      Driver                       |    Plugin ID     |                    Page                     |
|---------------------------------------------------|------------------|---------------------------------------------|
| Apache ActiveMQ Artemis (core client)             | `artemis`        | [Artemis](drivers/artemis.md)               |
| Apache BookKeeper via DistributedLog              | `bookkeeper`     | [BookKeeper](drivers/bookkeeper.md)         |
| Apache Kafka                                      | `kafka`          | [Kafka](drivers/kafka.md)                   |
| Kafka-on-Pulsar                                   | `kop`            | [KoP](drivers/kop.md)                       |
| MQTT 5                                            | `mqtt5`          | [MQTT 5](drivers/mqtt5.md)                  |
| Generic JMS (Artemis, Pulsar and Kafka providers) | `jms`            | [JMS](drivers/jms.md)                       |
| NATS JetStream                                    | `nats`           | [NATS](drivers/nats.md)                     |
| NATS Streaming (legacy)                           | `nats-streaming` | [NATS Streaming](drivers/nats-streaming.md) |
| NSQ                                               | `nsq`            | [NSQ](drivers/nsq.md)                       |
| Pravega                                           | `pravega`        | [Pravega](drivers/pravega.md)               |
| Apache Pulsar                                     | `pulsar`         | [Pulsar](drivers/pulsar.md)                 |
| RabbitMQ                                          | `rabbitmq`       | [RabbitMQ](drivers/rabbitmq.md)             |
| Redis Streams                                     | `redis`          | [Redis](drivers/redis.md)                   |
| Apache RocketMQ (remoting client)                 | `rocketmq`       | [RocketMQ](drivers/rocketmq.md)             |
| Apache RocketMQ 5 (gRPC client)                   | `rocketmq5`      | [RocketMQ 5](drivers/rocketmq5.md)          |

The [driver overview](drivers/index.md) lists configuration files, the regression suite's broker
versions and registered cases for each plugin.

## Relationship to upstream

This fork tracks the upstream
[openmessaging/benchmark](https://github.com/openmessaging/benchmark) project. The historical
upstream documentation at [openmessaging.cloud](http://openmessaging.cloud/docs/benchmarks/)
describes older Java versions, distribution names and deployment scripts. When it disagrees with
this site, this site describes the behavior of this repository.
