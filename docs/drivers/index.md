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
| [NATS Streaming](nats-streaming.md)      | `nats-streaming` | `io.openmessaging.benchmark.driver.natsStreaming.NatsStreamingBenchmarkDriver` | `nats-streaming.yaml`                                                 |
| [NSQ](nsq.md)                            | `nsq`            | `io.openmessaging.benchmark.driver.nsq.NsqBenchmarkDriver`                     | `nsq.yaml`                                                            |
| [Redis Streams](redis.md)                | `redis`          | `io.openmessaging.benchmark.driver.redis.RedisBenchmarkDriver`                 | `redis.yaml`                                                          |
| [Apache RocketMQ](rocketmq.md)           | `rocketmq`       | `io.openmessaging.benchmark.driver.rocketmq.RocketMQBenchmarkDriver`           | `driver-rocketmq/rocketmq.yaml`                                       |
| [Apache RocketMQ 5 (gRPC)](rocketmq5.md) | `rocketmq5`      | `io.openmessaging.benchmark.driver.rocketmq.RocketMQ5BenchmarkDriver`          | `driver-rocketmq5/rocketmq.yaml`                                      |
| [MQTT 5](mqtt5.md)                       | `mqtt5`          | `io.openmessaging.benchmark.driver.mqtt5.MqttBenchmarkDriver`                  | `mqtt5.yaml`                                                          |

Shipped configuration files are in the driver's `driver-<id>/` directory. They are examples: their
broker addresses, replication settings and cluster names usually need changing for your
environment, and several of them share the same `name`. Each driver page explains every setting.

> **Note:** The JMS driver needs a JMS provider's client jars, which are not shipped. Copy them into
> `drivers/driver-jms/ext/`; see [JMS](jms.md).
>
> **Note:** NATS Streaming is deprecated by its maintainers. Prefer the NATS JetStream driver for new
> work.

## Regression suite versions

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

## Writing a new driver

See [Implementing a driver](../development/implementing-a-driver.md).
