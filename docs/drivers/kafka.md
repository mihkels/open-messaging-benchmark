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

# Apache Kafka

The Kafka driver benchmarks Apache Kafka, or a broker that speaks the Kafka protocol, with the
official Java client `kafka-clients` 4.4.0. It creates topics through the Kafka Admin API, produces
with `KafkaProducer` and consumes with one `KafkaConsumer` per benchmark consumer.

|       Item        |                                                   Value                                                   |
|-------------------|-----------------------------------------------------------------------------------------------------------|
| Plugin ID         | `kafka`                                                                                                   |
| Module            | `driver-kafka`                                                                                            |
| `driverClass`     | `io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver`                                            |
| Example configs   | 14 files `driver-kafka/kafka-*.yaml`, see [Shipped configurations](#shipped-configurations)               |
| Client library    | `org.apache.kafka:kafka-clients` 4.4.0                                                                    |
| Tested broker     | `apache/kafka:4.4.0`, single KRaft node (Compose stack `kafka`)                                           |
| Regression cases  | `kafka-contract`, `kafka-partitioned-lz4`, `kafka-idempotent-fanout`, `kafka-size-distribution`           |
| Known limitations | No transactions; topics are not deleted after a run; offsets are committed asynchronously (at-least-once) |

The Kafka plugin is also the admin delegate of the [JMS driver](jms.md) for the Confluent Kafka JMS
client, and the [KoP driver](kop.md) reuses its producer and consumer classes.

## Prerequisites and broker setup

The driver needs:

- network access from every worker (or from the local driver process) to the brokers in
  `bootstrap.servers` and to the addresses the brokers advertise;
- permission to create topics through the Admin API, to produce, and to consume with consumer groups;
- enough brokers for the configured `replicationFactor` and `min.insync.replicas`. All shipped
  `kafka-*.yaml` files use `replicationFactor: 3` and `min.insync.replicas=2`, which needs at least
  three brokers.

The plugin bundle (`drivers/driver-kafka` in the distribution, `driver-kafka/target/omb-plugin` in a
source checkout) must be installed on the coordinator for local runs and on every worker. See
[Runtime and plugins](../reference/runtime-and-plugins.md).

### Start a test broker

The regression suite uses
[`e2e-tests/compose/stacks/kafka.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/kafka.yaml).
This command starts the same single-node KRaft broker, with the advertised listener changed to
`localhost:9092` so that a benchmark on the same host can reach it:

```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka:4.4.0
```

The image is multi-arch and runs natively on amd64 and Apple Silicon. A single node is only suitable
for functional checks; for measurements, see [Fair comparisons](../methodology/fair-comparisons.md).

## Minimal configuration and run

A single-broker test needs replication factor 1. Save this as `my-kafka.yaml`; it is
`driver-kafka/kafka-latency.yaml` with the replication settings reduced:

```yaml
name: Kafka
driverClass: io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver

replicationFactor: 1

topicConfig: |
  min.insync.replicas=1

commonConfig: |
  bootstrap.servers=localhost:9092

producerConfig: |
  acks=all
  linger.ms=1
  batch.size=131072

consumerConfig: |
  auto.offset.reset=earliest
  enable.auto.commit=false
  max.partition.fetch.bytes=10485760
```

Set `bootstrap.servers` to your brokers. The right host name depends on where the driver runs:

|            Where the driver runs            |                               Broker address                                |
|---------------------------------------------|-----------------------------------------------------------------------------|
| Source checkout or distribution on the host | `localhost:9092` for a local broker (the shipped files use this)            |
| Benchmark container (`/benchmark`)          | An address reachable from the container; `localhost` is the container       |
| Compose regression suite                    | `kafka:9092`, the Compose service name used in `e2e-tests/compose/drivers/` |

The client connects to the brokers' advertised listeners after bootstrapping, so those addresses must
be reachable from the driver as well.

Run in local mode from the repository root (after `mvn -DskipTests install`) or from `/benchmark`
inside the container; the relative paths are the same in both:

```bash
bin/benchmark --drivers my-kafka.yaml -o kafka-1-topic.json workloads/1-topic-1-partition-1kb.yaml
```

On a cluster with at least three brokers you can pass a shipped file unchanged, for example
`--drivers driver-kafka/kafka-latency.yaml`. `workloads/1-topic-1-partition-1kb.yaml` runs for
15 minutes at 50,000 messages/s; copy it and lower `producerRate` and `testDurationMinutes` for a quick
check (see [Workloads](../guides/workloads.md)). For remote workers, see
[Distributed runs](../getting-started/distributed-runs.md); for pointing the benchmark at an existing
cluster, see [Using your broker](../getting-started/using-your-broker.md).

## Configuration reference

The driver reads these keys (record `io.openmessaging.benchmark.driver.kafka.Config`). Unknown keys
are ignored.

|         Key         |           Type           |    Default     |                                                                     Description                                                                     |
|---------------------|--------------------------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`              | string                   | required       | Used in the result file name and the result's `driver` field.                                                                                       |
| `driverClass`       | string                   | required       | `io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver`                                                                                      |
| `replicationFactor` | short                    | `0` if omitted | Replication factor of every created topic. Set it explicitly: the broker rejects `0`. `-1` asks the broker to use its `default.replication.factor`. |
| `topicConfig`       | Java properties (string) | required       | Topic configs sent with every `CreateTopics` request, for example `min.insync.replicas`.                                                            |
| `commonConfig`      | Java properties (string) | required       | Client properties for the Admin client, every producer and every consumer. Put `bootstrap.servers` and security settings here.                      |
| `producerConfig`    | Java properties (string) | required       | Producer properties, applied on top of `commonConfig`.                                                                                              |
| `consumerConfig`    | Java properties (string) | required       | Consumer properties, applied on top of `commonConfig`. `enable.auto.commit` also selects the driver's commit mode, see [Semantics](#semantics).     |

"Required" for the four properties blocks means that initialization fails with a
`NullPointerException` when the key is missing; an empty value such as `topicConfig: ""` is accepted.

The driver always sets these properties, overriding your values:

- producers: `key.serializer` is `StringSerializer` and `value.serializer` is `ByteArraySerializer`;
- consumers: `key.deserializer` is `StringDeserializer`, `value.deserializer` is
  `ByteArrayDeserializer`, and `group.id` is the benchmark subscription name.

If `commonConfig` contains `client.id`, the placeholder `{zone.id}` in its value is replaced by the
`zone.id` system property of the driver JVM; without that property the value is used unchanged. Pass
it, for example, as `JVM_OPTS="-Dzone.id=zone-a" bin/benchmark-worker -p 8080`. See
[Runtime and plugins](../reference/runtime-and-plugins.md).

All other properties are passed unchanged to the Kafka client. See the official
[producer configs](https://kafka.apache.org/documentation/#producerconfigs),
[consumer configs](https://kafka.apache.org/documentation/#consumerconfigs),
[admin client configs](https://kafka.apache.org/documentation/#adminclientconfigs) and
[topic configs](https://kafka.apache.org/documentation/#topicconfigs).

## Shipped configurations

All files in `driver-kafka/` share `replicationFactor: 3`, topic `min.insync.replicas=2`,
`bootstrap.servers=localhost:9092`, producer `acks=all`, and consumer `auto.offset.reset=earliest`,
`enable.auto.commit=false` and `max.partition.fetch.bytes=10485760`. They differ as follows:

|              File               |        `name`        |                                                            Differences                                                            |
|---------------------------------|----------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `kafka-latency.yaml`            | `Kafka`              | `linger.ms=1`, `batch.size=131072`                                                                                                |
| `kafka-throughput.yaml`         | `Kafka`              | `linger.ms=1`, `batch.size=1048576`; `commonConfig` adds `default.api.timeout.ms=1200000` and `request.timeout.ms=1200000`        |
| `kafka-no-linger.yaml`          | `Kafka`              | `linger.ms=0`, `batch.size=1048576`                                                                                               |
| `kafka-sync.yaml`               | `Kafka`              | `linger.ms=1`, `batch.size=1048576`; topic `flush.messages=1` and `flush.ms=0`, so the broker flushes the log after every message |
| `kafka-big-batches.yaml`        | `Kafka`              | `linger.ms=100`, `batch.size=1048576`, `max.request.size=4194304`                                                                 |
| `kafka-big-batches-gzip.yaml`   | `Kafka`              | `linger.ms=200`, `batch.size=1048576`, `compression.type=gzip`                                                                    |
| `kafka-big-batches-lz4.yaml`    | `Kafka`              | `linger.ms=200`, `batch.size=1048576`, `compression.type=lz4`                                                                     |
| `kafka-big-batches-snappy.yaml` | `Kafka`              | `linger.ms=200`, `batch.size=1048576`, `compression.type=snappy`                                                                  |
| `kafka-big-batches-zstd.yaml`   | `Kafka`              | `linger.ms=200`, `batch.size=1048576`, `compression.type=zstd`                                                                    |
| `kafka-compression-gzip.yaml`   | `Kafka`              | `linger.ms=1`, `batch.size=1048576`, `compression.type=gzip`                                                                      |
| `kafka-compression-lz4.yaml`    | `Kafka`              | `linger.ms=1`, `batch.size=1048576`, `compression.type=lz4`                                                                       |
| `kafka-compression-snappy.yaml` | `Kafka`              | `linger.ms=1`, `batch.size=1048576`, `compression.type=snappy`                                                                    |
| `kafka-compression-zstd.yaml`   | `Kafka`              | `linger.ms=1`, `batch.size=1048576`, `compression.type=zstd`                                                                      |
| `kafka-exactly-once.yaml`       | `Kafka-exactly-once` | `enable.idempotence=true`, `max.in.flight.requests.per.connection=1`, `retries=2147483647`, `linger.ms=1`, `batch.size=1048576`   |

All files except `kafka-exactly-once.yaml` use the name `Kafka`, so the result files of different
variants differ only in their timestamp. Change `name` when you compare variants (see
[Results](../guides/results.md)).

### What `kafka-exactly-once.yaml` guarantees

Despite its name, this file does not configure Kafka exactly-once semantics. It configures an
**idempotent producer** only:

- `enable.idempotence=true` with `acks=all` and unlimited `retries`: the broker discards duplicates
  caused by producer retries, per partition and per producer session.
- `max.in.flight.requests.per.connection=1`: at most one unacknowledged request per broker connection.
  An idempotent producer keeps ordering with up to 5 in-flight requests, so this is stricter than
  required and limits pipelining.

It does not use transactions. The file sets no `transactional.id`, and the driver never calls
`initTransactions`, `beginTransaction` or `commitTransaction`. Consumers do not set `isolation.level`,
and they commit offsets asynchronously after handing a poll's records to the benchmark, so a consumer
can receive messages again after a rebalance or restart. Treat results from this file as "idempotent
producer, at-least-once consumption".

Kafka 4.x producers enable idempotence by default when the other settings allow it (as with
`acks=all`). The measurable difference to `kafka-latency.yaml` therefore comes mostly from
`max.in.flight.requests.per.connection=1`, `retries` and the larger `batch.size`. The regression case
`kafka-idempotent-fanout` uses the same producer settings.

## Semantics

| Benchmark concept |                                                          Kafka implementation                                                          |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Topic             | Kafka topic named `test-topic-<7-digit index>-<random suffix>`                                                                         |
| Partitions        | Kafka partitions; `partitionsPerTopic` is passed to `CreateTopics`                                                                     |
| Producer          | One `KafkaProducer` per benchmark producer                                                                                             |
| Subscription      | Consumer group; `group.id` is the benchmark subscription name                                                                          |
| Consumer          | One `KafkaConsumer` with its own poll thread, subscribed to one topic; consumers of the same subscription share the topic's partitions |
| Message key       | Record key; the Kafka client's default partitioner picks the partition. Without a key (`NO_KEY`) the record key is `null`              |

**Topic creation.** Topics are created with `AdminClient.createTopics` in batches of up to 500, with
`replicationFactor` and `topicConfig` applied to every topic. A topic that already exists counts as
created. Each attempt is limited to 60 seconds; creation is retried up to 6 times with an increasing
back-off, and before each retry the driver checks whether all topics already exist.

**Producing.** `sendAsync` calls `KafkaProducer.send` with a callback. The benchmark counts a message
as published when the callback reports success, so what "published" means for durability is decided by
`acks`, `min.insync.replicas` and the topic's flush settings. Failed sends are counted as publish errors.

**Consuming.** Each consumer polls with a 100 ms timeout. When `enable.auto.commit` in
`consumerConfig` is `false` or not set, the driver commits the offsets of every non-empty poll with
`commitAsync` after passing all its records to the benchmark. When it is `true`, the driver does not
commit and the client's auto-commit is used. Both modes are at-least-once.

**End-to-end latency.** The consumer reports `ConsumerRecord.timestamp()` as the publish time. The
producer does not set a timestamp, so with the default `message.timestamp.type=CreateTime` it is the
producer host's wall clock when `send` was called. Latency between hosts therefore depends on clock
synchronization. With `message.timestamp.type=LogAppendTime` in `topicConfig`, the broker's append time
is used instead.

**Backlog workloads.** The worker pauses consumers for backlog builds by blocking inside the message
callback. During the pause, the consumer thread does not call `poll`. If the pause is longer than the
consumer's `max.poll.interval.ms` (Kafka default: 5 minutes), the client leaves its group and rejoins
when the pause ends, causing a rebalance. See [Latency and backlog](../methodology/latency-and-backlog.md).

**Cleanup.** At the end of a run the driver closes its producers, consumers and Admin client. It does
not delete topics or consumer groups; remove them yourself or rely on retention.

Workload features that work: several topics, partitions, keys (`keyDistributor`), several
subscriptions per topic, several consumers per subscription, variable message sizes and backlog builds.
The regression cases cover all of them except backlog builds.

## Tuning examples

All tuning is done with pass-through client and topic properties. Change one setting at a time and
keep the broker side identical between compared runs (see
[Fair comparisons](../methodology/fair-comparisons.md)).

- **Batching versus latency.** `linger.ms` and `batch.size` in `producerConfig` trade per-message
  latency for larger requests. Compare `kafka-no-linger.yaml`, `kafka-latency.yaml` and
  `kafka-big-batches.yaml`.
- **Compression.** `compression.type` reduces network and disk bytes and costs producer and consumer
  CPU. It usually works better on larger batches, which is why the shipped files offer short-linger
  (`kafka-compression-*`) and long-linger (`kafka-big-batches-*`) variants. How compressible the data
  is depends on the payload (see [Payloads and keys](../guides/payloads-and-keys.md)).
- **Durability.** `acks`, `replicationFactor` and `min.insync.replicas` decide how many replicas must
  hold a message before it counts as published. `flush.messages` and `flush.ms` in `topicConfig`, as in
  `kafka-sync.yaml`, force log flushes and change the broker's disk behaviour substantially.
- **Fetching.** `max.partition.fetch.bytes`, `fetch.min.bytes` and `fetch.max.wait.ms` in
  `consumerConfig` control how much data a poll returns and how long the broker waits to fill it.
- **Many topics.** `kafka-throughput.yaml` raises `default.api.timeout.ms` and `request.timeout.ms`
  in `commonConfig`; this also applies to the Admin client that creates the topics.

## Troubleshooting and verification

Run the Kafka regression cases (Docker with Compose v2 required):

```bash
e2e-tests/compose/run-e2e.sh --cases kafka-contract
e2e-tests/compose/run-e2e.sh --stacks kafka
```

A passing case is a functional check at a low rate, not a performance result. See the
[Compose regression suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md).

|                                       Symptom                                        |                                                              Cause and fix                                                              |
|--------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `Failed to create topics after 6 attempts`, with `InvalidReplicationFactorException` | `replicationFactor` is larger than the number of brokers, or `0` because the key is missing. Use `1` for a single broker.               |
| `NotEnoughReplicasException` when producing                                          | `min.insync.replicas` in `topicConfig` is higher than the number of in-sync replicas. Use `min.insync.replicas=1` with a single broker. |
| `NullPointerException` during initialization                                         | One of `topicConfig`, `commonConfig`, `producerConfig` or `consumerConfig` is missing.                                                  |
| Bootstrap succeeds, then requests time out or a host name cannot be resolved         | The broker's advertised listener is not reachable from the driver, for example a container host name. Fix `advertised.listeners`.       |
| `Timed out waiting for consumers to be ready`                                        | Consumers could not join their group or fetch. Check reachability of all brokers and the driver log.                                    |
| `test-topic-*` topics accumulate on the cluster                                      | The driver never deletes topics. Delete them manually.                                                                                  |

See also [Troubleshooting](../guides/troubleshooting.md).

Source and configuration files:

- Driver source: [`driver-kafka/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-kafka/src/main/java/io/openmessaging/benchmark/driver/kafka)
- Shipped configurations: [`driver-kafka/`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-kafka)
- Regression driver configs:
  [`kafka.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/kafka.yaml),
  [`kafka-lz4.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/kafka-lz4.yaml),
  [`kafka-idempotent.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/kafka-idempotent.yaml)
- Compose stack: [`e2e-tests/compose/stacks/kafka.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/kafka.yaml)
- Legacy Terraform and Ansible files: [`driver-kafka/deploy/`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-kafka/deploy).
  They reference an older distribution, Java version and launcher settings and are not maintained for
  this fork. Adapt them to Java 27 and the current distribution and plugin layout before use.

