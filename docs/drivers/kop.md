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

# Kafka-on-Pulsar (KoP)

The KoP driver benchmarks an Apache Pulsar broker that runs the
[KoP (Kafka-on-Pulsar)](https://github.com/streamnative/kop) protocol handler. Producers and consumers
can each use either the Kafka client or the Pulsar client, so the driver can measure Kafka-to-Kafka
traffic through KoP and the conversions between the Kafka and Pulsar protocols. Topics are always
created through KoP's Kafka Admin API.

|       Item        |                                                                         Value                                                                         |
|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID         | `kop`                                                                                                                                                 |
| Module            | `driver-kop`                                                                                                                                          |
| `driverClass`     | `io.openmessaging.benchmark.driver.kop.KopBenchmarkDriver`                                                                                            |
| Example configs   | `driver-kop/kafka_to_kafka.yaml`, `driver-kop/kafka_to_pulsar.yaml`, `driver-kop/pulsar_to_kafka.yaml`                                                |
| Client libraries  | `kafka-clients` 4.4.0 (from the Kafka driver), `pulsar-client-all` 4.2.5 (from the Pulsar driver), `kafka-payload-processor` 4.1.0.10                 |
| Tested broker     | `apachepulsar/pulsar:2.10.4` standalone with the KoP 2.10.4.6 protocol handler (Compose stack `kop`)                                                  |
| Regression cases  | `kop-kafka-to-kafka`, `kop-kafka-to-pulsar`, `kop-pulsar-to-kafka`                                                                                    |
| Known limitations | Tested broker image is linux/amd64 only; topics are not deleted; replication is not configurable; Pulsar consumers always use a Failover subscription |

The plugin bundle contains both the Kafka and the Pulsar driver jars and clients, so it is
self-contained; you do not need to install the `kafka` or `pulsar` plugins for it.

## Prerequisites and broker setup

The broker must be a Pulsar broker with the KoP protocol handler loaded and a Kafka listener that the
driver can reach. The Compose stack
[`e2e-tests/compose/stacks/kop.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/kop.yaml)
sets these broker options (as `PULSAR_PREFIX_*` environment variables applied to `standalone.conf`):

|            Broker setting            |                         Value in the stack                          |                                                              Why                                                              |
|--------------------------------------|---------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `messagingProtocols`                 | `kafka`                                                             | Loads the KoP protocol handler.                                                                                               |
| `protocolHandlerDirectory`           | `/pulsar/protocols`                                                 | Directory containing the KoP NAR file.                                                                                        |
| `kafkaListeners`                     | `PLAINTEXT://0.0.0.0:9092`                                          | Kafka protocol listener.                                                                                                      |
| `kafkaAdvertisedListeners`           | `PLAINTEXT://pulsar-kop:9092`                                       | Address returned to Kafka clients; must be reachable from the driver.                                                         |
| `allowAutoTopicCreationType`         | `partitioned`                                                       | KoP works with partitioned topics.                                                                                            |
| `brokerEntryMetadataInterceptors`    | `org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor` | Required by KoP for Kafka offsets.                                                                                            |
| `brokerDeleteInactiveTopicsEnabled`  | `false`                                                             | Keeps idle benchmark topics.                                                                                                  |
| `kafkaTransactionCoordinatorEnabled` | `true`                                                              | Kafka 3.x and later producers are idempotent by default; KoP serves idempotent producers through its transaction coordinator. |
| `brokerDeduplicationEnabled`         | `true`                                                              | Also required by KoP for idempotent Kafka producers.                                                                          |

Without the transaction coordinator and broker deduplication, KoP rejects the default (idempotent)
Kafka producer. Alternatively, set `enable.idempotence=false` in `kafkaConfig`; that configuration is
not covered by the regression suite.

**Protocol handler NAR.** KoP is not part of the Pulsar image. The regression suite downloads the pinned
artifact `kop-nar` from
[`e2e-tests/compose/artifacts.txt`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/artifacts.txt)
(`pulsar-protocol-handler-kafka-2.10.4.6.nar`, sha256-checked) and mounts its directory at
`/pulsar/protocols`. If the download fails, the `kop` stack's cases fail. The KoP version must match the
Pulsar broker version.

**Platform.** `apachepulsar/pulsar:2.10.4` is published for linux/amd64 only. On Apple Silicon it runs
under emulation, which is slow; if the cases time out, pass `--case-timeout 600` to `run-e2e.sh`.

### Start a test broker

Derived from the stack file, with the advertised listener changed to `localhost`:

```bash
mkdir -p kop-protocols
curl -fsSL -o kop-protocols/pulsar-protocol-handler-kafka-2.10.4.6.nar \
  https://github.com/streamnative/kop/releases/download/v2.10.4.6/pulsar-protocol-handler-kafka-2.10.4.6.nar

docker run -d --name pulsar-kop --platform linux/amd64 \
  -p 6650:6650 -p 8080:8080 -p 9092:9092 \
  -v "$PWD/kop-protocols:/pulsar/protocols:ro" \
  -e PULSAR_MEM="-Xms256m -Xmx1g -XX:MaxDirectMemorySize=512m" \
  -e PULSAR_PREFIX_messagingProtocols=kafka \
  -e PULSAR_PREFIX_protocolHandlerDirectory=/pulsar/protocols \
  -e PULSAR_PREFIX_allowAutoTopicCreationType=partitioned \
  -e PULSAR_PREFIX_kafkaListeners=PLAINTEXT://0.0.0.0:9092 \
  -e PULSAR_PREFIX_kafkaAdvertisedListeners=PLAINTEXT://localhost:9092 \
  -e PULSAR_PREFIX_brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor \
  -e PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled=false \
  -e PULSAR_PREFIX_kafkaTransactionCoordinatorEnabled=true \
  -e PULSAR_PREFIX_brokerDeduplicationEnabled=true \
  apachepulsar/pulsar:2.10.4 \
  sh -c 'bin/apply-config-from-env.py conf/standalone.conf && exec bin/pulsar standalone -nfw -nss'
```

Check the published sha256 in `artifacts.txt` against the downloaded file. Standalone mode is for
functional checks only; see [Fair comparisons](../methodology/fair-comparisons.md) before measuring.

## Minimal configuration and run

`driver-kop/kafka_to_kafka.yaml` with only the required settings:

```yaml
name: KoP-kafka-to-kafka
driverClass: io.openmessaging.benchmark.driver.kop.KopBenchmarkDriver

producerType: kafka
consumerType: kafka

kafkaConfig: |
  bootstrap.servers=localhost:9092
  linger.ms=1
  batch.size=1048576
```

When `producerType` or `consumerType` is `pulsar`, also set the Pulsar service URL:

```yaml
pulsarConfig:
  serviceUrl: pulsar://localhost:6650
```

Host names to change:

|            Where the driver runs            |                           `bootstrap.servers` / `serviceUrl`                            |
|---------------------------------------------|-----------------------------------------------------------------------------------------|
| Source checkout or distribution on the host | `localhost:9092` / `pulsar://localhost:6650` (as in the shipped files)                  |
| Benchmark container (`/benchmark`)          | Addresses reachable from the container, not `localhost`                                 |
| Compose regression suite                    | `pulsar-kop:9092` / `pulsar://pulsar-kop:6650` (`e2e-tests/compose/drivers/kop-*.yaml`) |

`kafkaAdvertisedListeners` on the broker must also be reachable from the driver.

Run in local mode from the repository root (after `mvn -DskipTests install`) or from `/benchmark` in
the container:

```bash
bin/benchmark --drivers driver-kop/kafka_to_kafka.yaml -o kop-kafka-to-kafka.json \
  workloads/1-topic-1-partition-1kb.yaml
```

This workload runs for 15 minutes at 50,000 messages/s; copy it and lower `producerRate` and
`testDurationMinutes` for a quick check (see [Workloads](../guides/workloads.md)). For remote workers,
see [Distributed runs](../getting-started/distributed-runs.md); for an existing cluster, see
[Using your broker](../getting-started/using-your-broker.md).

> **Note:** All three shipped files use the same `name`, `Kafka producer and Pulsar consumer`, even
> `kafka_to_kafka.yaml` and `pulsar_to_kafka.yaml`. Set a distinct `name` so that result files and the
> result's `driver` field identify the producer and consumer types.

## Configuration reference

The driver reads these keys (record `io.openmessaging.benchmark.driver.kop.config.Config`). Unknown
keys are ignored.

|       Key       |           Type           |  Default  |                                                                                          Description                                                                                           |
|-----------------|--------------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`          | string                   | required  | Used in the result file name and the result's `driver` field.                                                                                                                                  |
| `driverClass`   | string                   | required  | `io.openmessaging.benchmark.driver.kop.KopBenchmarkDriver`                                                                                                                                     |
| `producerType`  | `kafka` or `pulsar`      | `kafka`   | Client used by benchmark producers.                                                                                                                                                            |
| `consumerType`  | `kafka` or `pulsar`      | `kafka`   | Client used by benchmark consumers.                                                                                                                                                            |
| `kafkaConfig`   | Java properties (string) | required  | Kafka client properties. Must contain `bootstrap.servers`. The same properties are used for the Admin client, the Kafka producers and the Kafka consumers, also when only one side uses Kafka. |
| `pollTimeoutMs` | long                     | `100`     | Poll timeout of Kafka consumers, in milliseconds. A non-numeric value falls back to `100`.                                                                                                     |
| `pulsarConfig`  | object                   | see below | Pulsar client settings; only used for the side(s) set to `pulsar`.                                                                                                                             |

`pulsarConfig` keys:

|                     Key                     |  Type   |          Default          | Used by  |                                     Description                                     |
|---------------------------------------------|---------|---------------------------|----------|-------------------------------------------------------------------------------------|
| `serviceUrl`                                | string  | `pulsar://localhost:6650` | both     | Pulsar binary protocol URL. One Pulsar client is shared by producers and consumers. |
| `batchingEnabled`                           | boolean | `true`                    | producer | Pulsar producer batching.                                                           |
| `batchingMaxPublishDelayMs`                 | int     | `1`                       | producer | Maximum batching delay, in milliseconds.                                            |
| `batchingMaxBytes`                          | int     | `131072`                  | producer | Maximum batch size, in bytes.                                                       |
| `blockIfQueueFull`                          | boolean | `true`                    | producer | Block `sendAsync` instead of failing when the pending queue is full.                |
| `pendingQueueSize`                          | int     | `1000`                    | producer | `maxPendingMessages`, in messages.                                                  |
| `maxPendingMessagesAcrossPartitions`        | int     | `50000`                   | producer | Pending messages across all partitions, in messages.                                |
| `receiverQueueSize`                         | int     | `1000`                    | consumer | Receiver queue per consumer, in messages.                                           |
| `maxTotalReceiverQueueSizeAcrossPartitions` | int     | `50000`                   | consumer | Total receiver queue across partitions, in messages.                                |

If `kafkaConfig` is missing or has no `bootstrap.servers`, initialization fails with
`bootstrap.servers is not set`.

The driver always sets these Kafka properties, overriding `kafkaConfig`: key and value
(de)serializers (`String` keys, `byte[]` values), `group.id` = the benchmark subscription name, and
`auto.offset.reset=earliest` for consumers.

Pass-through properties are documented in the official
[Kafka producer configs](https://kafka.apache.org/documentation/#producerconfigs),
[Kafka consumer configs](https://kafka.apache.org/documentation/#consumerconfigs) and the
[KoP configuration](https://github.com/streamnative/kop/blob/master/docs/configuration.md). Other Pulsar
client settings are not configurable in this driver.

### Shipped configurations

|          File          | `producerType` | `consumerType` |                        `pulsarConfig` in the file                         |    Regression case    |
|------------------------|----------------|----------------|---------------------------------------------------------------------------|-----------------------|
| `kafka_to_kafka.yaml`  | `kafka`        | `kafka`        | All producer and consumer keys (unused, both sides are Kafka)             | `kop-kafka-to-kafka`  |
| `kafka_to_pulsar.yaml` | `kafka`        | `pulsar`       | `serviceUrl`, `batchingMaxPublishDelayMs: 1`, `batchingMaxBytes: 1048576` | `kop-kafka-to-pulsar` |
| `pulsar_to_kafka.yaml` | `pulsar`       | `kafka`        | `serviceUrl`, `batchingMaxPublishDelayMs: 1`, `batchingMaxBytes: 1048576` | `kop-pulsar-to-kafka` |

All three use `kafkaConfig` with `bootstrap.servers=localhost:9092`, `linger.ms=1` and
`batch.size=1048576`. The regression configs in `e2e-tests/compose/drivers/kop-*.yaml` set every
`pulsarConfig` key explicitly.

## Semantics

| Benchmark concept |                                                                             Implementation                                                                             |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Topic             | Created with the Kafka Admin API (`CreateTopics`) as `test-topic-<7-digit index>-<random suffix>` with `partitionsPerTopic` partitions and replication factor 1        |
| Partitions        | Kafka partitions, stored by KoP as a partitioned Pulsar topic                                                                                                          |
| Kafka producer    | One `KafkaProducer` per benchmark producer; message key = record key                                                                                                   |
| Pulsar producer   | One Pulsar producer per benchmark producer on the bare topic name; message key = Pulsar message key                                                                    |
| Kafka consumer    | One `KafkaConsumer` per benchmark consumer; consumer group = subscription name                                                                                         |
| Pulsar consumer   | For each partition of the topic, one Pulsar consumer on subscription = subscription name, type `Failover`, with `KafkaPayloadProcessor` to decode Kafka-format entries |

**Topic namespace.** The Kafka side uses KoP's default tenant and namespace for Kafka clients; the
Pulsar side uses the bare topic name, which Pulsar resolves to `persistent://public/default/<topic>`. The
regression stack leaves KoP's `kafkaTenant` and `kafkaNamespace` at their defaults. If you change them,
the Pulsar clients no longer address the same topics.

**Replication.** The driver requests replication factor 1 from KoP; data durability is decided by the
Pulsar namespace persistence settings on the broker, which this driver does not change.

**Producing.** Kafka producers behave as in the [Kafka driver](kafka.md#semantics): a message counts as
published when the send callback succeeds. Pulsar producers count a message as published when
`sendAsync` completes.

**Consuming.** Kafka consumers use the Kafka driver's consumer loop, polling with `pollTimeoutMs`. The
driver commits offsets with `commitAsync` after each non-empty poll. Because the driver does not set
`enable.auto.commit`, the Kafka client's default (`true`) also applies unless you set it in
`kafkaConfig`. Pulsar consumers acknowledge every message individually and asynchronously after the
benchmark callback. With a Failover subscription, only one consumer per partition receives messages;
additional consumers on the same subscription are standbys.

Kafka consumer creation waits for the initial group assignment and positions as in the
[Kafka driver](kafka.md#semantics); normal subsequent rebalance and offset-reset behavior is unchanged.
Pulsar consumer creation completes after all partition consumers have been created.

**End-to-end latency.** Kafka consumers report `ConsumerRecord.timestamp()` as the publish time, which
is the producer timestamp for Kafka producers and the timestamp KoP derives for Pulsar-produced
messages. Pulsar consumers report the message's publish time (`Message.getPublishTime()`). Both are set
from the producing host's clock, so cross-host latency depends on clock synchronization.

**Cleanup.** The benchmark closes successfully created producer and consumer handles; the driver closes
the shared Admin and Pulsar clients. Failed or cancelled creation closes untransferred handles,
including late partition consumers. Topics and subscriptions are not deleted; the stack disables
inactive-topic deletion.

Workload features: the regression suite covers one topic with one partition and one subscription for
each producer/consumer combination. Partitions, keys and several subscriptions are implemented as
described above but are not covered by a regression case. Backlog builds pause consumers inside the
callback (see [Latency and backlog](../methodology/latency-and-backlog.md)); the Kafka consumer's
`max.poll.interval.ms` caveat from the [Kafka driver](kafka.md#semantics) applies.

## Tuning examples

- **Kafka batching.** `linger.ms` and `batch.size` in `kafkaConfig` trade latency for request size, as
  with Apache Kafka. They apply to the Kafka producer only; the Kafka client logs unknown-config warnings
  when they are also passed to consumers and the Admin client.
- **Pulsar batching.** `batchingEnabled`, `batchingMaxPublishDelayMs` and `batchingMaxBytes` control the
  Pulsar producer in `pulsar_to_kafka` runs.
- **Pulsar flow control.** `pendingQueueSize`, `maxPendingMessagesAcrossPartitions` and
  `blockIfQueueFull` limit outstanding sends; `receiverQueueSize` and
  `maxTotalReceiverQueueSizeAcrossPartitions` limit prefetching by Pulsar consumers. Larger queues allow
  more in-flight messages and use more client memory.
- **Kafka polling.** `pollTimeoutMs` sets how long a Kafka consumer waits in `poll` when no data is
  available.

Compare producer/consumer combinations only with the broker configuration unchanged; see
[Fair comparisons](../methodology/fair-comparisons.md).

## Troubleshooting and verification

Run the KoP regression cases; the runner downloads the pinned NAR automatically:

```bash
e2e-tests/compose/run-e2e.sh --stacks kop
e2e-tests/compose/run-e2e.sh --cases kop-kafka-to-kafka --case-timeout 600
```

On Apple Silicon, use `--case-timeout 600` because the broker runs under amd64 emulation. A passing
case is a functional check at a low rate, not a performance result. See the
[Compose regression suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md).

|                          Symptom                           |                                                       Cause and fix                                                       |
|------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `bootstrap.servers is not set`                             | `kafkaConfig` is missing or has no `bootstrap.servers`.                                                                   |
| Kafka producer fails on initialization or every send fails | KoP rejects idempotent producers unless `kafkaTransactionCoordinatorEnabled` and `brokerDeduplicationEnabled` are `true`. |
| Connection to port 9092 refused                            | The KoP NAR was not loaded (`messagingProtocols`, `protocolHandlerDirectory`), or KoP and Pulsar versions do not match.   |
| Kafka client times out after bootstrapping                 | `kafkaAdvertisedListeners` is not reachable from the driver.                                                              |
| `kop` stack cases fail before the benchmark starts         | The `kop-nar` artifact could not be downloaded; check network access to GitHub releases.                                  |
| Cases time out on Apple Silicon                            | Emulated amd64 image; pass `--case-timeout 600`.                                                                          |
| Pulsar consumer receives nothing                           | KoP is configured with a non-default `kafkaTenant` or `kafkaNamespace`, so the Pulsar clients address a different topic.  |

See also [Troubleshooting](../guides/troubleshooting.md).

Source and configuration files:

- Driver source: [`driver-kop/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-kop/src/main/java/io/openmessaging/benchmark/driver/kop)
- Shipped configurations: [`driver-kop/`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-kop)
- Regression driver configs:
  [`kop-kafka-to-kafka.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/kop-kafka-to-kafka.yaml),
  [`kop-kafka-to-pulsar.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/kop-kafka-to-pulsar.yaml),
  [`kop-pulsar-to-kafka.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/kop-pulsar-to-kafka.yaml)
- Compose stack: [`e2e-tests/compose/stacks/kop.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/kop.yaml)
- Related drivers: [Kafka](kafka.md), [Pulsar](pulsar.md)

