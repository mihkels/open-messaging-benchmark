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

# Apache Kafka driver

This plugin benchmarks Apache Kafka, or a Kafka-protocol broker, with the Java client
`kafka-clients` 4.4.0: topics are created through the Admin API, messages are sent with
`KafkaProducer` and read by one `KafkaConsumer` per benchmark consumer.

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/kafka/>

## Plugin and configuration files

- Plugin ID: `kafka`
- `driverClass`: `io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver`
- Example configurations in this directory: `kafka-latency.yaml`, `kafka-throughput.yaml`,
  `kafka-no-linger.yaml`, `kafka-sync.yaml`, `kafka-big-batches*.yaml`, `kafka-compression-*.yaml` and
  `kafka-exactly-once.yaml`. The full page compares them.

All shipped files use `bootstrap.servers=localhost:9092`, `replicationFactor: 3` and
`min.insync.replicas=2`, so they need at least three brokers. For a single test broker, copy one and set
`replicationFactor: 1` and `min.insync.replicas=1`.

`kafka-exactly-once.yaml` configures an idempotent producer (`enable.idempotence=true`,
`max.in.flight.requests.per.connection=1`). It does **not** use Kafka transactions, and consumers commit
offsets asynchronously, so it is not end-to-end exactly-once.

## Prerequisites

- A reachable Kafka cluster whose advertised listeners are reachable from the driver.
- Permission to create topics, produce and consume with consumer groups.
- The plugin bundle on the coordinator (local mode) and every worker: `drivers/driver-kafka` in the
  distribution, `driver-kafka/target/omb-plugin` in a source checkout after `mvn -DskipTests install`.

A single-node test broker matching the regression stack:

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

## Usage

Local mode, from the repository root or from `/benchmark` in the container (`my-kafka.yaml` is a copy
of `driver-kafka/kafka-latency.yaml` with replication factor 1):

```bash
bin/benchmark --drivers my-kafka.yaml -o kafka-1-topic.json workloads/1-topic-1-partition-1kb.yaml
```

On a cluster with three or more brokers, pass a shipped file directly, for example
`--drivers driver-kafka/kafka-latency.yaml`. For remote workers, add `--workers` (see
[Distributed runs](https://mihkels.github.io/open-messaging-benchmark/getting-started/distributed-runs/)).

## Smoke check

```bash
e2e-tests/compose/run-e2e.sh --cases kafka-contract
e2e-tests/compose/run-e2e.sh --stacks kafka
```

See the [Compose regression suite](../e2e-tests/compose/README.md). A passing case is a functional
check, not a performance result.

## Limitations

- No Kafka transactions; delivery to consumers is at-least-once.
- Topics (`test-topic-*`) and consumer groups are not deleted after a run.
- `topicConfig`, `commonConfig`, `producerConfig` and `consumerConfig` must all be present.

## Further reading

- [Kafka driver page](https://mihkels.github.io/open-messaging-benchmark/drivers/kafka/)
- [Workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/),
  [Results](https://mihkels.github.io/open-messaging-benchmark/guides/results/),
  [Troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/)
- [Runtime and plugins](https://mihkels.github.io/open-messaging-benchmark/reference/runtime-and-plugins/)

## Legacy deployment files

[`deploy/`](deploy/README.md) contains Terraform and Ansible files for AWS and Alibaba Cloud from the
original project. They are unmaintained: they reference an older distribution, Java version and launcher
settings, and must be adapted to Java 27 and the current distribution and plugin layout before use.
