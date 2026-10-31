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

# Kafka-on-Pulsar (KoP) driver

This plugin benchmarks a Pulsar broker running the KoP (Kafka-on-Pulsar) protocol handler. Producers
and consumers each use either the Kafka client (`kafka-clients` 4.4.0) or the Pulsar client
(`pulsar-client-all` 4.2.5). Topics are created through KoP's Kafka Admin API.

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/kop/>

## Plugin and configuration files

- Plugin ID: `kop`
- `driverClass`: `io.openmessaging.benchmark.driver.kop.KopBenchmarkDriver`
- Configurations in this directory:

|          File          | Producer | Consumer |
|------------------------|----------|----------|
| `kafka_to_kafka.yaml`  | Kafka    | Kafka    |
| `kafka_to_pulsar.yaml` | Kafka    | Pulsar   |
| `pulsar_to_kafka.yaml` | Pulsar   | Kafka    |

All three use `localhost:9092` and `pulsar://localhost:6650`, and all three have the same `name`
(`Kafka producer and Pulsar consumer`); set a distinct `name` to tell results apart.

## Prerequisites

- A Pulsar broker with the KoP protocol handler NAR in its `protocolHandlerDirectory` and
  `messagingProtocols=kafka`. The KoP version must match the Pulsar version. The regression suite uses
  `apachepulsar/pulsar:2.10.4` with `pulsar-protocol-handler-kafka-2.10.4.6.nar` (artifact `kop-nar` in
  [`e2e-tests/compose/artifacts.txt`](../e2e-tests/compose/artifacts.txt)).
- `kafkaTransactionCoordinatorEnabled=true` and `brokerDeduplicationEnabled=true` on the broker, because
  Kafka 3.x and later producers are idempotent by default.
- `brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor`,
  `allowAutoTopicCreationType=partitioned`, and `kafkaAdvertisedListeners` reachable from the driver.
- The default KoP `kafkaTenant`/`kafkaNamespace` (`public/default`), so Kafka and Pulsar clients address
  the same topics.
- The plugin bundle on the coordinator (local mode) and every worker: `drivers/driver-kop` in the
  distribution, `driver-kop/target/omb-plugin` in a source checkout after `mvn -DskipTests install`. It
  includes the Kafka and Pulsar driver code; no other plugin is needed.

The tested broker image is linux/amd64 only. See
[`e2e-tests/compose/stacks/kop.yaml`](../e2e-tests/compose/stacks/kop.yaml) for a complete broker
definition, and the full page for a `docker run` equivalent.

## Usage

Local mode, from the repository root or from `/benchmark` in the container:

```bash
bin/benchmark --drivers driver-kop/kafka_to_kafka.yaml -o kop-kafka-to-kafka.json \
  workloads/1-topic-1-partition-1kb.yaml
```

Replace `localhost` with addresses reachable from where the driver runs. For remote workers, add
`--workers` (see
[Distributed runs](https://mihkels.github.io/open-messaging-benchmark/getting-started/distributed-runs/)).

## Smoke check

```bash
e2e-tests/compose/run-e2e.sh --stacks kop
e2e-tests/compose/run-e2e.sh --cases kop-kafka-to-pulsar --case-timeout 600
```

The runner downloads and verifies the KoP NAR. On Apple Silicon the broker runs under emulation; use
`--case-timeout 600`. See the [Compose regression suite](../e2e-tests/compose/README.md). A passing case
is a functional check, not a performance result.

## Limitations

- Topics are created with replication factor 1 and are never deleted.
- Pulsar consumers use a Failover subscription with one consumer per partition; extra consumers on the
  same subscription are standbys.
- `kafkaConfig` is applied to the Admin client, producers and consumers alike.
- Kafka consumers commit with `commitAsync`; the client's `enable.auto.commit` default (`true`) also
  applies unless set in `kafkaConfig`.
- Only one topic, one partition and one subscription per combination are covered by regression cases.

## Further reading

- [KoP driver page](https://mihkels.github.io/open-messaging-benchmark/drivers/kop/)
- [Kafka driver](https://mihkels.github.io/open-messaging-benchmark/drivers/kafka/),
  [Pulsar driver](https://mihkels.github.io/open-messaging-benchmark/drivers/pulsar/)
- [Workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/),
  [Troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/)

