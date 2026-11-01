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

# NATS JetStream

The NATS driver benchmarks [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream), the persistence layer
built into the NATS server. It uses the `jnats` client (2.26.4). Each benchmark topic is a JetStream stream and each
subscription is a durable push consumer with a deliver group.

This driver is not the NATS Streaming (STAN) driver. NATS Streaming is a separate, deprecated product with its own
server and client; see [NATS Streaming](nats-streaming.md).

|          Item          |                                                                                Value                                                                                |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID              | `nats`                                                                                                                                                              |
| Module                 | `driver-nats`                                                                                                                                                       |
| `driverClass`          | `io.openmessaging.benchmark.driver.nats.NatsBenchmarkDriver`                                                                                                        |
| Example config shipped | `driver-nats/nats.yaml` (`localhost:4222`, `replicationFactor: 3`, file storage by default)                                                                         |
| Tested broker          | `nats:2.11-alpine` with `-js` (Compose stack `nats`, NATS 2.11 with JetStream)                                                                                      |
| Regression cases       | `nats-contract` (file storage), `nats-memory-fanout` (memory storage, 2 subjects x 2 durable consumers)                                                             |
| Known limitations      | Partitions and keys are ignored; the payload's first 8 bytes carry the publish timestamp (messages must be at least 8 bytes); streams and consumers are not deleted |

## Prerequisites and broker setup

The driver needs:

- A NATS server or cluster with JetStream enabled, reachable on its client port (4222 by default).
- Enough JetStream servers for `replicationFactor`. The shipped `nats.yaml` uses `replicationFactor: 3`, which needs
  a JetStream cluster of at least three servers; a single server needs `replicationFactor: 1`.
- Permission for the connecting user to create streams and consumers (JetStream API) and to publish and subscribe
  on the benchmark subjects and the deliver subjects (`delivery-subject-*`).

Nothing has to be created in advance. The driver creates one stream per topic and one durable consumer per
subscription.

The regression suite starts NATS from
[`e2e-tests/compose/stacks/nats.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/nats.yaml).
A comparable single server with JetStream, file storage in `/data` and the monitoring port:

```bash
docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:2.11-alpine -js -sd /data -m 8222
```

The image is multi-arch and runs natively on Apple Silicon. `http://localhost:8222/healthz?js-enabled-only=true`
reports whether JetStream is enabled.

## Minimal configuration and benchmark command

The shipped `driver-nats/nats.yaml`:

```yaml
name: NATS
driverClass: io.openmessaging.benchmark.driver.nats.NatsBenchmarkDriver

natsHostUrl: localhost:4222

replicationFactor: 3
```

For the single-server `docker run` above, set `replicationFactor: 1`. A minimal single-server configuration with
explicit storage:

```yaml
name: NATS
driverClass: io.openmessaging.benchmark.driver.nats.NatsBenchmarkDriver
natsHostUrl: nats://localhost:4222
replicationFactor: 1
storageType: file
```

Run a local-mode benchmark from the repository root (after a full `mvn -DskipTests install`) or from `/benchmark`
in the container image, using your edited copy of the driver file:

```bash
bin/benchmark --drivers driver-nats/nats.yaml workloads/1-topic-1-partition-1kb.yaml
```

Host names depend on where the benchmark runs:

- Source checkout or distribution on the machine running the `docker run` server above: `localhost` works.
- Benchmark inside a container: `localhost` is the container itself; use a host name the container can resolve.
- Compose regression suite: [`nats-file.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/nats-file.yaml)
  and [`nats-memory.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/nats-memory.yaml)
  use `nats://nats:4222`.

For remote workers add `--workers` or `--workers-file`; see [Distributed runs](../getting-started/distributed-runs.md).
Install the plugin bundle on every worker, see [Runtime and plugins](../reference/runtime-and-plugins.md), and
[Using your broker](../getting-started/using-your-broker.md) for adapting a shipped configuration.

## Configuration reference

All keys are read by `NatsConfig`. Unknown keys are ignored.

|         Key         |  Type  | Default  |                                                                Description                                                                |
|---------------------|--------|----------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `natsHostUrl`       | string | required | Server URL, for example `nats://host:4222`. One URL; the driver opens one connection per worker.                                          |
| `replicationFactor` | int    | `0`      | Stream replicas. Must be set to a value from 1 to 5; the default 0 is rejected by the client (`Replicas must be from 1 to 5 inclusive.`). |
| `storageType`       | enum   | `file`   | Stream storage: `file` or `memory`. Values must be lower case; `File` or `Memory` are not accepted.                                       |
| `maxBytes`          | int    | `-1`     | Stream `max_bytes` limit in bytes; `-1` means unlimited. A 32-bit integer, so the largest finite value is 2147483647 bytes (about 2 GiB). |

Connection settings are fixed in the driver: 5 s connection timeout, up to 5 reconnect attempts 1 s apart, and a
60 s ping interval. There is no user, token, credentials-file or TLS setting, and no pass-through client properties.
Other stream and consumer settings are server and client defaults; see the
[JetStream documentation](https://docs.nats.io/nats-concepts/jetstream).

## Semantics

**Topics and partitions.** Topic names start with `Nats-benchmark` followed by an index and a random suffix.
`createTopic` creates a stream named after the topic, with the topic as its only subject, the configured storage,
replicas and `maxBytes`. Other stream settings (retention, discard policy, limits) are defaults. `partitionsPerTopic`
is ignored: every topic is one stream with one subject.

**Producers.** A send publishes to the topic's subject with JetStream `publishAsync` and completes successfully
when the server returns the publish acknowledgement, that is when the message is stored in the stream. Keys are
ignored. Closing a producer fails its unacknowledged sends and rejects further sends without closing the shared
JetStream connection. A publish in flight at close may still reach the stream even if its send future fails.
Closing from a send completion callback is rejected and may be retried after the callback returns.

**Subscriptions and consumers.** For each consumer the driver creates or updates a durable consumer named
`durable-<subscription>` on the stream, filtered on the topic subject, with deliver subject
`delivery-subject-<subscription>` and deliver group `group-<subscription>`, and subscribes to it as a push
subscription. Consumers of the same subscription join the same deliver group and share messages. Each subscription
is its own durable consumer and receives every message, so multiple subscriptions per topic work.

**Acknowledgement.** Auto-ack is disabled. Each message is acknowledged with `ack()` after the benchmark callback.
The ack policy, ack wait and maximum pending acknowledgements are consumer defaults.

**End-to-end latency.** The producer copies the payload for each send and writes `System.currentTimeMillis()` into
the first 8 bytes of the copy (big-endian), which the consumer reads back. The caller's payload is not modified;
on the wire, the timestamp replaces its first 8 bytes and the remaining bytes are unchanged. Payloads shorter than
8 bytes fail without being published. The per-send copy adds allocation and copying overhead, so compare throughput
and allocation using equivalent workloads when evaluating performance. Latency is measured between the producer's
and the consumer's clocks; synchronise worker clocks. See
[Latency and backlog](../methodology/latency-and-backlog.md).

**Durability.** `storageType: file` stores messages on disk, `memory` keeps them in server memory; both are
replicated according to `replicationFactor`. The publish acknowledgement is returned after the server has stored the
message according to these settings.

**Cleanup.** Closing a benchmark consumer stops its dispatcher and unsubscribes its local delivery subscription
without closing the shared connection. Streams and durable consumers are not deleted after a run. Names contain random
suffixes, so each run leaves new streams behind. Remove them, for example with the `nats` CLI (`nats stream rm`).

**Workload features.** Multiple topics, multiple subscriptions per topic and multiple consumers per subscription
work. Partition counts and keys are ignored. For backlog workloads (`consumerBacklogSizeGB`) the framework blocks the
consumer callback; the stream keeps the backlog. See [Workloads](../guides/workloads.md).

## Tuning examples

Memory storage, as in the `nats-memory-fanout` regression config:

```yaml
storageType: memory
```

Memory storage avoids disk writes but the data is lost when the server restarts, and stream size is limited by
server memory. Compare file and memory runs separately; see [Fair comparisons](../methodology/fair-comparisons.md).

Replication on a three-server JetStream cluster:

```yaml
replicationFactor: 3
```

More replicas mean each publish acknowledgement waits for replication to a quorum of servers.

Limiting stream size:

```yaml
maxBytes: 1073741824
```

With the default discard policy the oldest messages are removed when the limit is reached, so a long run or a large
backlog can lose messages that consumers have not read yet.

## Troubleshooting and verification

Run the NATS JetStream regression cases (Docker required, see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks nats
e2e-tests/compose/run-e2e.sh --cases nats-contract,nats-memory-fanout
```

A passing case is a functional check at a low rate, not a performance result.

Common problems:

- `Replicas must be from 1 to 5 inclusive.`: `replicationFactor` is missing or out of range.
- Stream creation fails with an insufficient resources or no suitable peers error: `replicationFactor` is larger than
  the number of JetStream servers (the shipped `nats.yaml` uses 3).
- Configuration error for `storageType`: use lower-case `file` or `memory`.
- JetStream not enabled: the server was started without `-js` (or the account has no JetStream access).
- `NATS payload must be at least 8 bytes` on send: increase the message size to at least 8 bytes.
- Driver fails to initialize: `natsHostUrl` is unreachable; the connection attempt times out after 5 s.

General advice is in [Troubleshooting](../guides/troubleshooting.md); result interpretation in
[Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-nats/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-nats/src/main/java/io/openmessaging/benchmark/driver/nats)
- Shipped config: [`driver-nats/nats.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-nats/nats.yaml)
- Regression configs: [`nats-file.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/nats-file.yaml), [`nats-memory.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/nats-memory.yaml)
- Stack file: [`e2e-tests/compose/stacks/nats.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/nats.yaml)
- Legacy Terraform/Ansible files: [`driver-nats/deploy`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-nats/deploy). They are unmaintained, reference older distribution names, Java versions and launcher settings, and must be adapted before use.

