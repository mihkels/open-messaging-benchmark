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

# NATS Streaming (legacy STAN)

The NATS Streaming driver benchmarks the legacy [NATS Streaming server](https://docs.nats.io/legacy/stan)
(also called STAN) with the `java-nats-streaming` client (2.2.3). Each benchmark topic is a NATS Streaming channel
and each subscription is a queue group.

> **Note:** NATS Streaming is deprecated by the NATS maintainers, who ended support for it in June 2023 and
> recommend [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) instead. NATS Streaming is a separate
> server and protocol layered on NATS; it is not JetStream. Use this driver only to measure existing NATS Streaming
> deployments. For JetStream use the [NATS JetStream driver](nats.md).

|          Item          |                                                                              Value                                                                              |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID              | `nats-streaming`                                                                                                                                                |
| Module                 | `driver-nats-streaming`                                                                                                                                         |
| `driverClass`          | `io.openmessaging.benchmark.driver.natsStreaming.NatsStreamingBenchmarkDriver`                                                                                  |
| Example config shipped | `driver-nats-streaming/nats-streaming.yaml` (`nats://localhost:4222`, cluster ID `mycluster`)                                                                   |
| Tested broker          | `nats-streaming:0.25.6` with in-memory store (Compose stack `nats-streaming`)                                                                                   |
| Regression cases       | `nats-streaming-contract` (queue subscription with acknowledgements), `nats-streaming-fanout` (2 channels x 2 queue groups)                                     |
| Known limitations      | Deprecated upstream; partitions and keys are ignored; non-durable subscriptions that start with new messages only; end-to-end latency uses the server timestamp |

## Prerequisites and broker setup

The driver needs:

- A NATS Streaming server reachable on its NATS client port (4222 by default).
- The server's cluster ID. It must match `clusterId` in the driver file; a mismatch makes the connection time out.
  The NATS Streaming server's own default cluster ID is `test-cluster`, which is also the driver's default.

Channels are created by the server on first use; nothing has to be created in advance. Channel limits (maximum
messages, bytes, age, number of channels) are server settings.

The regression suite starts the server from
[`e2e-tests/compose/stacks/nats-streaming.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/nats-streaming.yaml)
with cluster ID `omb-e2e` and the in-memory store. A comparable local server, with the cluster ID of the shipped
driver file:

```bash
docker run -d --name nats-streaming -p 4222:4222 -p 8222:8222 nats-streaming:0.25.6 -cid mycluster -m 8222 -store memory
```

The image is multi-arch and runs natively on Apple Silicon. `-store memory` keeps messages only in memory; use the
file store (`-store file -dir <path>`) to measure a persistent setup.

## Minimal configuration and benchmark command

The shipped `driver-nats-streaming/nats-streaming.yaml`:

```yaml
name: Nats Streaming
driverClass: io.openmessaging.benchmark.driver.natsStreaming.NatsStreamingBenchmarkDriver

natsHostUrl: nats://localhost:4222
clusterId: mycluster
```

Change `natsHostUrl` to your server and `clusterId` to its cluster ID.

Run a local-mode benchmark from the repository root (after a full `mvn -DskipTests install`) or from `/benchmark`
in the container image:

```bash
bin/benchmark --drivers driver-nats-streaming/nats-streaming.yaml workloads/1-topic-1-partition-1kb.yaml
```

Host names depend on where the benchmark runs:

- Source checkout or distribution on the machine running the `docker run` server above: `localhost` works.
- Benchmark inside a container: `localhost` is the container itself; use a host name the container can resolve.
- Compose regression suite: [`e2e-tests/compose/drivers/nats-streaming.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/nats-streaming.yaml)
  uses `nats://nats-streaming:4222` and cluster ID `omb-e2e`.

For remote workers add `--workers` or `--workers-file`; see [Distributed runs](../getting-started/distributed-runs.md).
Install the plugin bundle on every worker, see [Runtime and plugins](../reference/runtime-and-plugins.md), and
[Using your broker](../getting-started/using-your-broker.md) for adapting a shipped configuration.

## Configuration reference

All keys are read by `NatsStreamingClientConfig`. Unknown keys are ignored.

|         Key          |  Type  |                 Default                  |                                               Description                                                |
|----------------------|--------|------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `natsHostUrl`        | string | client default (`nats://localhost:4222`) | NATS URL of the NATS Streaming server.                                                                   |
| `clusterId`          | string | `test-cluster`                           | NATS Streaming cluster ID; must match the server's `-cid`.                                               |
| `maxPubAcksInFlight` | int    | `16384`                                  | Maximum published messages awaiting a server acknowledgement per connection; publishing blocks above it. |
| `maxInFlight`        | int    | `1024`                                   | Maximum unacknowledged messages the server delivers to each subscription member.                         |

There are no pass-through client properties. Other connection and subscription options (connect wait, ack wait,
ping interval, authentication, TLS) are client defaults; see the
[java-nats-streaming client](https://github.com/nats-io/stan.java).

## Semantics

**Topics and partitions.** Topic names start with `Nats-streaming-benchmark` followed by an index and a random
suffix. Each topic is one NATS Streaming channel. `createTopic` does nothing (it logs that partitions are ignored);
the server creates the channel on first use. `partitionsPerTopic` has no effect.

**Producers.** All producers of one worker share a single streaming connection with a random client ID. A send
publishes asynchronously and completes when the server's publish acknowledgement arrives, that is after the server
has stored the message in its configured store. The driver owns the message bytes even if the send is canceled.
Closing a producer handle waits for in-progress publish calls to return, then fails its pending sends without closing
the shared connection. Closing from a callback executing synchronously inside a `sendAsync` call on that handle throws
`IllegalStateException`. Closing from a send-completion callback running inline with a client acknowledgement or a
close-induced failure is also rejected before it changes the handle's state. Retry after the call or callback returns,
or schedule close on another thread without waiting for it in the callback. Keys are ignored.

**Subscriptions and consumers.** Each consumer opens its own streaming connection with a random client ID and
creates a queue subscription on the topic's channel, with the benchmark subscription name as the queue group.
Consumers of the same subscription share messages; each subscription is its own queue group and receives every
message, so multiple subscriptions per topic work. The subscriptions are not durable and use the default start
position, so they receive only messages published after they subscribed. Connection and subscription failures complete
creation exceptionally with their cause; a connection opened for a failed subscription is closed.

**Acknowledgement.** Subscriptions use the client's automatic acknowledgement mode: the client acknowledges each
message after the benchmark callback returns. `maxInFlight` limits unacknowledged messages per member.

**End-to-end latency.** The consumer uses the message timestamp assigned by the NATS Streaming server (nanoseconds,
converted to milliseconds) as the publish time. Latency is therefore measured from when the server stored the
message, using the server's clock, and excludes the time from the producer to the server. Clock offset between the
server and the workers adds directly to the result. See [Latency and backlog](../methodology/latency-and-backlog.md).

**Durability.** Determined by the server store (`-store memory` or `file`) and its sync settings, not by the driver.

**Cleanup.** Each consumer closes its connection; the driver closes the shared producer connection. The driver does
not delete channels. With the memory store they disappear when the server stops; with the file store they remain until
removed by server channel limits or by deleting the store.

**Workload features.** Multiple topics, multiple subscriptions per topic and multiple consumers per subscription
work. Partition counts and keys are ignored. For backlog workloads (`consumerBacklogSizeGB`) the framework blocks the
consumer callback; delivery to a member then stops at `maxInFlight` unacknowledged messages. See
[Workloads](../guides/workloads.md).

## Tuning examples

The values used by the regression config:

```yaml
maxPubAcksInFlight: 1024
maxInFlight: 256
```

`maxPubAcksInFlight` bounds outstanding publishes on the shared producer connection of a worker; when it is reached,
publishing blocks until acknowledgements arrive. A lower value limits the publish rate a worker can reach and the
memory used for pending acknowledgements. `maxInFlight` bounds outstanding deliveries per subscription member;
lower values reduce buffering in the client, higher values allow more messages in flight per consumer.

Server-side choices (memory or file store, store sync, channel limits) usually matter more than these client
settings; record them with the results, see [Fair comparisons](../methodology/fair-comparisons.md).

## Troubleshooting and verification

Run the NATS Streaming regression cases (Docker required, see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks nats-streaming
e2e-tests/compose/run-e2e.sh --cases nats-streaming-contract,nats-streaming-fanout
```

A passing case is a functional check at a low rate, not a performance result.

Common problems:

- `nats streaming create producer exception` or `nats streaming create consumer exception` in the worker log:
  connecting failed. Typical causes are a wrong `natsHostUrl` or a `clusterId` that does not match the server's
  `-cid` (the connect request times out). The creation future fails with the underlying error.
- Connecting to a plain NATS or JetStream server: the NATS connection succeeds but the streaming connect request
  times out, because no NATS Streaming server answers.
- No messages consumed: subscriptions start with new messages only; messages published before the consumers
  subscribed are not delivered.

General advice is in [Troubleshooting](../guides/troubleshooting.md); result interpretation in
[Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-nats-streaming/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-nats-streaming/src/main/java/io/openmessaging/benchmark/driver/natsStreaming)
- Shipped config: [`driver-nats-streaming/nats-streaming.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-nats-streaming/nats-streaming.yaml)
- Regression config: [`e2e-tests/compose/drivers/nats-streaming.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/nats-streaming.yaml)
- Stack file: [`e2e-tests/compose/stacks/nats-streaming.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/nats-streaming.yaml)
- Legacy Terraform/Ansible files: [`driver-nats-streaming/deploy`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-nats-streaming/deploy). They are unmaintained, reference older distribution names, Java versions and launcher settings, and must be adapted before use.

