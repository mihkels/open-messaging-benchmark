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

# Pravega

The Pravega plugin benchmarks Pravega streams with the Pravega Java client. Each benchmark topic is
a Pravega stream in one scope, each partition is a stream segment, and each benchmark subscription
is a reader group. Producers can write events directly or in transactions.

|       Item        |                                                                        Value                                                                        |
|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID         | `pravega`                                                                                                                                           |
| Module            | `driver-pravega`                                                                                                                                    |
| `driverClass`     | `io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver`                                                                                  |
| Shipped configs   | `driver-pravega/pravega.yaml`, `driver-pravega/pravega-exactly-once.yaml`                                                                           |
| Client library    | `pravega-client` 0.13.0                                                                                                                             |
| Tested broker     | `pravega/pravega:0.13.0` in standalone mode (Compose stack `pravega`, amd64 image only)                                                             |
| Regression cases  | `pravega-contract`, `pravega-transactions`                                                                                                          |
| Known limitations | amd64-only image; `pravega-exactly-once.yaml` uses 100 events per transaction (typo in the file); transactional publish latency excludes the commit |

## Prerequisites and broker setup

The driver needs a Pravega cluster where:

- the **controller** (`client.controllerURI`, default `tcp://localhost:9090`) is reachable from
  every worker;
- the **segment stores** advertise an address that every worker can reach. Clients get segment
  store addresses from the controller and then connect to them directly (port 12345 by default);
- the client is allowed to create the scope (unless `createScope: false` and the scope exists),
  streams and reader groups, and to seal and delete streams.

The driver does not configure authentication or TLS; it only sets the controller URI.

### Starting a test broker

The regression suite uses
[`e2e-tests/compose/stacks/pravega.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/pravega.yaml):
`pravega/pravega:0.13.0` with the `standalone` command, `HOST_IP: pravega` (the address the segment
store advertises) and a health check on the controller REST API (port 9091) and the segment store
(port 12345). The controller listens on 9090.

The image is published for `linux/amd64` only. On Apple Silicon it runs under emulation, which is
slower; pass `--case-timeout 600` to `run-e2e.sh`. Emulated runs are only functional checks.

To reach a standalone instance from the host, advertise an address the host can resolve and publish
both ports:

```bash
docker run -d --name pravega --platform linux/amd64 \
  -e HOST_IP=127.0.0.1 -p 9090:9090 -p 12345:12345 \
  pravega/pravega:0.13.0 standalone
```

## Minimal configuration and benchmark command

```yaml
name: Pravega
driverClass: io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver
client:
  controllerURI: tcp://localhost:9090
  scopeName: examples
writer:
  enableConnectionPooling: true
includeTimestampInEvent: true
```

What to change:

- `client.controllerURI`: the controller address from every worker's point of view (in the Compose
  suite, `tcp://pravega:9090`);
- `client.scopeName`: the scope that holds the benchmark streams. The shipped files use `examples`;
  the built-in default is `openmessaging-benchmark`.

Run a local-mode benchmark from the repository root after `mvn -DskipTests install`, or from
`/benchmark` inside the benchmark image:

```bash
bin/benchmark \
  --drivers driver-pravega/pravega.yaml \
  -o pravega-1-topic-1-partition-1kb.json \
  workloads/1-topic-1-partition-1kb.yaml
```

For several worker hosts, see [Distributed runs](../getting-started/distributed-runs.md); install the
`pravega` plugin bundle on every worker ([Runtime and plugins](../reference/runtime-and-plugins.md)).

## Configuration reference

The configuration is read from a map: unknown keys are ignored, and boolean and integer values may be
written as YAML strings or with any capitalization (`True`, `true`).

|               Key                |  Type   |          Default          |                                                            Description                                                            |
|----------------------------------|---------|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `client.controllerURI`           | string  | `tcp://localhost:9090`    | Controller URI.                                                                                                                   |
| `client.scopeName`               | string  | `openmessaging-benchmark` | Scope for streams and reader groups.                                                                                              |
| `writer.enableConnectionPooling` | boolean | `true`                    | Event writers share connections to segment stores instead of opening their own.                                                   |
| `includeTimestampInEvent`        | boolean | `true`                    | Prefix each event with an 8-byte send timestamp used for end-to-end latency. Turn off only if you do not need end-to-end latency. |
| `createScope`                    | boolean | `true`                    | Create the scope (if missing) before creating each stream.                                                                        |
| `deleteStreams`                  | boolean | `true`                    | Delete the created streams when the driver closes. Streams are always sealed.                                                     |
| `enableStreamAutoScaling`        | boolean | `false`                   | Use an auto-scaling policy instead of a fixed number of segments. Needs `eventsPerSecond` or `kbytesPerSecond`.                   |
| `eventsPerSecond`                | integer | `-1` (unset)              | Target events per second per segment for auto-scaling (`ScalingPolicy.byEventRate`). Ignored unless auto-scaling is enabled.      |
| `kbytesPerSecond`                | integer | `-1` (unset)              | Target KB per second per segment for auto-scaling (`ScalingPolicy.byDataRate`). Used only if `eventsPerSecond` is unset.          |
| `enableTransaction`              | boolean | `false`                   | Write events in Pravega transactions.                                                                                             |
| `eventsPerTransaction`           | integer | `100`                     | Events per transaction when `enableTransaction` is true.                                                                          |

### What `pravega-exactly-once.yaml` does

It sets `enableTransaction: true`, so producers write through a transactional writer. It also sets
`eventPerTransaction: 1`, but the key the driver reads is `eventsPerTransaction` (with an `s`).
The misspelled key is ignored, so the file runs with **100 events per transaction**, not 1. To get
one event per transaction, use `eventsPerTransaction: 1`.

The file does not make the benchmark exactly-once end to end. Transactions make each group of events
visible to readers atomically, at commit. Readers do not use checkpoints, and the benchmark does not
de-duplicate on the consumer side.

## Semantics

- **Scope and streams.** Each benchmark topic becomes a stream named
  `openmessaging-benchmark-<index>-<random>`; characters other than letters, digits and `-` are
  removed. The leader worker creates the scope (if `createScope`) and the streams.
- **Partitions and segments.** Without auto-scaling, a stream has exactly `partitionsPerTopic`
  segments (`ScalingPolicy.fixed`). With `enableStreamAutoScaling: true` and a rate set, the stream
  starts with `partitionsPerTopic` segments, never scales below that, and splits or merges segments
  by the target rate with a scale factor of 2. In the shipped `pravega.yaml`,
  `eventsPerSecond: 10000` has no effect because auto-scaling is off.
- **Keys.** Message keys are used as Pravega routing keys: events with the same key go to the same
  segment and keep their order. Events without a key are spread across segments.
- **Producers.** Each benchmark producer is one event writer. A send completes when the segment
  store has durably stored the event.
- **Transactional producers.** With `enableTransaction: true`:
  - the producer begins a transaction, writes events to it and commits after every
    `eventsPerTransaction` events. The commit is called synchronously in the send path;
  - each send returns an already completed future, so **publish latency does not include the
    commit**. Commit time slows the send loop and shows up in end-to-end latency instead;
  - readers see events only after their transaction commits, so end-to-end latency grows with
    `eventsPerTransaction` divided by the publish rate;
  - when the producer closes, an open transaction is aborted. Up to `eventsPerTransaction - 1`
    events written at the end of the run are dropped;
  - the readiness probe message (key `key`) is padded with copies to a full transaction, so it is
    committed and reaches the consumers.
- **Subscriptions and consumers.** Each subscription is a reader group named after the subscription
  (characters cleaned as for streams). The group is created if missing and starts at the beginning of
  the stream. Each benchmark consumer is a reader in that group; segments are shared out between the
  readers, so consumers in one subscription compete for events and separate subscriptions each get
  every event. A reader with no assigned segment receives nothing, so more consumers than segments
  leave some idle.
- **Reading.** Each consumer reads on one thread, polling with a 1-second timeout. Readers do not
  checkpoint; time windows are disabled.
- **End-to-end timestamp.** With `includeTimestampInEvent: true` (the default), the producer prefixes
  each event with `System.currentTimeMillis()` and the consumer reads it back. Latency therefore
  compares producer and consumer host clocks; see
  [Latency and backlog](../methodology/latency-and-backlog.md). With `false`, the consumer reports a
  fixed placeholder timestamp, and end-to-end latency figures are meaningless. Each event uses an
  owned buffer so pending writes and transactions cannot reuse caller-owned payloads; this adds an
  allocation per send.
- **Backlog workloads.** The framework blocks the consumer callback, so the reader stops reading
  until it is released.
- **Cleanup.** Closing a non-transactional producer fails any send still pending after the writer
  closes, including when close itself fails. Closing from that producer's own completion callback is
  rejected and may be retried after the callback returns. When the leader's driver closes, it seals
  every stream it created and deletes it if `deleteStreams` is true. Reader groups and the scope are
  not deleted. Consumer close reports a timeout if the reader thread does not stop; closing from its
  own callback is rejected and may be retried after the callback returns.

## Tuning examples

Measure latency and throughput together and read
[Fair comparisons](../methodology/fair-comparisons.md) before comparing systems.

- **Segments.** Raise `partitionsPerTopic` in the workload to spread a stream across more segments
  and segment containers. Use at least as many segments as consumers per subscription.
- **Auto-scaling.** Set `enableStreamAutoScaling: true` with `eventsPerSecond` or
  `kbytesPerSecond` to let Pravega add segments under load. Scaling changes the stream during the
  run, so results depend on when scaling happens.
- **Connection pooling.** `writer.enableConnectionPooling: true` lets writers share connections.
  `false` gives each writer its own connections, which costs more connections and can change
  latency with many producers.
- **Transactions.** `eventsPerTransaction` trades commit overhead against how long events stay
  invisible. Small values commit often; large values add end-to-end latency and lose more events
  when the run ends.

## Troubleshooting and verification

Run the Compose regression cases; see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md):

```bash
e2e-tests/compose/run-e2e.sh --stacks pravega --case-timeout 600
```

This runs `pravega-contract` (scope and stream creation, event writer and reader group) and
`pravega-transactions` (100 events per transaction, 4 segments, routing keys). A passing case is a
functional check at a low fixed rate. It is not a performance result.

Common problems:

- **Writers or readers hang after creating streams.** The segment store advertises an address the
  workers cannot reach (check `HOST_IP` or the segment store's published address and port 12345).
- **`Timed out waiting for consumers to be ready`.** Readers did not receive the probe event within
  60 seconds: the reader group could not be created, the segment store is unreachable, or (for
  transactions) the commit did not complete.
- **End-to-end latency is meaningless.** `includeTimestampInEvent` is false.
- **Consumed count is lower than published with transactions.** Events in the last open transaction
  are aborted at close.
- **Very slow start on Apple Silicon.** The image runs under amd64 emulation; increase the case
  timeout.

See also [Troubleshooting](../guides/troubleshooting.md) and [Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-pravega/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-pravega/src/main/java/io/openmessaging/benchmark/driver/pravega)
- Shipped configs: [`pravega.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-pravega/pravega.yaml), [`pravega-exactly-once.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-pravega/pravega-exactly-once.yaml)
- Compose driver configs: [`drivers/pravega.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/pravega.yaml), [`drivers/pravega-transactions.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/pravega-transactions.yaml)
- Compose stack: [`stacks/pravega.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/pravega.yaml)
- Legacy AWS, Kubernetes and build files, unmaintained: [`driver-pravega`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-pravega) (`deploy/`, `doc/`, shell scripts)

To write workloads and connect to your own cluster, see [Workloads](../guides/workloads.md) and
[Using your broker](../getting-started/using-your-broker.md).
