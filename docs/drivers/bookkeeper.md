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

# Apache BookKeeper (DistributedLog)

The BookKeeper plugin benchmarks Apache BookKeeper through the DistributedLog (DL) library that ships
with BookKeeper. Each benchmark topic is a DistributedLog log stream; DL writes log records into
BookKeeper ledgers and stores its metadata in ZooKeeper. The driver does not use the raw BookKeeper
ledger API, so results include the DistributedLog writer and reader layers.

|       Item        |                                                                      Value                                                                      |
|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID         | `bookkeeper`                                                                                                                                    |
| Module            | `driver-bookkeeper`                                                                                                                             |
| `driverClass`     | `io.openmessaging.benchmark.driver.bookkeeper.DlogBenchmarkDriver`                                                                              |
| Shipped config    | `driver-bookkeeper/bookkeeper.yaml`                                                                                                             |
| Client library    | `distributedlog-core-shaded` 4.18.2 (BookKeeper version from the root `pom.xml`)                                                                |
| Tested cluster    | ZooKeeper and one bookie from `apachepulsar/pulsar:4.0.6`, plus a bound DL namespace (Compose stack `bookkeeper`)                               |
| Regression cases  | `bookkeeper-contract`                                                                                                                           |
| Known limitations | One reader per consumer reads the whole stream; subscriptions are not shared; partitions are created but not used; keys are ignored; no cleanup |

## Prerequisites and broker setup

The driver needs:

1. **ZooKeeper**, reachable from every worker.
2. **A BookKeeper cluster** whose metadata is initialized in that ZooKeeper, with enough writable
   bookies for the ensemble size in `dlogConf` (`bkcEnsembleSize`). Bookies must advertise addresses
   that workers can reach.
3. **A DistributedLog namespace** bound to the BookKeeper ledger metadata. The driver opens the
   namespace given by `dlogUri` (for example `distributedlog://zookeeper:2181/streams`) but does not
   create or bind it. Bind it once before the first run with the DistributedLog admin tool:

```bash
java -cp '/benchmark/drivers/driver-bookkeeper/lib/*' \
  dlshade.org.apache.distributedlog.tools.Tool \
  dlshade.org.apache.distributedlog.admin.DistributedLogAdmin \
  bind -l /ledgers -s zookeeper:2181 -c distributedlog://zookeeper:2181/streams
```

This is the command the Compose stack runs (in its `dlog-init` service), using the shaded DL classes
from the installed plugin bundle. `-l` is the ledger metadata root path in ZooKeeper, `-s` the
ZooKeeper address, and `-c` the namespace URI that goes into `dlogUri`. From a source checkout, the
bundle is at `driver-bookkeeper/target/omb-plugin/lib/*` after a build.

### Starting a test cluster

The regression suite uses
[`e2e-tests/compose/stacks/bookkeeper.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/bookkeeper.yaml).
It starts, in order:

- `zookeeper`: `apachepulsar/pulsar:4.0.6` running `bin/pulsar zookeeper`;
- `bookkeeper-init`: `bin/bookkeeper shell initnewcluster` with
  `metadataServiceUri=zk+hierarchical://zookeeper:2181/ledgers`;
- `bookie`: one bookie (`bin/bookkeeper bookie`) with `advertisedAddress: bookie`, port 3181 and its
  HTTP server on port 8000 for the health check;
- `dlog-init`: the `bind` command above.

The Pulsar image is multi-architecture, so the stack runs natively on amd64 and arm64. The bookie
advertises the Compose service name `bookie`, so the stack is only usable by clients on the same
Compose network. For a cluster reachable from the host, set `advertisedAddress` to an address the
workers can resolve.

## Minimal configuration and benchmark command

The shipped `driver-bookkeeper/bookkeeper.yaml` expects ZooKeeper on `localhost:2181` and at least
three bookies (`bkcEnsembleSize=3`, rack-aware placement). For a single-bookie test cluster, use the
values of the Compose driver configuration:

```yaml
name: BookKeeper
driverClass: io.openmessaging.benchmark.driver.bookkeeper.DlogBenchmarkDriver
dlogUri: distributedlog://localhost:2181/streams
dlogConf: |
  bkcEnsembleSize=1
  bkcWriteQuorumSize=1
  bkcAckQuorumSize=1
  bkc.ensemblePlacementPolicy=dlshade.org.apache.bookkeeper.client.DefaultEnsemblePlacementPolicy
  bkc.allowShadedLedgerManagerFactoryClass=true
  writeLockEnabled=false
  enableLedgerAllocatorPool=false
  writerOutputBufferSize=524288
  enableImmediateFlush=false
  periodicFlushFrequencyMilliSeconds=2
  logSegmentRollingMinutes=0
  maxLogSegmentBytes=2147483648
  enableReadAhead=true
```

What to change:

- the host in `dlogUri`: the ZooKeeper address from every worker's point of view. In the Compose
  suite it is `zookeeper:2181`; the path (`/streams`) must match the namespace you bound;
- the quorum settings and placement policy, to match your number of bookies and racks.

Run a local-mode benchmark from the repository root after `mvn -DskipTests install`, or from
`/benchmark` inside the benchmark image:

```bash
bin/benchmark \
  --drivers bookkeeper-local.yaml \
  -o bookkeeper-1-topic-1-partition-1kb.json \
  workloads/1-topic-1-partition-1kb.yaml
```

Use a workload with one partition and one consumer per subscription (see
[Semantics](#semantics)). For several worker hosts, see
[Distributed runs](../getting-started/distributed-runs.md); install the `bookkeeper` plugin bundle on
every worker ([Runtime and plugins](../reference/runtime-and-plugins.md)).

## Configuration reference

The driver's configuration class (`Config`) reads two keys. Unknown keys are ignored.

|    Key     |  Type  | Default  |                                                            Description                                                            |
|------------|--------|----------|-----------------------------------------------------------------------------------------------------------------------------------|
| `dlogUri`  | string | required | DistributedLog namespace URI, `distributedlog://<zookeeper host:port>/<namespace path>`.                                          |
| `dlogConf` | string | required | DistributedLog configuration in Java properties format, loaded into `DistributedLogConfiguration`. Use a YAML block scalar (`|`). |

### `dlogConf` properties used by the shipped file

`dlogConf` is passed through to DistributedLog; any `DistributedLogConfiguration` property can be
set, and `bkc.`-prefixed properties are passed to the embedded BookKeeper client. The shipped file
sets these groups:

|                                      Property                                       |                              Shipped value                              |                                  Meaning                                  |
|-------------------------------------------------------------------------------------|-------------------------------------------------------------------------|---------------------------------------------------------------------------|
| `zkSessionTimeoutSeconds`, `zkNumRetries`, `zkRetry*`                               | 30 s, 0, 100–200 ms                                                     | DL ZooKeeper client session and retries                                   |
| `bkcZKSessionTimeoutSeconds`, `bkcZK*`                                              | 60 s, 20 retries, 100–200 ms                                            | ZooKeeper client used by the BookKeeper client                            |
| `bkcWriteTimeoutSeconds`, `bkcReadTimeoutSeconds`                                   | 10 s, 1 s                                                               | BookKeeper client request timeouts                                        |
| `bkcNumWorkerThreads`, `bkc.numChannelsPerBookie`                                   | 16, 1                                                                   | BookKeeper client threads and connections per bookie                      |
| `bkcEnsembleSize`, `bkcWriteQuorumSize`, `bkcAckQuorumSize`                         | 3, 3, 2                                                                 | Ledger ensemble, copies per entry, acknowledgements required per entry    |
| `bkc.ensemblePlacementPolicy`                                                       | `dlshade.org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy` | Bookie selection; the class must be the shaded (`dlshade.`) name          |
| `bkc.delayEnsembleChange`                                                           | `true`                                                                  | Delay ensemble changes on bookie failures                                 |
| `writeLockEnabled`                                                                  | `false`                                                                 | Do not take the DL single-writer lock                                     |
| `writerOutputBufferSize`                                                            | 524288 bytes                                                            | Records are buffered up to this size before a BookKeeper entry is written |
| `enableImmediateFlush`                                                              | `false`                                                                 | Do not flush after every record                                           |
| `periodicFlushFrequencyMilliSeconds`                                                | 2 ms                                                                    | Flush the output buffer at this interval                                  |
| `logFlushTimeoutSeconds`                                                            | 120 s                                                                   | Timeout for flushing a log segment                                        |
| `logSegmentRetentionHours`                                                          | 48                                                                      | Retention of completed log segments                                       |
| `logSegmentRollingMinutes`, `maxLogSegmentBytes`                                    | 0 (time rolling disabled), 2 GiB                                        | Roll log segments (ledgers) by size only                                  |
| `enableReadAhead`, `readAheadMaxRecords`, `readAheadBatchSize`, `readAheadWaitTime` | `true`, 1000000, 10, 200 ms                                             | Reader read-ahead cache                                                   |

For all properties and their defaults, see the
[DistributedLog configuration](https://bookkeeper.apache.org/docs/next/api/distributedlog-api/) and
[BookKeeper client configuration](https://bookkeeper.apache.org/docs/reference/config/) documentation.

## Semantics

- **Topics.** Each benchmark topic is a DL log stream named `test-stream-<index>-<random>` in the
  namespace. `createTopic` creates that log.
- **Partitions.** With `partitionsPerTopic` greater than 1, the driver also creates the logs
  `<topic>/000000`, `<topic>/000001`, and so on, but producers and consumers only open the base log
  `<topic>`. The extra logs stay empty, so partitioned workloads do not add parallelism.
- **Producers.** Each producer opens an asynchronous log writer on the stream. Every message becomes
  one DL log record whose transaction ID comes from a time-based sequencer (the current time in
  milliseconds, kept monotonic per writer). A send completes when DistributedLog acknowledges the
  record, that is after the buffered entry containing it has been written to `bkcAckQuorumSize`
  bookies. The driver copies the payload for each send so that cancellation cannot expose a
  caller-owned buffer to a pending write.
- **Multiple producers per topic.** DistributedLog is designed for one writer per log. The shipped
  configuration disables the write lock (`writeLockEnabled=false`), but the regression suite only
  tests one producer per topic.
- **Keys.** Message keys are ignored.
- **Consumers and subscriptions.** The subscription name is ignored. Every benchmark consumer
  starts its own reader at the beginning of the log (`DLSN.InitialDLSN`) and reads every record.
  Consumer creation succeeds only after that initial reader opens; an initial open failure fails
  creation.
  - Several subscriptions per topic work as independent full readers.
  - Several consumers in one subscription each receive every message, so the consumed count is
    multiplied by `consumerPerSubscription`. Use `consumerPerSubscription: 1`.
  - Consumers do not acknowledge or store a read position; a reader that fails is reopened from the
    last record it read.
- **End-to-end timestamp.** The consumer reports the record's transaction ID, which is the
  producer-side wall-clock time in milliseconds when the record was created. End-to-end latency
  therefore compares producer and consumer host clocks; see
  [Latency and backlog](../methodology/latency-and-backlog.md).
- **Backlog workloads.** The framework blocks the consumer callback; the reader then stops reading
  until the callback is released.
- **Metrics.** BookKeeper and DistributedLog client statistics are not exported (the driver uses a
  null stats logger).
- **Cleanup.** The worker closes each producer's writer and log manager and each consumer's reader
  and log manager; the driver closes the shared namespace. Failed or cancelled creation also closes
  resources that opened only after the operation was abandoned. Any send still pending after writer
  close fails instead of remaining unresolved, including when close itself fails. Closing a producer
  or consumer from its own callback is rejected and can be retried after the callback returns.
  Consumer close allows five seconds for reader and manager cleanup and the reader thread to stop.
  If that wait times out, in-progress cleanup continues on daemon threads, including cleanup of a
  reader opened by a late reconnect. The driver does not delete logs; their segments are kept
  according to `logSegmentRetentionHours`.

## Tuning examples

All tuning happens in `dlogConf`. Each change has a trade-off, so measure latency and throughput
together, and read [Fair comparisons](../methodology/fair-comparisons.md) before comparing with other
systems.

- **Buffering and flushing.** `writerOutputBufferSize` and `periodicFlushFrequencyMilliSeconds`
  control how many records go into one BookKeeper entry. A larger buffer or longer interval means
  fewer, larger writes and more time before a record is acknowledged. `enableImmediateFlush=true`
  writes each record without waiting.
- **Replication.** `bkcEnsembleSize`, `bkcWriteQuorumSize` and `bkcAckQuorumSize` set how many bookies
  store and acknowledge each entry. An ack quorum smaller than the write quorum completes writes
  before all copies are stored.
- **Placement.** `RackawareEnsemblePlacementPolicy` spreads ensembles across racks;
  `DefaultEnsemblePlacementPolicy` ignores racks. Use the shaded class names.
- **Read-ahead.** `readAheadMaxRecords`, `readAheadBatchSize` and `readAheadWaitTime` control how far
  readers fetch ahead of the application.
- **Client concurrency.** `bkcNumWorkerThreads` and `bkc.numChannelsPerBookie` set the BookKeeper
  client threads and connections per bookie.

## Troubleshooting and verification

Run the Compose regression case; see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md):

```bash
e2e-tests/compose/run-e2e.sh --cases bookkeeper-contract
```

A passing case is a functional check at a low fixed rate. It is not a performance result.

Common problems:

- **Namespace errors when the driver opens `dlogUri`.** The namespace is not bound, or the URI path
  differs from the one used with `DistributedLogAdmin bind`.
- **Writes fail or time out.** Fewer writable bookies than `bkcEnsembleSize`, a rack-aware policy
  without enough racks, or bookies that advertise addresses the workers cannot reach.
- **`ClassNotFoundException` for a placement policy.** The class name is not the shaded
  `dlshade.org.apache.bookkeeper...` name.
- **Consumed count is a multiple of the published count.** The workload has more than one consumer
  per subscription; every consumer reads the whole stream.
- **`Timed out waiting for consumers to be ready`.** The readers could not open the log or reach the
  bookies within 60 seconds.

See also [Troubleshooting](../guides/troubleshooting.md) and [Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-bookkeeper/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-bookkeeper/src/main/java/io/openmessaging/benchmark/driver/bookkeeper)
- Shipped config: [`bookkeeper.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-bookkeeper/bookkeeper.yaml)
- Compose driver config: [`drivers/bookkeeper.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/bookkeeper.yaml)
- Compose stack: [`stacks/bookkeeper.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/bookkeeper.yaml)
- Legacy Terraform/Ansible files, unmaintained: [`driver-bookkeeper/deploy`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-bookkeeper/deploy)

To write workloads and connect to your own cluster, see [Workloads](../guides/workloads.md) and
[Using your broker](../getting-started/using-your-broker.md).
