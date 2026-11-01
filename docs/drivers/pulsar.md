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

# Apache Pulsar

The Pulsar plugin benchmarks Apache Pulsar with the Java client for publishing and consuming. It
uses the Pulsar admin client to create a tenant, a namespace and partitioned topics before the run.

|       Item        |                                                      Value                                                       |
|-------------------|------------------------------------------------------------------------------------------------------------------|
| Plugin ID         | `pulsar`                                                                                                         |
| Module            | `driver-pulsar`                                                                                                  |
| `driverClass`     | `io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkDriver`                                                 |
| Shipped configs   | `driver-pulsar/pulsar.yaml`, `driver-pulsar/pulsar-effectively-once.yaml`                                        |
| Client library    | Pulsar Java client and admin client 4.2.5                                                                        |
| Tested broker     | `apachepulsar/pulsar:4.0.6`, standalone mode (Compose stack `pulsar`)                                            |
| Regression cases  | `pulsar-contract`, `pulsar-shared-no-batching`, `pulsar-key-shared-dedup`                                        |
| Known limitations | No cleanup of topics or namespaces; no transactions; no client-side producer tuning beyond the keys listed below |

## Prerequisites and broker setup

The driver needs two endpoints, reachable from every worker:

- the binary protocol endpoint (`serviceUrl`, usually port 6650) for producers and consumers;
- the HTTP admin endpoint (`httpUrl`, usually port 8080). The driver calls the admin API during
  initialization. The configured identity must be allowed to list and create tenants, create
  namespaces, set namespace policies and create partitioned topics.

When the driver is initialized on a worker, it:

1. Takes the tenant name from the part of `namespacePrefix` before the first `/`. For
   `benchmark/ns` the tenant is `benchmark`. If the tenant does not exist, the driver creates it with
   no admin roles and with `clusterName` as its only allowed cluster. `clusterName` must therefore
   name an existing cluster of the Pulsar instance. For `bin/pulsar standalone`, the cluster is
   `standalone`.
2. Creates a new namespace `<namespacePrefix>-<random suffix>`, for example `benchmark/ns-AbCdEfG`.
3. Applies namespace policies:
   - persistence: `ensembleSize`, `writeQuorum`, `ackQuorum`, and a mark-delete rate of 1.0;
   - an unlimited backlog quota with the `producer_exception` retention policy;
   - deduplication on or off, from `persistence.deduplicationEnabled`.

The persistence policy must be satisfiable by the cluster. An ensemble of 3 needs at least 3
writable bookies. A standalone broker has one bookie, so use `1/1/1` against it, as the Compose
driver configurations do.

**Concurrent tenant creation.** All workers initialize the driver at the same time, so several
workers can try to create the same tenant. The driver ignores the `ConflictException` from that
race. Some metadata stores report the race as a different admin error; in that case the driver
lists the tenants again and fails only if the tenant still does not exist.

### Starting a test broker

The regression suite uses
[`e2e-tests/compose/stacks/pulsar.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/pulsar.yaml):
Pulsar standalone, with the transaction coordinator enabled for the JMS lane (this plugin does not
use transactions). A minimal equivalent with plain Docker:

```bash
docker run -d --name pulsar -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:4.0.6 bin/pulsar standalone -nfw -nss
```

The image is multi-architecture and runs natively on amd64 and arm64. Wait until
`curl -fsS http://localhost:8080/admin/v2/brokers/health` succeeds before you start a benchmark.

> **Note:** Pulsar's admin port 8080 is also the default port of `bin/benchmark-worker`. If you run a
> worker on the same host, start the worker on another port (for example `bin/benchmark-worker -p 8081`)
> or publish the Pulsar admin API on a different host port.

## Minimal configuration and benchmark command

This configuration targets the standalone broker above. It is the shipped `driver-pulsar/pulsar.yaml`
with the cluster name and persistence quorums changed for a single-bookie broker:

```yaml
name: Pulsar
driverClass: io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkDriver

client:
  serviceUrl: pulsar://localhost:6650
  httpUrl: http://localhost:8080
  clusterName: standalone
  namespacePrefix: benchmark/ns
  topicType: persistent
  persistence:
    ensembleSize: 1
    writeQuorum: 1
    ackQuorum: 1
    deduplicationEnabled: false

producer:
  batchingEnabled: true
  batchingMaxPublishDelayMs: 1
  batchingMaxBytes: 131072
  blockIfQueueFull: true
  pendingQueueSize: 0

consumer:
  receiverQueueSize: 10000
  subscriptionType: Failover
```

What to change for your environment:

- `serviceUrl` and `httpUrl`: addresses that every worker can reach. In the Compose suite they are
  `pulsar://pulsar:6650` and `http://pulsar:8080` (Compose service names). For TLS, use
  `pulsar+ssl://` and `https://` URLs.
- `clusterName`: the cluster name of your Pulsar instance. The shipped files use `local`.
- `persistence`: quorums that your number of bookies can satisfy.

Save the file, for example as `pulsar-local.yaml`, and run a local-mode benchmark. In local mode the
coordinator starts the driver in a child JVM; no worker is needed. Run the command from the
repository root after `mvn -DskipTests install`, or from `/benchmark` inside the benchmark image,
where the same relative paths exist:

```bash
bin/benchmark \
  --drivers pulsar-local.yaml \
  -o pulsar-1-topic-1-partition-1kb.json \
  workloads/1-topic-1-partition-1kb.yaml
```

Without `-o`, the result is written to `1-topic-1-partition-1kb-Pulsar-<yyyy-MM-dd-HH-mm-ss>.json`.
For runs with several worker hosts, see [Distributed runs](../getting-started/distributed-runs.md).
The `pulsar` plugin bundle must be installed on every worker; see
[Runtime and plugins](../reference/runtime-and-plugins.md).

## Configuration reference

All keys are read by `PulsarConfig` and its nested classes. Unknown keys are ignored.

### `client`

|                Key                 |  Type   |   Default    |                                                     Description                                                      |
|------------------------------------|---------|--------------|----------------------------------------------------------------------------------------------------------------------|
| `serviceUrl`                       | string  | required     | Binary protocol URL, for example `pulsar://host:6650`. TLS settings are applied only if it starts with `pulsar+ssl`. |
| `httpUrl`                          | string  | required     | Admin REST URL, for example `http://host:8080`. TLS settings are applied only if it starts with `https`.             |
| `namespacePrefix`                  | string  | required     | `<tenant>/<namespace prefix>`. A random suffix is appended for each driver instance.                                 |
| `clusterName`                      | string  | none         | Only allowed cluster of the tenant when the driver creates it. Not used if the tenant already exists.                |
| `topicType`                        | enum    | `persistent` | `persistent` or `non-persistent`. Used as the scheme of the topic names.                                             |
| `ioThreads`                        | int     | `8`          | Client I/O threads.                                                                                                  |
| `connectionsPerBroker`             | int     | `8`          | Client connections per broker.                                                                                       |
| `maxConcurrentLookupRequests`      | int     | `1000`       | Concurrent topic lookup requests.                                                                                    |
| `clientMemoryLimitMB`              | long    | `256`        | Client memory limit for pending messages, in MB.                                                                     |
| `persistence.ensembleSize`         | int     | `3`          | Namespace persistence policy: bookies in a ledger ensemble.                                                          |
| `persistence.writeQuorum`          | int     | `3`          | Namespace persistence policy: copies of each entry.                                                                  |
| `persistence.ackQuorum`            | int     | `2`          | Namespace persistence policy: bookie acknowledgements required for each entry.                                       |
| `persistence.deduplicationEnabled` | boolean | `false`      | Broker-side message deduplication on the benchmark namespace.                                                        |
| `tlsAllowInsecureConnection`       | boolean | `false`      | Accept untrusted server certificates (client and admin client).                                                      |
| `tlsEnableHostnameVerification`    | boolean | `false`      | Verify the server host name (client and admin client).                                                               |
| `tlsTrustCertsFilePath`            | string  | none         | Path of the trusted CA certificate file on each worker.                                                              |
| `authentication.plugin`            | string  | none         | Authentication plugin class name. Applied to the client and the admin client when not empty.                         |
| `authentication.data`              | string  | none         | Authentication parameters passed to the plugin.                                                                      |

The driver also sets these client options, which are not configurable: operation timeout 10 minutes,
no limit on lookup requests, and one listener thread per available processor.

### `producer`

|             Key             |  Type   |  Default  |                                                Description                                                 |
|-----------------------------|---------|-----------|------------------------------------------------------------------------------------------------------------|
| `batchingEnabled`           | boolean | `true`    | Producer batching.                                                                                         |
| `batchingMaxPublishDelayMs` | int     | `1`       | Longest time a batch is held before it is sent, in milliseconds.                                           |
| `batchingMaxBytes`          | int     | `1048576` | Largest batch, in bytes. The driver does not limit the number of messages in a batch.                      |
| `blockIfQueueFull`          | boolean | `true`    | Block the send call when the pending queue is full, instead of failing the send.                           |
| `pendingQueueSize`          | int     | `0`       | Passed to the producer's `maxPendingMessages`. See the Pulsar client documentation for the meaning of `0`. |

The send timeout is fixed at 0 (disabled): a send waits until the broker acknowledges it.

### `consumer`

|         Key         | Type |  Default   |                    Description                     |
|---------------------|------|------------|----------------------------------------------------|
| `receiverQueueSize` | int  | `10000`    | Messages prefetched by each Pulsar consumer.       |
| `subscriptionType`  | enum | `Failover` | `Exclusive`, `Shared`, `Failover` or `Key_Shared`. |

There is no pass-through map for other Pulsar client, producer or consumer settings. For the meaning
of the underlying settings, see the
[Pulsar Java client documentation](https://pulsar.apache.org/docs/next/client-libraries-java/).

## Semantics

- **Topic names.** `<topicType>://<namespace>/test-<index>-<random>`. The namespace is the one
  created by the worker that creates the topics. In a distributed run every other worker also
  creates its own namespace during initialization; those namespaces stay empty.
- **Partitions.** With `partitionsPerTopic: 1` the driver does not create the topic; the broker
  creates a non-partitioned topic when the first producer or consumer attaches. This needs topic
  auto-creation on the broker, which Pulsar enables by default. With more than one partition, the
  driver creates a partitioned topic through the admin API.
- **Consumers.** For each benchmark consumer, the driver looks up the partitions of the topic and
  creates one Pulsar consumer per partition. All of them use the benchmark subscription name and the
  configured `subscriptionType`, so the subscription type applies to each partition on its own:
  - `Failover`: each partition has one active consumer in a subscription; the others are on standby.
  - `Shared` and `Key_Shared`: messages of each partition are spread across the consumers of the
    subscription.
  - `Exclusive`: a second consumer on the same subscription is rejected, so use it only with
    `consumerPerSubscription: 1`.
- **Subscriptions.** Each benchmark subscription is a Pulsar subscription. With several
  subscriptions per topic, each subscription receives every message.
- **Keys.** If the workload's key distributor produces a key, the driver sets it as the message key.
  Pulsar uses the key for partition routing and for `Key_Shared` dispatch.
- **Acknowledgements.** Every message is acknowledged individually and asynchronously after the
  benchmark callback returns.
- **End-to-end timestamp.** The driver reports the message publish time
  (`Message.getPublishTime()`), which the producer client sets. End-to-end latency therefore
  compares producer and consumer host clocks; see
  [Latency and backlog](../methodology/latency-and-backlog.md).
- **Durability.** A send completes when the broker acknowledges it. For persistent topics, the broker
  acknowledges after `ackQuorum` bookies have acknowledged the entry. The driver copies the payload
  for each send so that cancellation cannot expose a caller-owned buffer to a pending write.
- **Backlog workloads.** For `consumerBacklogSizeGB`, the benchmark framework blocks the consumer
  callback; the driver has no Pulsar-specific pause. The namespace backlog quota is unlimited, so a
  quota does not block producers.
- **Cleanup.** The worker closes producer and consumer handles; the driver closes the shared client
  and admin client. Partial or late producer and partition-consumer creations are closed on failure
  or cancellation. Any send still pending after producer close fails instead of remaining unresolved,
  including when close itself fails.
  Closing a producer from its own completion callback is rejected and may be retried after the
  callback returns. The driver does not delete topics, subscriptions or namespaces, so each run
  leaves new namespaces under the tenant. Remove them with `pulsar-admin` if needed.

### What `pulsar-effectively-once.yaml` configures

Compared with `pulsar.yaml`, this file:

- enables broker-side deduplication on the benchmark namespace (`persistence.deduplicationEnabled: true`);
- uses `ioThreads: 4` instead of 16;
- omits `batchingMaxBytes`, `pendingQueueSize` and `subscriptionType`, so the defaults apply
  (1 MB batches, `0`, `Failover`);
- is named `Pulsar-effectively-once`, which appears in result file names.

It does not enable transactions, does not set producer names or sequence IDs, and does not change
acknowledgements. Broker deduplication drops a message that a producer resends with a sequence ID
that is already persisted. On its own, it does not give end-to-end exactly-once processing. Describe
results from this file as "Pulsar with broker deduplication enabled". Like `pulsar.yaml`, it uses
`clusterName: local` and quorums of 3/3/2, so adjust both for your cluster.

## Tuning examples

Each change trades one property for another, so measure both latency and throughput. Before you
compare results with other systems, read [Fair comparisons](../methodology/fair-comparisons.md).

- **Batching.** `batchingEnabled: false` sends each message as its own entry. This removes the batch
  delay, but at higher rates it causes more broker and bookie requests. A larger
  `batchingMaxPublishDelayMs` or `batchingMaxBytes` builds larger batches and adds publish latency.
- **Durability.** With `persistence.ackQuorum` lower than `writeQuorum`, a write completes before
  all copies are acknowledged. Setting it equal to `writeQuorum` waits for all copies.
- **Deduplication.** `persistence.deduplicationEnabled: true` makes the broker track producer
  sequence IDs.
- **Consumer prefetch.** A larger `receiverQueueSize` lets consumers buffer more messages but uses
  more client memory. With `Shared` subscriptions, a large queue can also spread messages less
  evenly between consumers.
- **Subscription type.** `Shared` or `Key_Shared` lets more consumers than partitions receive from
  one subscription. `Failover` keeps one active consumer, and its order, per partition.
- **Client resources.** `ioThreads`, `connectionsPerBroker` and `clientMemoryLimitMB` control the
  concurrency and buffering of the client on each worker.
- **Non-persistent topics.** `topicType: non-persistent` does not store messages in BookKeeper and
  the broker can drop messages. Results are not comparable with persistent topics.

## Troubleshooting and verification

Run the Compose regression cases of this plugin; see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md):

```bash
e2e-tests/compose/run-e2e.sh --cases pulsar-contract
e2e-tests/compose/run-e2e.sh --stacks pulsar
```

A passing case is a functional check at a low fixed rate. It is not a performance result.

Common problems:

- **Initialization fails with an `IOException` that wraps a `PulsarAdminException`.** Check
  `httpUrl`, the authentication settings and, if the tenant has to be created, that `clusterName`
  exists. The shipped files use `local`; a standalone broker's cluster is `standalone`.
- **Publishing stalls without errors.** The persistence policy needs more bookies than are
  available, for example `ensembleSize: 3` against a standalone broker. The send timeout is disabled,
  so sends wait instead of failing.
- **`Timed out waiting for consumers to be ready`.** The coordinator's probe message did not reach
  every subscription within 60 seconds. Check that all consumers could subscribe; for example,
  `Exclusive` fails with more than one consumer per subscription.
- **Port 8080 already in use** by the Pulsar admin API and a `benchmark-worker` on the same host.

See also [Troubleshooting](../guides/troubleshooting.md) and [Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-pulsar/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-pulsar/src/main/java/io/openmessaging/benchmark/driver/pulsar)
- Shipped configs: [`pulsar.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-pulsar/pulsar.yaml), [`pulsar-effectively-once.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-pulsar/pulsar-effectively-once.yaml)
- Compose driver configs: [`pulsar.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/pulsar.yaml), [`pulsar-shared-nobatch.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/pulsar-shared-nobatch.yaml), [`pulsar-key-shared-dedup.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/pulsar-key-shared-dedup.yaml)
- Compose stack: [`stacks/pulsar.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/pulsar.yaml)
- Legacy Terraform/Ansible files, unmaintained: [`driver-pulsar/deploy`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-pulsar/deploy)

To write workloads and connect to your own cluster, see [Workloads](../guides/workloads.md) and
[Using your broker](../getting-started/using-your-broker.md).
