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

# Apache RocketMQ 5

The RocketMQ 5 plugin benchmarks Apache RocketMQ through the RocketMQ 5 gRPC client
(`rocketmq-client-java`). Producers and consumers connect to a RocketMQ proxy, not to the brokers.
Topic and consumer group creation still uses the classic admin API, which talks to the name server
and the brokers directly. For the classic remoting client, see [RocketMQ](rocketmq.md).

|       Item        |                                                                    Value                                                                    |
|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID         | `rocketmq5`                                                                                                                                 |
| Module            | `driver-rocketmq5`                                                                                                                          |
| `driverClass`     | `io.openmessaging.benchmark.driver.rocketmq.RocketMQ5BenchmarkDriver`                                                                       |
| Shipped config    | `driver-rocketmq5/rocketmq.yaml` (the file is named `rocketmq.yaml`, like the classic plugin's)                                             |
| Client library    | `rocketmq-client-java` 5.0.7 (gRPC); `rocketmq-tools` 5.1.0 for administration                                                              |
| Tested broker     | `apache/rocketmq:5.3.2`: name server, one broker and a proxy (Compose stack `rocketmq5`)                                                    |
| Regression cases  | `rocketmq5-contract`                                                                                                                        |
| Known limitations | No client tuning options; keys do not select queues; `sendDelayMsg: true` without `delayTimeInSec` fails; topics and groups are not deleted |

## Differences from the classic RocketMQ plugin

|                   |                        `rocketmq` (classic)                         |                         `rocketmq5`                          |
|-------------------|---------------------------------------------------------------------|--------------------------------------------------------------|
| Client protocol   | Remoting protocol, directly to brokers                              | gRPC to the proxy (`grpcEndpoint`)                           |
| Shipped config    | `driver-rocketmq/rocketmq.yaml`                                     | `driver-rocketmq5/rocketmq.yaml`                             |
| Topic name prefix | `RocketMQ-Benchmark`                                                | `Benchmark`                                                  |
| Topic attributes  | `+queue.type=BatchCQ` when `batchCQ`                                | `+message.type=NORMAL` or `DELAY`                            |
| Producers         | One shared producer per worker                                      | One producer per benchmark producer                          |
| Consumer group    | The subscription name; created by the broker on demand              | `<topic>_<subscription>`, created explicitly on every broker |
| Client tuning     | VIP channel, size limits, compression, auto-batching, back-pressure | None                                                         |
| Delay messages    | No                                                                  | `sendDelayMsg`, `delayTimeInSec`                             |
| Credentials       | One key pair for admin and clients                                  | Separate admin and client key pairs                          |

## Prerequisites and broker setup

The driver needs:

- a **RocketMQ 5 proxy** reachable from every worker at `grpcEndpoint` (port 8081 by default);
- the **name server** (`namesrvAddr`, port 9876) and every **broker** in `clusterName` reachable
  from the leader worker: it creates topics and consumer groups with the admin API, which connects
  to the broker addresses (`brokerIP1`) directly;
- the proxy able to reach the name server and the brokers;
- if ACL is enabled, admin credentials (`adminAccessKey`, `adminSecretKey`) allowed to create topics
  and groups, and client credentials (`accessKey`, `secretKey`) allowed to produce and consume.

Because both the proxy and the workers connect to the broker's advertised address, the simplest
setup is to run the workers on the same network as the brokers.

### Starting a test broker

The regression suite combines
[`stacks/rocketmq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/rocketmq.yaml)
(name server and broker; see [RocketMQ](rocketmq.md#starting-a-test-broker)) with
[`stacks/rocketmq5-proxy.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/rocketmq5-proxy.yaml),
which adds the proxy:

```bash
sh mqproxy -n namesrv:9876
```

using the same `apache/rocketmq:5.3.2` image (amd64 and arm64), with gRPC on port 8081. The workers
run on the same Compose network.

## Minimal configuration and benchmark command

```yaml
name: RocketMQ5
driverClass: io.openmessaging.benchmark.driver.rocketmq.RocketMQ5BenchmarkDriver
namesrvAddr: localhost:9876
clusterName: DefaultCluster
grpcEndpoint: localhost:8081
sendDelayMsg: false
```

What to change:

- `grpcEndpoint`: the proxy address from every worker's point of view (in the Compose suite,
  `proxy:8081`);
- `namesrvAddr` and `clusterName`: the name server and broker cluster used for administration (in
  the Compose suite, `namesrv:9876` and `DefaultCluster`);
- the credential keys, if ACL is enabled.

Run a local-mode benchmark from the repository root after `mvn -DskipTests install`, or from
`/benchmark` inside the benchmark image:

```bash
bin/benchmark \
  --drivers driver-rocketmq5/rocketmq.yaml \
  -o rocketmq5-1-topic-1-partition-1kb.json \
  workloads/1-topic-1-partition-1kb.yaml
```

For several worker hosts, see [Distributed runs](../getting-started/distributed-runs.md); install the
`rocketmq5` plugin bundle on every worker ([Runtime and plugins](../reference/runtime-and-plugins.md)).

## Configuration reference

Unknown keys are ignored.

|                Key                 |  Type   | Default |                                                Description                                                |
|------------------------------------|---------|---------|-----------------------------------------------------------------------------------------------------------|
| `namesrvAddr`                      | string  | none    | Name server address, used by the admin client.                                                            |
| `clusterName`                      | string  | none    | Broker cluster on which topics and consumer groups are created.                                           |
| `grpcEndpoint`                     | string  | none    | Proxy gRPC endpoint (`host:port`), used by producers and consumers.                                       |
| `sendDelayMsg`                     | boolean | `false` | Create topics with message type `DELAY` and send every message as a delay message.                        |
| `delayTimeInSec`                   | integer | none    | Delivery delay in seconds when `sendDelayMsg` is true. Required in that case; the shipped file sets `60`. |
| `adminAccessKey`, `adminSecretKey` | string  | empty   | ACL credentials for the admin client. Used when both are non-blank.                                       |
| `accessKey`, `secretKey`           | string  | empty   | Credentials for producers and consumers (static session credentials). Used when both are non-blank.       |

## Semantics

- **Topics.** The leader worker creates topics named `Benchmark-<index>-<random>` with the admin
  API, on every broker address in `clusterName` (masters and slaves), with permission 6 and
  `max(1, partitionsPerTopic / number of broker addresses)` read and write queues per broker. The
  topic attribute `+message.type` is `NORMAL`, or `DELAY` with `sendDelayMsg: true`.
- **Producers.** Each benchmark producer is its own gRPC producer bound to its topic. Sends are
  asynchronous and complete when the proxy returns a send receipt. The driver owns the message
  bytes even if the send is canceled.
- **Keys.** A message key is set as the RocketMQ message key (indexed for lookup). It does not
  choose the queue, so per-key ordering is not preserved.
- **Delay messages.** With `sendDelayMsg: true`, each message gets a delivery timestamp of the send
  time plus `delayTimeInSec`. The born timestamp stays the send time, so end-to-end latency includes
  the full delay.
- **Subscriptions and consumers.** For each consumer the driver creates (or updates) the consumer
  group `<topic>_<subscription>` on every broker, then starts a push consumer in that group
  subscribed to all tags (`*`). Including the topic in the group name keeps groups for different
  topics apart. Consumers in one group share messages; separate subscriptions each receive every
  message.
- **Acknowledgement.** The listener returns `SUCCESS` after the callback, and the client
  acknowledges the message.
- **End-to-end timestamp.** The consumer reports the message's born timestamp, set on the producer
  host. Latency compares producer and consumer host clocks; see
  [Latency and backlog](../methodology/latency-and-backlog.md).
- **Backlog workloads.** The framework blocks the consumer callback, which holds the push consumer's
  consumption threads until it is released.
- **Errors.** Admin startup failures abort initialization with their cause; producer and consumer
  creation errors complete their futures exceptionally.
- **Cleanup.** Each producer handle closes its own client, then fails any sends still pending,
  retaining a client-close failure as their cause; consumers close their clients. Closing a handle
  from a callback running synchronously inside its own `sendAsync` invocation or from any
  send-completion callback running inline with a client acknowledgement or close-induced failure
  throws `IllegalStateException` before changing the handle's state. Retry after the call or
  callback returns, or schedule close on another thread without waiting for it in the callback.
  The driver shuts down its admin client. Topics and consumer groups are not deleted.

## Tuning examples

The plugin exposes no client tuning options; the gRPC client runs with its defaults. What you can
change:

- **Queues.** Raise `partitionsPerTopic` so each broker gets more queues and more consumers can
  read in parallel.
- **Producers per topic.** Each benchmark producer is a separate gRPC client, so
  `producersPerTopic` adds client connections as well as send concurrency.
- **Delay messages.** `sendDelayMsg: true` measures the delay-message path. Keep `delayTimeInSec`
  well below 60 seconds: the readiness probe message is also delayed, and the coordinator waits
  60 seconds for it. Subtract the delay when reading end-to-end latency.

Read [Fair comparisons](../methodology/fair-comparisons.md) before comparing with other systems:
results through the proxy include an extra network hop compared with the classic plugin.

## Troubleshooting and verification

Run the Compose regression case; see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md):

```bash
e2e-tests/compose/run-e2e.sh --cases rocketmq5-contract
```

It runs a gRPC producer and consumer through the proxy. A passing case is a functional check at a
low fixed rate. It is not a performance result.

Common problems:

- **`Failed to start the RocketMQ admin tool`.** Check the name server address and admin credentials;
  initialization reports the startup cause.
- **`Failed to create topic` or `Failed to create subscription`.** The admin client could not reach
  the name server or a broker address, `clusterName` is wrong, or the admin credentials are missing.
- **Producer or consumer creation fails.** The proxy at `grpcEndpoint` is unreachable, does not know
  the topic yet, or rejects the client credentials.
- **`NullPointerException` when sending.** `sendDelayMsg` is true and `delayTimeInSec` is not set.
- **`Timed out waiting for consumers to be ready`.** Consumers have not received the probe message
  within 60 seconds; with delay messages, check `delayTimeInSec`.

See also [Troubleshooting](../guides/troubleshooting.md) and [Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-rocketmq5/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-rocketmq5/src/main/java/io/openmessaging/benchmark/driver/rocketmq)
- Shipped config: [`driver-rocketmq5/rocketmq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-rocketmq5/rocketmq.yaml)
- Compose driver config: [`drivers/rocketmq5.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/rocketmq5.yaml)
- Compose stacks: [`stacks/rocketmq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/rocketmq.yaml), [`stacks/rocketmq5-proxy.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/rocketmq5-proxy.yaml)

To write workloads and connect to your own cluster, see [Workloads](../guides/workloads.md) and
[Using your broker](../getting-started/using-your-broker.md).
