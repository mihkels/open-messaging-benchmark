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

# Apache RocketMQ

The RocketMQ plugin benchmarks Apache RocketMQ with the classic remoting client
(`rocketmq-client`). Clients find brokers through the name server and connect to the brokers
directly. Topics are created with the admin API; producers send asynchronously and consumers are
push consumers in a consumer group. For the gRPC client that connects through the RocketMQ 5 proxy,
see [RocketMQ 5](rocketmq5.md).

|       Item        |                                                      Value                                                       |
|-------------------|------------------------------------------------------------------------------------------------------------------|
| Plugin ID         | `rocketmq`                                                                                                       |
| Module            | `driver-rocketmq`                                                                                                |
| `driverClass`     | `io.openmessaging.benchmark.driver.rocketmq.RocketMQBenchmarkDriver`                                             |
| Shipped config    | `driver-rocketmq/rocketmq.yaml`                                                                                  |
| Client library    | `rocketmq-client` and `rocketmq-tools` 5.5.1, `rocketmq-acl` 5.3.2                                               |
| Tested broker     | `apache/rocketmq:5.3.2`: one name server and one broker (Compose stack `rocketmq`)                               |
| Regression cases  | `rocketmq-contract`, `rocketmq-partitioned`                                                                      |
| Known limitations | Keys do not select queues; queue count is divided across all broker addresses; topics and groups are not deleted |

## Prerequisites and broker setup

The driver needs:

- a **name server** (`namesrvAddr`, port 9876) reachable from every worker;
- a **broker cluster** registered under `clusterName`. Every broker must advertise an address
  (`brokerIP1`) that the workers can reach, on its listen port (10911 by default);
- permission for the admin client to create and update topics on every broker. With ACL enabled on
  the brokers, set `accessKey` and `secretKey`.

The driver creates topics itself. It does not create consumer groups, so the brokers must allow
automatic subscription group creation (`autoCreateSubscriptionGroup=true`, the broker default), or
you must create the groups beforehand.

### Starting a test broker

The regression suite uses
[`e2e-tests/compose/stacks/rocketmq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/rocketmq.yaml):
a name server (`sh mqnamesrv`) and a broker started with this configuration:

```properties
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0
brokerIP1=broker
autoCreateTopicEnable=true
autoCreateSubscriptionGroup=true
```

The image runs natively on amd64 and arm64. The broker advertises the Compose service name `broker`,
so only clients on the same Compose network can reach it. To run a broker that clients on the host
can use, advertise `127.0.0.1` and publish the ports:

```bash
docker network create rocketmq
docker run -d --name namesrv --network rocketmq -p 9876:9876 \
  apache/rocketmq:5.3.2 sh mqnamesrv
docker run -d --name broker --network rocketmq -p 10911:10911 \
  -e NAMESRV_ADDR=namesrv:9876 apache/rocketmq:5.3.2 \
  sh -c 'printf "brokerClusterName=DefaultCluster\nbrokerName=broker-a\nbrokerId=0\nbrokerIP1=127.0.0.1\n" > $HOME/broker.conf && exec sh mqbroker -c $HOME/broker.conf'
```

## Minimal configuration and benchmark command

```yaml
name: RocketMQ
driverClass: io.openmessaging.benchmark.driver.rocketmq.RocketMQBenchmarkDriver
clusterName: DefaultCluster
namesrvAddr: 127.0.0.1:9876
vipChannelEnabled: false
```

What to change:

- `namesrvAddr`: the name server address from every worker's point of view (in the Compose suite,
  `namesrv:9876`). Several name servers are separated with `;`;
- `clusterName`: the `brokerClusterName` of your brokers;
- `accessKey` and `secretKey`, if the brokers have ACL enabled.

The shipped `rocketmq.yaml` also enables BatchCQ topics, producer auto-batching and back-pressure
(see the reference below).

Run a local-mode benchmark from the repository root after `mvn -DskipTests install`, or from
`/benchmark` inside the benchmark image:

```bash
bin/benchmark \
  --drivers driver-rocketmq/rocketmq.yaml \
  -o rocketmq-1-topic-1-partition-1kb.json \
  workloads/1-topic-1-partition-1kb.yaml
```

For several worker hosts, see [Distributed runs](../getting-started/distributed-runs.md); install the
`rocketmq` plugin bundle on every worker ([Runtime and plugins](../reference/runtime-and-plugins.md)).

## Configuration reference

All keys are optional in the configuration class and unknown keys are ignored. A key that is not set
leaves the client default unchanged. `namesrvAddr` and `clusterName` are needed in practice.

|             Key              |  Type   |  Shipped value   |                                                                     Description                                                                     |
|------------------------------|---------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `namesrvAddr`                | string  | `127.0.0.1:9876` | Name server address list.                                                                                                                           |
| `clusterName`                | string  | `DefaultCluster` | Broker cluster on which topics are created.                                                                                                         |
| `vipChannelEnabled`          | boolean | `false`          | Use the broker's VIP channel (listen port − 2). Applied to producers and consumers.                                                                 |
| `maxMessageSize`             | integer | client default   | Largest message the producer accepts, in bytes.                                                                                                     |
| `compressMsgBodyOverHowmuch` | integer | client default   | Compress message bodies larger than this many bytes.                                                                                                |
| `batchCQ`                    | boolean | `true`           | Create topics with the `BatchCQ` queue type (topic attribute `+queue.type=BatchCQ`). Requires a broker that supports topic attributes (RocketMQ 5). |
| `autoBatch`                  | boolean | `true`           | Producer automatically groups asynchronous sends into batches.                                                                                      |
| `batchMaxBytes`              | integer | client default   | Maximum size of one automatic batch.                                                                                                                |
| `batchMaxDelayMs`            | integer | client default   | Maximum time a message waits for its automatic batch.                                                                                               |
| `totalBatchMaxBytes`         | integer | client default   | Memory limit for all pending automatic batches.                                                                                                     |
| `enableBackpressure`         | boolean | `true`           | Limit in-flight asynchronous sends; a new send waits for a free slot when the limit is reached.                                                     |
| `backpressureConcurrency`    | integer | `1024`           | Maximum number of in-flight asynchronous sends with back-pressure enabled.                                                                          |
| `accessKey`, `secretKey`     | string  | empty            | ACL credentials. When both are non-blank, the admin client, producer and consumers sign requests with them.                                         |

## Semantics

- **Topics.** The leader worker creates topics named `RocketMQ-Benchmark-<index>-<random>` with the
  admin API. It looks up every broker address in `clusterName` (masters and slaves) and creates the
  topic on each with permission 6 (read and write) and the same number of read and write queues:
  `max(1, partitionsPerTopic / number of broker addresses)`. The total number of queues can
  therefore differ from `partitionsPerTopic`, and slave addresses also count in the division.
- **Producers.** One `DefaultMQProducer` (with a random producer group) is shared by all benchmark
  producers of a worker. Each send is asynchronous and completes when the broker acknowledges it.
  The driver owns the message bytes even if the send is canceled.
- **Keys.** A message key is set as the RocketMQ message key, which the broker indexes for lookup.
  It does not choose the queue: the producer spreads messages across queues regardless of the key,
  so per-key ordering is not preserved.
- **Subscriptions and consumers.** Each benchmark consumer is a `DefaultMQPushConsumer` in the
  consumer group named after the subscription, subscribed to all tags (`*`) in the default
  clustering mode. Consumers in one group share the topic's queues; separate subscriptions each
  receive every message.
- **Acknowledgement.** The listener processes messages concurrently and returns `CONSUME_SUCCESS`
  after the callback; the client then commits offsets in the background.
- **End-to-end timestamp.** The consumer reports the message's born timestamp, set by the producer
  host when the message is created. Latency compares producer and consumer host clocks; see
  [Latency and backlog](../methodology/latency-and-backlog.md).
- **Backlog workloads.** The framework blocks the consumer callback, which holds the push consumer's
  consume threads until it is released.
- **Errors.** Admin startup failures abort initialization with their cause. Producer and consumer
  startup failures complete creation exceptionally and shut down the partially created client.
- **Cleanup.** Closing a producer handle fails its pending sends without shutting down the shared
  client. The driver shuts down the shared producer and admin client; each consumer owns its client.
  Closing a handle from a callback running synchronously inside its own `sendAsync` invocation or
  from a send-completion callback running inline with a client acknowledgement or close-induced
  failure throws `IllegalStateException` before changing the handle's state. Retry after the call
  or callback returns, or schedule close on another thread without waiting for it in the callback.
  Topics and consumer groups are not deleted.

## Tuning examples

Measure latency and throughput together and read
[Fair comparisons](../methodology/fair-comparisons.md) before comparing systems.

- **Automatic batching.** With `autoBatch: true`, tune `batchMaxBytes` and `batchMaxDelayMs`: larger
  batches and longer delays raise throughput and add publish latency. Set `autoBatch: false` to send
  each message on its own.
- **Back-pressure.** `backpressureConcurrency` caps in-flight sends. A low cap limits the producer's
  rate; a high cap uses more memory and lets queues build up in the client.
- **Queues.** Raise `partitionsPerTopic` so each broker gets more queues and more consumers can read
  in parallel. Remember the division by the number of broker addresses.
- **Queue type.** `batchCQ: false` creates topics with the default queue type, for brokers that do
  not support topic attributes.

## Troubleshooting and verification

Run the Compose regression cases; see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md):

```bash
e2e-tests/compose/run-e2e.sh --stacks rocketmq
```

This runs `rocketmq-contract` (topic creation with the admin API, asynchronous send) and
`rocketmq-partitioned` (4 queues, keyed messages, 2 consumers in one group). A passing case is a
functional check at a low fixed rate. It is not a performance result.

Common problems:

- **`Failed to start the RocketMQ admin tool`.** Check the name server address and admin credentials;
  initialization reports the startup cause.
- **`get brokerAddr return null, clusterName: ...` or `Failed to create topic`.** `clusterName` does
  not match the brokers, or the name server is unreachable.
- **Sends time out after topics are created.** The broker advertises an address (`brokerIP1`) the
  workers cannot reach, or `vipChannelEnabled: true` with the VIP port not published.
- **Consumer creation fails or readiness times out.** Check the reported startup cause and whether
  the broker creates subscription groups automatically.
- **ACL errors.** Both `accessKey` and `secretKey` must be set; with one of them blank, requests are
  unsigned.

See also [Troubleshooting](../guides/troubleshooting.md) and [Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-rocketmq/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-rocketmq/src/main/java/io/openmessaging/benchmark/driver/rocketmq)
- Shipped config: [`rocketmq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-rocketmq/rocketmq.yaml)
- Compose driver config: [`drivers/rocketmq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/rocketmq.yaml)
- Compose stack: [`stacks/rocketmq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/rocketmq.yaml)
- Legacy Alibaba Cloud Terraform/Ansible files, unmaintained: [`driver-rocketmq/deploy`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-rocketmq/deploy)

To write workloads and connect to your own cluster, see [Workloads](../guides/workloads.md) and
[Using your broker](../getting-started/using-your-broker.md).
