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

# Apache ActiveMQ Artemis

The Artemis driver benchmarks Apache ActiveMQ Artemis with the native Artemis core client
(`artemis-core-client`), without JMS. A benchmark topic is a multicast address, and each benchmark
subscription is a durable multicast queue bound to that address.

|       Item        |                                                                   Value                                                                    |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID         | `artemis`                                                                                                                                  |
| Module            | `driver-artemis`                                                                                                                           |
| `driverClass`     | `io.openmessaging.benchmark.driver.artemis.ArtemisBenchmarkDriver`                                                                         |
| Example config    | `driver-artemis/artemis.yaml`                                                                                                              |
| Client library    | `org.apache.artemis:artemis-core-client` 2.57.0                                                                                            |
| Tested broker     | `apache/artemis:2.57.0` (Compose stack `artemis`)                                                                                          |
| Regression cases  | `artemis-contract`, `artemis-fanout`                                                                                                       |
| Known limitations | One partition per topic; no authentication settings; keys are ignored; one consumer per subscription; addresses and queues are not deleted |

## Artemis native or Artemis via JMS

Artemis can also be benchmarked through the [JMS driver](jms.md#apache-activemq-artemis). The two
measure different client stacks:

|     Aspect     |                   `artemis` plugin (this page)                   |                              `jms` plugin with the Artemis provider                               |
|----------------|------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| Client         | Artemis core client API, bundled with the plugin                 | JMS API over `artemis-jms-client-all`, which you put in the JMS `ext/` directory                  |
| Configuration  | `brokerAddress` only                                             | Connection factory URL with any Artemis client URL parameters                                     |
| Topic creation | Address created explicitly as multicast                          | Created by the broker on first use                                                                |
| Subscriptions  | Durable multicast queue `<topic>-<subscription>`, one consumer   | JMS 2.0 shared durable subscription (several consumers), or non-durable consumers in JMS 1.1 mode |
| Keys           | Ignored                                                          | Sent as `JMSXGroupID` (message groups)                                                            |
| Transactions   | Not supported                                                    | `sendWithTransactions` (one message per transaction)                                              |
| Selectors      | Not supported                                                    | `messageSelector` and `properties`                                                                |
| Producer flow  | Asynchronous sends confirmed through a 1 MiB confirmation window | JMS 2.0 asynchronous send or JMS 1.1 synchronous send                                             |

Use the native plugin to measure Artemis with the least client overhead; use the JMS plugin when the
application under study uses JMS, or to compare Artemis with other JMS providers.

## Prerequisites and broker setup

You need an Artemis broker whose core protocol acceptor (port 61616 by default) is reachable from the
driver, and that:

- accepts sessions **without credentials**: the driver does not send a user name or password, so the
  broker must allow anonymous login, or the default user must have the needed permissions;
- lets that user create addresses and durable queues, send and consume.

No client jar needs to be added; the plugin bundles the core client. Install the plugin bundle on the
coordinator (local mode) and every worker: `drivers/driver-artemis` in the distribution,
`driver-artemis/target/omb-plugin` in a source checkout after `mvn -DskipTests install`.

### Start a test broker

Same image and settings as the regression stack
[`e2e-tests/compose/stacks/artemis.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/artemis.yaml):

```bash
docker run -d --name artemis -p 61616:61616 \
  -e ANONYMOUS_LOGIN=true \
  -e JAVA_ARGS_APPEND=-Xmx768m \
  apache/artemis:2.57.0
```

This broker is for functional checks. Before measuring, size the broker's journal and memory for the
workload and follow [Fair comparisons](../methodology/fair-comparisons.md).

## Minimal configuration and run

`driver-artemis/artemis.yaml`:

```yaml
name: Artemis
driverClass: io.openmessaging.benchmark.driver.artemis.ArtemisBenchmarkDriver

brokerAddress: tcp://localhost:61616
```

Host names to change:

|            Where the driver runs            |                         `brokerAddress`                          |
|---------------------------------------------|------------------------------------------------------------------|
| Source checkout or distribution on the host | `tcp://localhost:61616` (as shipped)                             |
| Benchmark container (`/benchmark`)          | An address reachable from the container, not `localhost`         |
| Compose regression suite                    | `tcp://artemis:61616` (`e2e-tests/compose/drivers/artemis.yaml`) |

Run in local mode from the repository root or from `/benchmark` in the container:

```bash
bin/benchmark --drivers driver-artemis/artemis.yaml -o artemis-1-topic.json \
  workloads/1-topic-1-partition-1kb.yaml
```

This workload runs for 15 minutes at 50,000 messages/s; copy it and lower `producerRate` and
`testDurationMinutes` for a quick check (see [Workloads](../guides/workloads.md)). The workload must
use `partitionsPerTopic: 1`. For remote workers, see
[Distributed runs](../getting-started/distributed-runs.md); for an existing broker, see
[Using your broker](../getting-started/using-your-broker.md).

## Configuration reference

|       Key       |  Type  | Default  |                                             Description                                              |
|-----------------|--------|----------|------------------------------------------------------------------------------------------------------|
| `name`          | string | required | Used in the result file name and the result's `driver` field.                                        |
| `driverClass`   | string | required | `io.openmessaging.benchmark.driver.artemis.ArtemisBenchmarkDriver`                                   |
| `brokerAddress` | string | required | Artemis client URL passed to `ActiveMQClient.createServerLocator`, for example `tcp://broker:61616`. |

`brokerAddress` accepts the URL syntax of the Artemis core client, including URL parameters such as
`sslEnabled` or `consumerWindowSize` (see the
[Artemis client configuration](https://activemq.apache.org/components/artemis/documentation/latest/configuring-transports.html)).
Settings that the driver sets in code after parsing the URL override the same URL parameters:

|  Server locator setting  | Value set by the driver |                                         Effect                                         |
|--------------------------|-------------------------|----------------------------------------------------------------------------------------|
| `confirmationWindowSize` | 1 MiB                   | Enables asynchronous send confirmations; the broker confirms every 1 MiB of sent data. |
| `blockOnDurableSend`     | `false`                 | Durable sends do not wait for the broker.                                              |
| `blockOnNonDurableSend`  | `false`                 | Non-durable sends do not wait for the broker.                                          |
| `blockOnAcknowledge`     | `false`                 | Acknowledgements are sent asynchronously.                                              |

Unknown keys in the driver file are ignored. There are no keys for credentials, durability or
consumer settings.

## Semantics

| Benchmark concept |                                            Implementation                                             |
|-------------------|-------------------------------------------------------------------------------------------------------|
| Topic             | Multicast address `test-<7-digit index>-<random suffix>`, created with the auto-created flag          |
| Partitions        | Not supported; `partitionsPerTopic` other than 1 fails with `Partitions are not supported in Artemis` |
| Subscription      | Durable multicast queue `<address>-<subscription name>` bound to the address                          |
| Producer          | One core session and producer per benchmark producer; durable messages                                |
| Consumer          | One core session and consumer on the subscription queue, with a message handler                       |
| Key               | Ignored                                                                                               |

**Producing.** Messages are sent asynchronously and count as published when the broker confirms them
through the `SendAcknowledgementHandler`. Messages are durable, so the broker persists them to its
journal according to its own journal settings before confirming.

**Consuming.** The message handler copies the body, calls the benchmark callback, and then acknowledges
the message. Acknowledgements are asynchronous (`blockOnAcknowledge=false`) and may be batched by the
client session. Delivery is at-least-once. Every subscription queue receives a copy of every message on
its address, so several subscriptions give fan-out, as tested by the `artemis-fanout` case (2 addresses,
2 subscriptions each).

**Consumers per subscription.** Each benchmark consumer creates its subscription queue before consuming.
The regression suite uses one consumer per subscription only; with `consumerPerSubscription` greater than
1, the second consumer tries to create a queue that already exists, so expect that configuration to fail.

**End-to-end latency.** The producer sets the core message timestamp to its wall clock; the consumer
reports latency as its wall clock minus the message timestamp. Cross-host latency depends on clock
synchronization.

**Backlog.** The worker pauses consumers inside the message handler (see
[Latency and backlog](../methodology/latency-and-backlog.md)). Messages that are not delivered stay in
the durable queue, subject to the broker's address-full policy (paging by default).

**Cleanup.** The driver closes producers, consumers, sessions and the session factory at the end of a
run. Addresses and queues are not deleted. Addresses are flagged as auto-created, so the broker's
auto-delete settings for addresses can remove them once they have no queues; the durable queues stay
until you delete them.

## Tuning examples

- **Send confirmations.** The 1 MiB confirmation window is fixed in code. Producer-side flow control can
  be adjusted with URL parameters in `brokerAddress`, for example `producerWindowSize`.
- **Consumer prefetch.** `consumerWindowSize` in `brokerAddress` sets the number of bytes the broker
  pushes to each consumer before it acknowledges.
- **Broker side.** Journal type, `journal-sync-transactional`/`journal-sync-non-transactional` and
  address-full policy in `broker.xml` decide durability and throughput; set them on the broker, not in
  the driver.

Compare Artemis results with other brokers only with comparable durability settings; see
[Fair comparisons](../methodology/fair-comparisons.md).

## Troubleshooting and verification

```bash
e2e-tests/compose/run-e2e.sh --stacks artemis
e2e-tests/compose/run-e2e.sh --cases artemis-contract
```

A passing case is a functional check at a low rate, not a performance result. See the
[Compose regression suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md).

|                       Symptom                       |                                            Cause and fix                                             |
|-----------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `Partitions are not supported in Artemis`           | Set `partitionsPerTopic: 1` in the workload.                                                         |
| Security exception when creating the session        | The broker requires credentials; enable anonymous login for the benchmark user or use a test broker. |
| Permission errors when creating addresses or queues | The user lacks `createAddress`, `createDurableQueue`, `send` or `consume` permissions.               |
| Queue already exists when creating consumers        | `consumerPerSubscription` is greater than 1; use 1.                                                  |
| Connection refused or timeout                       | `brokerAddress` is not reachable from the driver; in a container, do not use `localhost`.            |

See also [Troubleshooting](../guides/troubleshooting.md).

Source and configuration files:

- Driver source: [`driver-artemis/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-artemis/src/main/java/io/openmessaging/benchmark/driver/artemis)
- Shipped configuration: [`driver-artemis/artemis.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-artemis/artemis.yaml)
- Regression driver config: [`e2e-tests/compose/drivers/artemis.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/artemis.yaml)
- Related driver: [JMS](jms.md)

