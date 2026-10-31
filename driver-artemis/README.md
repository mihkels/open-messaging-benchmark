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

# Apache ActiveMQ Artemis driver

This plugin benchmarks Apache ActiveMQ Artemis with the native core client (`artemis-core-client`
2.57.0), without JMS. A benchmark topic is a multicast address; each subscription is a durable multicast
queue named `<address>-<subscription>`.

To benchmark Artemis through JMS instead, use the [JMS driver](../driver-jms/README.md) with the
`artemis-jms-client-all` provider. That path adds JMS shared durable subscriptions, message groups,
selectors and transactions, and measures the JMS client layer as well.

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/artemis/>

## Plugin and configuration files

- Plugin ID: `artemis`
- `driverClass`: `io.openmessaging.benchmark.driver.artemis.ArtemisBenchmarkDriver`
- Configuration: [`artemis.yaml`](artemis.yaml), with a single key, `brokerAddress`
  (`tcp://localhost:61616`). It accepts Artemis client URL parameters.

The driver fixes `confirmationWindowSize` at 1 MiB and sets `blockOnDurableSend`,
`blockOnNonDurableSend` and `blockOnAcknowledge` to `false`.

## Prerequisites

- An Artemis broker with the core acceptor reachable from the driver.
- Anonymous access (the driver sends no credentials), with permission to create addresses and durable
  queues, send and consume.
- The plugin bundle on the coordinator (local mode) and every worker: `drivers/driver-artemis` in the
  distribution, `driver-artemis/target/omb-plugin` in a source checkout after `mvn -DskipTests install`.

A test broker matching the regression stack:

```bash
docker run -d --name artemis -p 61616:61616 \
  -e ANONYMOUS_LOGIN=true -e JAVA_ARGS_APPEND=-Xmx768m apache/artemis:2.57.0
```

## Usage

Local mode, from the repository root or from `/benchmark` in the container (in the container, replace
`localhost` with an address reachable from it):

```bash
bin/benchmark --drivers driver-artemis/artemis.yaml -o artemis-1-topic.json \
  workloads/1-topic-1-partition-1kb.yaml
```

For remote workers, add `--workers` (see
[Distributed runs](https://mihkels.github.io/open-messaging-benchmark/getting-started/distributed-runs/)).

## Smoke check

```bash
e2e-tests/compose/run-e2e.sh --stacks artemis
```

This runs `artemis-contract` and `artemis-fanout`. See the
[Compose regression suite](../e2e-tests/compose/README.md). A passing case is a functional check, not a
performance result.

## Limitations

- `partitionsPerTopic` must be 1.
- Use one consumer per subscription: each consumer creates the subscription queue, so a second consumer
  on the same subscription is expected to fail.
- Message keys are ignored; no transactions, selectors or credentials.
- Addresses and queues are not deleted after a run.

## Further reading

- [Artemis driver page](https://mihkels.github.io/open-messaging-benchmark/drivers/artemis/)
- [JMS driver page](https://mihkels.github.io/open-messaging-benchmark/drivers/jms/)
- [Workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/),
  [Troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/)

