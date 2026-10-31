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

# Apache RocketMQ driver

This plugin benchmarks Apache RocketMQ with the classic remoting client (`rocketmq-client` 5.5.1).
Clients find brokers through the name server and connect to the brokers directly. Topics are
created with the admin API; producers send asynchronously (optionally with automatic batching and
back-pressure) and consumers are push consumers in a consumer group named after the subscription.
For the gRPC client that goes through the RocketMQ 5 proxy, use [`driver-rocketmq5`](../driver-rocketmq5).

- Plugin ID: `rocketmq`
- `driverClass`: `io.openmessaging.benchmark.driver.rocketmq.RocketMQBenchmarkDriver`
- Config: [`rocketmq.yaml`](rocketmq.yaml)

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/rocketmq/>

## Prerequisites

- A name server (`namesrvAddr`, port 9876) and a broker cluster (`clusterName`) whose advertised
  broker addresses (`brokerIP1`, port 10911) are reachable from every worker.
- Permission to create topics on every broker (set `accessKey`/`secretKey` if ACL is enabled).
- Automatic subscription group creation on the brokers (the default), or pre-created groups.
- The shipped config sets `batchCQ: true`, which needs a broker with topic attribute support
  (RocketMQ 5). Set it to `false` for older brokers.

## Usage

From the repository root (after `mvn -DskipTests install`) or from `/benchmark` in the benchmark
image:

```bash
bin/benchmark --drivers driver-rocketmq/rocketmq.yaml workloads/1-topic-1-partition-1kb.yaml
```

## Smoke check

```bash
e2e-tests/compose/run-e2e.sh --stacks rocketmq
```

This runs `rocketmq-contract` and `rocketmq-partitioned` against `apache/rocketmq:5.3.2`. See the
[Compose regression suite](../e2e-tests/compose/README.md). A passing case is a functional check, not
a performance result.

## Limitations

- Message keys are set as RocketMQ keys (indexed), not used to choose queues; per-key order is not
  kept.
- Queues per broker are `max(1, partitionsPerTopic / number of broker addresses)`, and slave
  addresses count in that division.
- Failures to start the admin client, producer or consumers are only logged.
- Topics and consumer groups are not deleted after the run.

More: [workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/),
[results](https://mihkels.github.io/open-messaging-benchmark/guides/results/),
[troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/).

## Legacy deployment files

[`deploy/`](deploy) contains Terraform (`provision-rocketmq-alicloud.tf`) and Ansible
(`deploy.yaml`) files for an Alibaba Cloud cluster from the original project. They are unmaintained.
Adapt them for Java 27 and the current distribution and plugin layout (`drivers/` bundles on every
worker) before use.
