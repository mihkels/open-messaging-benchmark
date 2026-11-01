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

# Apache RocketMQ 5 driver

This plugin benchmarks Apache RocketMQ through the RocketMQ 5 gRPC client
(`rocketmq-client-java` 5.0.7). Producers and consumers connect to a RocketMQ proxy; topics and
consumer groups are created with the classic admin API (`rocketmq-tools` 5.1.0) through the name
server and brokers. It can also send delay messages.

- Plugin ID: `rocketmq5`
- `driverClass`: `io.openmessaging.benchmark.driver.rocketmq.RocketMQ5BenchmarkDriver`
- Config: [`rocketmq.yaml`](rocketmq.yaml). The file has the same name as the classic plugin's
  config, so always use the full path `driver-rocketmq5/rocketmq.yaml`.

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/rocketmq5/>

## Differences from `driver-rocketmq`

- gRPC to the proxy (`grpcEndpoint`) instead of the remoting protocol to brokers.
- One gRPC producer per benchmark producer instead of one shared producer per worker.
- Consumer groups are named `<topic>_<subscription>` and created explicitly on every broker.
- No client tuning options (no batching, back-pressure, compression or BatchCQ).
- Delay messages (`sendDelayMsg`, `delayTimeInSec`) and topics typed `NORMAL` or `DELAY`.
- Separate admin (`adminAccessKey`/`adminSecretKey`) and client (`accessKey`/`secretKey`)
  credentials.

## Prerequisites

- A RocketMQ 5 proxy reachable from every worker at `grpcEndpoint` (port 8081).
- The name server (`namesrvAddr`) and every broker in `clusterName` reachable from the leader worker
  for topic and group creation.

## Usage

From the repository root (after `mvn -DskipTests install`) or from `/benchmark` in the benchmark
image:

```bash
bin/benchmark --drivers driver-rocketmq5/rocketmq.yaml workloads/1-topic-1-partition-1kb.yaml
```

## Smoke check

```bash
e2e-tests/compose/run-e2e.sh --cases rocketmq5-contract
```

The `rocketmq5` stack adds a proxy (`sh mqproxy -n namesrv:9876`, `apache/rocketmq:5.3.2`) to the
RocketMQ name server and broker. See the [Compose regression suite](../e2e-tests/compose/README.md).
A passing case is a functional check, not a performance result.

Admin startup failures abort initialization, and driver shutdown closes the admin client.
Closing a producer from a callback running inside its own `sendAsync` call is rejected; retry after
the send returns or schedule close on another thread without waiting in the callback.

## Limitations

- `sendDelayMsg: true` without `delayTimeInSec` fails the send with a `NullPointerException`. Delays
  count in end-to-end latency, and a delay near 60 seconds makes the readiness probe time out.
- Message keys are set as RocketMQ keys (indexed), not used to choose queues.
- Queues per broker are `max(1, partitionsPerTopic / number of broker addresses)`, slaves included.
- Topics and consumer groups are not deleted.

More: [workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/),
[results](https://mihkels.github.io/open-messaging-benchmark/guides/results/),
[troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/).

## Legacy deployment files

This module has no deployment files of its own. The unmaintained Alibaba Cloud files in
[`driver-rocketmq/deploy`](../driver-rocketmq/deploy) do not set up a proxy; adapt them for Java 27,
the current plugin layout and a RocketMQ 5 proxy before use.
