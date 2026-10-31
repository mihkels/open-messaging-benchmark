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

# NATS Streaming driver (legacy STAN)

This plugin benchmarks the legacy [NATS Streaming server](https://docs.nats.io/legacy/stan) (STAN). Each benchmark
topic is a channel and each subscription is a queue group.

> **Deprecated upstream.** NATS Streaming is deprecated by the NATS maintainers (support ended in June 2023) and is
> a different product from [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream). For JetStream use
> [`driver-nats`](../driver-nats).

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/nats-streaming/>

- Plugin ID: `nats-streaming`
- `driverClass`: `io.openmessaging.benchmark.driver.natsStreaming.NatsStreamingBenchmarkDriver`
- Shipped config: [`nats-streaming.yaml`](nats-streaming.yaml) (`nats://localhost:4222`, cluster ID `mycluster`)
- Tested with: `nats-streaming:0.25.6` ([stack file](../e2e-tests/compose/stacks/nats-streaming.yaml))

## Prerequisites

- A NATS Streaming server reachable from the coordinator (local mode) or every worker.
- `clusterId` in the driver file equal to the server's cluster ID (`-cid`); the driver default is `test-cluster`.
- The `nats-streaming` plugin bundle installed on the coordinator (local mode) and on every worker.

A local test server matching the shipped config:

```bash
docker run -d --name nats-streaming -p 4222:4222 -p 8222:8222 nats-streaming:0.25.6 -cid mycluster -m 8222 -store memory
```

## Usage

Edit `natsHostUrl` and `clusterId` in `nats-streaming.yaml` (optional: `maxPubAcksInFlight`, default 16384, and
`maxInFlight`, default 1024), then run from the repository root (after `mvn -DskipTests install`) or from
`/benchmark` in the container image:

```bash
bin/benchmark --drivers driver-nats-streaming/nats-streaming.yaml workloads/1-topic-1-partition-1kb.yaml
```

Smoke check against a server started by Docker Compose (see [`e2e-tests/compose/README.md`](../e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks nats-streaming
```

## Limitations

- `partitionsPerTopic` and message keys are ignored.
- Subscriptions are non-durable queue subscriptions that receive only messages published after subscribing.
- End-to-end latency uses the server-assigned message timestamp (server clock), not the producer's send time.
- A connection failure is only logged (`nats streaming create producer exception` / `... consumer exception`); the
  run then fails without a clearer error.
- The driver does not delete channels.

## Further reading

- [Workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/)
- [Distributed runs](https://mihkels.github.io/open-messaging-benchmark/getting-started/distributed-runs/)
- [Runtime and plugins](https://mihkels.github.io/open-messaging-benchmark/reference/runtime-and-plugins/)
- [Troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/)

## Legacy deployment files

[`deploy/`](deploy) contains Terraform and Ansible files for AWS (and Alibaba Cloud under `deploy/ali`) from the
original project. They are unmaintained: they reference older distribution names, Java versions and launcher
settings. Adapt them to Java 27, the current distribution and the plugin layout (complete `drivers/` bundles on
every worker) before using them.
