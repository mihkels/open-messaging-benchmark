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

# NATS JetStream driver

This plugin benchmarks [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream). Each benchmark topic is a
JetStream stream with file or memory storage, and each subscription is a durable push consumer with a deliver group.

It is not the NATS Streaming (STAN) driver; for that deprecated product see [`driver-nats-streaming`](../driver-nats-streaming).

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/nats/>

- Plugin ID: `nats`
- `driverClass`: `io.openmessaging.benchmark.driver.nats.NatsBenchmarkDriver`
- Shipped config: [`nats.yaml`](nats.yaml) (`localhost:4222`, `replicationFactor: 3`)
- Tested with: `nats:2.11-alpine -js` ([stack file](../e2e-tests/compose/stacks/nats.yaml))

## Prerequisites

- A NATS server or cluster with JetStream enabled, reachable from the coordinator (local mode) or every worker.
- At least `replicationFactor` JetStream servers. The shipped file uses 3; set `replicationFactor: 1` for a single
  server. The value must be between 1 and 5.
- The `nats` plugin bundle installed on the coordinator (local mode) and on every worker.

A local single server:

```bash
docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:2.11-alpine -js -sd /data -m 8222
```

## Usage

Edit `natsHostUrl`, `replicationFactor` and optionally `storageType` (`file` or `memory`, lower case) and `maxBytes`
in `nats.yaml`, then run from the repository root (after `mvn -DskipTests install`) or from `/benchmark` in the
container image:

```bash
bin/benchmark --drivers driver-nats/nats.yaml workloads/1-topic-1-partition-1kb.yaml
```

Smoke check against a server started by Docker Compose (see [`e2e-tests/compose/README.md`](../e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks nats
```

## Limitations

- `partitionsPerTopic` and message keys are ignored: one stream and one subject per topic.
- The publish timestamp is written into the first 8 bytes of the payload, so messages must be at least 8 bytes and
  latency depends on synchronised worker clocks.
- `storageType` must be lower case (`file`, `memory`).
- `maxBytes` is a 32-bit integer (at most about 2 GiB); `-1` means unlimited.
- No authentication or TLS settings.
- Streams and durable consumers are not deleted after a run.

## Further reading

- [Workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/)
- [Distributed runs](https://mihkels.github.io/open-messaging-benchmark/getting-started/distributed-runs/)
- [Runtime and plugins](https://mihkels.github.io/open-messaging-benchmark/reference/runtime-and-plugins/)
- [Troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/)

## Legacy deployment files

[`deploy/`](deploy) contains Terraform and Ansible files for AWS (and Alibaba Cloud under `deploy/alicloud`) from the
original project. They are unmaintained: they reference older distribution names, Java versions and launcher
settings. Adapt them to Java 27, the current distribution and the plugin layout (complete `drivers/` bundles on
every worker) before using them.
