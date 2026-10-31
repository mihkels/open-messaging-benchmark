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

# RabbitMQ driver

This plugin benchmarks [RabbitMQ](https://www.rabbitmq.com/) over AMQP 0-9-1. Each benchmark topic is a durable
fanout exchange and each subscription is a durable classic, quorum or stream queue bound to it.

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/rabbitmq/>

- Plugin ID: `rabbitmq`
- `driverClass`: `io.openmessaging.benchmark.driver.rabbitmq.RabbitMqBenchmarkDriver`
- Shipped config: [`rabbitmq.yaml`](rabbitmq.yaml) (quorum queues, `amqp://localhost`)
- Tested with: `rabbitmq:4.1-management` ([stack file](../e2e-tests/compose/stacks/rabbitmq.yaml))

## Prerequisites

- A RabbitMQ broker reachable on its AMQP port (5672) from the coordinator (local mode) or every worker.
- A user allowed to declare exchanges and queues in the virtual host `/`. Credentials go in the URI,
  `amqp://user:password@host:5672`; without them the client uses `guest`/`guest`.
- The `rabbitmq` plugin bundle installed on the coordinator (local mode) and on every worker.

A local test broker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.1-management
```

## Usage

Edit `amqpUris` and `queueType` (`CLASSIC`, `QUORUM` or `STREAM`, upper case) in `rabbitmq.yaml`, then run from the
repository root (after `mvn -DskipTests install`) or from `/benchmark` in the container image:

```bash
bin/benchmark --drivers driver-rabbitmq/rabbitmq.yaml workloads/1-topic-1-partition-1kb.yaml
```

Smoke check against a broker started by Docker Compose (see [`e2e-tests/compose/README.md`](../e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks rabbitmq
```

## Limitations

- `partitionsPerTopic` must be 1; other values fail topic creation.
- Message keys are sent as routing keys but ignored by the fanout exchange.
- End-to-end latency uses the AMQP `timestamp` property, which has one-second resolution.
- Stream queues use manual acknowledgements with a fixed prefetch of 1000; classic and quorum queues use automatic
  acknowledgements.
- The URI path (virtual host) and TLS are not supported; only host, port, user and password are used.
- Exchanges and queues are not deleted after a run.

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
