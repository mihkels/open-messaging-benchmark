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

# RabbitMQ

The RabbitMQ driver publishes through one durable fanout exchange per benchmark topic and consumes from one
durable queue per subscription, using the RabbitMQ Java AMQP 0-9-1 client. The queue type is classic, quorum or
stream, selected with `queueType`.

|          Item          |                                                                            Value                                                                            |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID              | `rabbitmq`                                                                                                                                                  |
| Module                 | `driver-rabbitmq`                                                                                                                                           |
| `driverClass`          | `io.openmessaging.benchmark.driver.rabbitmq.RabbitMqBenchmarkDriver`                                                                                        |
| Example config shipped | `driver-rabbitmq/rabbitmq.yaml` (quorum queues, `amqp://localhost`)                                                                                         |
| Tested broker          | `rabbitmq:4.1-management` (Compose stack `rabbitmq`, RabbitMQ 4.1)                                                                                          |
| Regression cases       | `rabbitmq-contract` (quorum), `rabbitmq-classic-fanout` (classic, persistent messages), `rabbitmq-stream` (stream)                                          |
| Known limitations      | One partition per topic; keys do not affect routing; end-to-end latency has one-second resolution; no TLS or virtual host selection; no cleanup after a run |

## Prerequisites and broker setup

The driver needs:

- AMQP 0-9-1 access (port 5672 by default) to every broker in `amqpUris`.
- A user that may declare exchanges and queues, bind them, publish and consume in the virtual host `/`. The driver
  does not select a virtual host: the URI path is ignored and the client default `/` is used.
- For `queueType: QUORUM` or `STREAM`, a RabbitMQ version with quorum or stream queues. The tested 4.1 image has both.

Nothing has to be created in advance. The driver declares exchanges, queues and bindings itself.

The regression suite starts the broker from
[`e2e-tests/compose/stacks/rabbitmq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/rabbitmq.yaml).
A comparable single broker for a local run, with the same image and the AMQP and management ports published:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.1-management
```

The official image lets the default `guest` user connect from other hosts, which the regression suite relies on. On
a broker installed another way, `guest` is normally restricted to localhost; create a dedicated user. The image is
multi-arch and runs natively on Apple Silicon.

## Minimal configuration and benchmark command

The shipped `driver-rabbitmq/rabbitmq.yaml`:

```yaml
name: RabbitMQ
driverClass: io.openmessaging.benchmark.driver.rabbitmq.RabbitMqBenchmarkDriver

producerCreationDelay: 100
producerCreationBatchSize: 5
consumerCreationDelay: 100
consumerCreationBatchSize: 5

amqpUris:
  - amqp://localhost
messagePersistence: false
queueType: QUORUM
```

Change the host in `amqpUris` to your broker. Put credentials in the URI, for example
`amqp://benchmark:secret@rabbit-1.example.com:5672`; without credentials the client uses `guest`/`guest`, and
without a port it uses the AMQP default port.

Run a local-mode benchmark from the repository root (after a full `mvn -DskipTests install`) or from `/benchmark`
in the container image; the relative paths are the same in both:

```bash
bin/benchmark --drivers driver-rabbitmq/rabbitmq.yaml workloads/1-topic-1-partition-1kb.yaml
```

Host names depend on where the benchmark runs:

- Source checkout or distribution on the machine running the `docker run` broker above: `amqp://localhost` works.
- Benchmark inside a container: `localhost` is the container itself; use a host name the container can resolve.
- Compose regression suite: the configs in
  [`e2e-tests/compose/drivers/`](https://github.com/mihkels/open-messaging-benchmark/tree/main/e2e-tests/compose/drivers)
  use the service name, `amqp://guest:guest@rabbitmq:5672`.

For remote workers add `--workers` or `--workers-file`; see [Distributed runs](../getting-started/distributed-runs.md).
The plugin bundle must be installed on every worker, see [Runtime and plugins](../reference/runtime-and-plugins.md).
[Using your broker](../getting-started/using-your-broker.md) covers adapting a shipped configuration in general.

## Configuration reference

All keys are read by `RabbitMqConfig`. Unknown keys are ignored.

|             Key             |      Type       |  Default   |                                                                Description                                                                 |
|-----------------------------|-----------------|------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `amqpUris`                  | list of strings | empty list | Required. AMQP URIs `amqp://[user:password@]host[:port]`. Only host, port, user and password are used; path, query and scheme are ignored. |
| `queueType`                 | enum            | `CLASSIC`  | `CLASSIC`, `QUORUM` or `STREAM`, upper case. Sets the `x-queue-type` queue argument (none for `CLASSIC`).                                  |
| `messagePersistence`        | boolean         | `false`    | When `true`, messages are published with delivery mode 2 (persistent).                                                                     |
| `producerCreationBatchSize` | int             | `5`        | Producers created per batch.                                                                                                               |
| `producerCreationDelay`     | long            | `100`      | Delay between producer creation batches, in milliseconds.                                                                                  |
| `consumerCreationBatchSize` | int             | `5`        | Consumers created per batch.                                                                                                               |
| `consumerCreationDelay`     | long            | `100`      | Delay between consumer creation batches, in milliseconds.                                                                                  |

There are no pass-through client properties. Other connection settings (heartbeat, TLS, virtual host, frame size)
are defaults of the [RabbitMQ Java client](https://www.rabbitmq.com/client-libraries/java-api-guide). Automatic
connection recovery is enabled.

## Semantics

**Topics.** The topic name prefix is a broker URI with an `exchange` query parameter, for example
`amqp://localhost?exchange=test-exchange`, to which the benchmark appends an index and a random suffix. The driver
uses the parameter value (`test-exchange-0000000-<random>`) as the exchange name and the part before `?` as the
broker. With several `amqpUris`, topics are assigned to brokers round robin. One connection is opened per primary
broker; the remaining URIs are passed as fallback addresses for reconnection.

**Partitions.** `createTopic` rejects any `partitionsPerTopic` other than 1 with
`Cannot create topic with partitions in RabbitMQ`. Use workloads with `partitionsPerTopic: 1`.

**Producers.** Each producer opens a channel, declares the topic's exchange as a durable `fanout` exchange and
enables publisher confirms. A send completes when the broker confirms the message and fails when the broker
negatively acknowledges it. The message key is passed as the routing key, but fanout exchanges ignore routing keys,
so keys have no effect on delivery or ordering.

**Subscriptions and consumers.** Each consumer opens a channel, declares the exchange and a durable, non-exclusive,
non-auto-delete queue named `<exchange>-<subscription>`, and binds the queue to the exchange. Consumers of the same
subscription share that queue as competing consumers. Each subscription has its own queue and receives every
message, so multiple subscriptions per topic work.

**Queue types and acknowledgements.**

| `queueType` |     Queue argument     |                       Consumer acknowledgement                        |
|-------------|------------------------|-----------------------------------------------------------------------|
| `CLASSIC`   | none                   | Automatic acknowledgement, no prefetch limit set                      |
| `QUORUM`    | `x-queue-type: quorum` | Automatic acknowledgement, no prefetch limit set                      |
| `STREAM`    | `x-queue-type: stream` | Manual acknowledgement after the callback, prefetch (`basicQos`) 1000 |

Stream queues do not support automatic acknowledgements and require a prefetch limit, so for `STREAM` the driver
always acknowledges manually with a fixed prefetch of 1000. The prefetch is not configurable. Stream consumers do
not set a stream offset, so they read messages published after they subscribed.

**Durability.** Exchanges and queues are declared durable. `messagePersistence` only sets the per-message delivery
mode; quorum and stream queues store messages durably regardless of it.

**End-to-end latency.** The producer sets the AMQP `timestamp` property to the current time and the consumer uses it
as the publish time. The AMQP timestamp property is encoded in whole seconds, so end-to-end latency from this driver
has one-second resolution. Publish latency (until the confirm) is measured by the framework and is not affected. See
[Latency and backlog](../methodology/latency-and-backlog.md).

**Cleanup.** Exchanges and queues are not deleted after a run. Names contain random suffixes, so every run adds
new durable exchanges and queues. Remove them between runs, for example with the management UI or `rabbitmqctl`.

**Workload features.** Single-partition topics, multiple topics, multiple subscriptions per topic and multiple
consumers per subscription work. Keys are accepted but ignored. For backlog workloads (`consumerBacklogSizeGB`) the
framework pauses consumption by blocking the consumer callback; with `CLASSIC` and `QUORUM` there is no prefetch
limit, so the broker keeps pushing messages to the client while the callback is blocked. See
[Workloads](../guides/workloads.md).

## Tuning examples

Classic queues with persistent messages, as in the `rabbitmq-classic-fanout` regression config:

```yaml
queueType: CLASSIC
messagePersistence: true
```

Persistent messages make classic queues write messages to disk, which adds disk I/O compared with transient messages.

Stream queues:

```yaml
queueType: STREAM
```

Streams keep messages after consumption and use manual acknowledgements with a prefetch of 1000, while classic and
quorum queues use automatic acknowledgements. Keep this difference in mind when comparing queue types; see
[Fair comparisons](../methodology/fair-comparisons.md).

Spreading topics over several brokers:

```yaml
amqpUris:
  - amqp://benchmark:secret@rabbit-1:5672
  - amqp://benchmark:secret@rabbit-2:5672
  - amqp://benchmark:secret@rabbit-3:5672
```

Producers and consumers of a topic connect to one primary broker chosen round robin. In a cluster, the queue
leader may be on a different node from the one the client is connected to.

With many producers or consumers, `producerCreationBatchSize`, `consumerCreationBatchSize` and the two delay keys
trade setup time against the rate at which connections and channels are opened on the broker.

## Troubleshooting and verification

Run the RabbitMQ regression cases (Docker required, see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks rabbitmq
e2e-tests/compose/run-e2e.sh --cases rabbitmq-contract,rabbitmq-classic-fanout,rabbitmq-stream
```

A passing case is a functional check at a low rate, not a performance result.

Common problems:

- `Cannot create topic with partitions in RabbitMQ`: the workload sets `partitionsPerTopic` greater than 1.
- The run stays in producer or consumer creation: failed creations (for example an unreachable host or rejected
  credentials) are retried until they succeed, and the failure is logged only at debug level. Check broker
  reachability and the credentials in `amqpUris`.
- `ACCESS_REFUSED` or login failures: on a default broker installation `guest` may only connect from localhost; use
  a dedicated user in the URI.
- `PRECONDITION_FAILED` when declaring a queue: a queue with the same name already exists with different arguments.
- Configuration error for `queueType`: values are case-sensitive and must be upper case, for example `QUORUM`.
- End-to-end latency values are multiples of about one second: expected, see the semantics above.

General advice is in [Troubleshooting](../guides/troubleshooting.md); result interpretation in
[Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-rabbitmq/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-rabbitmq/src/main/java/io/openmessaging/benchmark/driver/rabbitmq)
- Shipped config: [`driver-rabbitmq/rabbitmq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-rabbitmq/rabbitmq.yaml)
- Regression configs: [`rabbitmq-quorum.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/rabbitmq-quorum.yaml), [`rabbitmq-classic.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/rabbitmq-classic.yaml), [`rabbitmq-stream.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/rabbitmq-stream.yaml)
- Stack file: [`e2e-tests/compose/stacks/rabbitmq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/rabbitmq.yaml)
- Legacy Terraform/Ansible files: [`driver-rabbitmq/deploy`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-rabbitmq/deploy). They are unmaintained, reference older distribution names, Java versions and launcher settings, and must be adapted before use.

