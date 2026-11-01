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

# Using your broker

This page shows how to point the benchmark at a messaging system you already run. It assumes you
have a benchmark runtime from the [Quickstart](quickstart.md): the `openmessaging-benchmark:local`
image, or an unpacked distribution or source build (see
[Building the project](../development/building-the-project.md)). Commands are the same in all
three; they run from the benchmark directory (`/benchmark` in the image).

## 1. Choose the driver

Find your system in the [driver overview](../drivers/index.md) and open its page. Each page lists:

- the shipped configuration files, for example `driver-pulsar/pulsar.yaml`;
- what must exist on the broker side before a run, such as a namespace, an admin endpoint, a
  stream storage type or a provider jar;
- the configuration keys the driver reads and their defaults.

The `driverClass` in the configuration file selects the plugin. All plugins are already installed
in the image and the distribution under `drivers/`.

## 2. Write a driver configuration

Copy the shipped file and change the endpoints. For example, for Kafka:

```bash
cp driver-kafka/kafka-latency.yaml my-kafka.yaml
```

```yaml
name: Kafka
driverClass: io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver

replicationFactor: 3

topicConfig: |
  min.insync.replicas=2

commonConfig: |
  bootstrap.servers=broker-1:9092,broker-2:9092,broker-3:9092

producerConfig: |
  acks=all
  linger.ms=1
  batch.size=131072

consumerConfig: |
  auto.offset.reset=earliest
  enable.auto.commit=false
```

Things to check for every driver:

- **Reachability.** The addresses must resolve from the process that runs the driver: the
  coordinator in local mode, or every worker in distributed mode. `localhost` inside a container is
  the container itself.
- **Permissions.** Most drivers create and sometimes delete topics, queues, streams or namespaces.
  The client identity needs those rights, or the resources must exist in advance as described on the
  driver page.
- **Guarantees.** Settings such as acknowledgements, replication and persistence change both
  results and durability. Record them; see [Fair comparisons](../methodology/fair-comparisons.md).
- **Provider jars.** The JMS driver needs a JMS provider jar in `drivers/driver-jms/ext/`. See
  [JMS](../drivers/jms.md).

Security settings (TLS, SASL, tokens) are passed through the client properties a driver exposes.
Not every driver exposes them; check its configuration reference.

## 3. Pick a small workload

Start with a short, low-rate run to prove connectivity before measuring anything:

```yaml
name: smoke
topics: 1
partitionsPerTopic: 1
messageSize: 1024
payloadFile: payload/payload-1Kb.data
subscriptionsPerTopic: 1
producersPerTopic: 1
consumerPerSubscription: 1
producerRate: 100
testDurationMinutes: 1
warmupDurationMinutes: 0
```

`payloadFile` is resolved relative to the directory you run `bin/benchmark` from, and the file must
contain exactly `messageSize` bytes. See [Workloads](../guides/workloads.md) for every option.

## 4. Run it

Local mode runs producers and consumers on the coordinator host:

```bash
bin/benchmark --drivers my-kafka.yaml -o smoke.json smoke.yaml
```

With the Docker image and files on the host, mount them into a container on a network that can
reach the broker:

```bash
docker run --rm -it -v "$PWD:/work" -w /benchmark openmessaging-benchmark:local \
  bin/benchmark --drivers /work/my-kafka.yaml -o /work/smoke.json /work/smoke.yaml
```

Add `--network <name>` or `--network host` (Linux) when the broker is only reachable that way.
The driver child JVM uses a 4 GiB heap by default; set `-e HEAP_OPTS="-Xms1G -Xmx1G"` on small
machines (see [Runtime and plugins](../reference/runtime-and-plugins.md)).

For more load than one host can generate, start workers and pass `--workers`; see
[Distributed runs](distributed-runs.md).

## 5. Check the output

The run succeeded when:

- the log contains `All consumers are ready` and, at the end, `Writing test result into ...`;
- the coordinator exits with status zero;
- publish errors are zero and the consume rate matches the publish rate times the number of
  subscriptions per topic.

If consumers never become ready, the coordinator stops after 60 seconds with
`Timed out waiting for consumers to be ready`. See [Troubleshooting](../guides/troubleshooting.md).

## 6. Clean up

Each run creates new topics with random names (for example `test-topic-<random>-0000`; the prefix is
driver-specific). Most drivers do not delete them. Remove them with your broker's tools, or use a
dedicated namespace, virtual host or scope for benchmarks. The driver pages describe what each
driver creates.
