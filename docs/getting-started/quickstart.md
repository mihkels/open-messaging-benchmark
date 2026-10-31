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

# Quickstart

This page gets you from nothing to a benchmark result against a single-node Kafka broker, using
only Docker. Kafka is used because the repository ships a ready-made Compose environment for it;
every other driver works the same way once its broker is reachable (see
[Using your broker](using-your-broker.md)).

You will:

1. build a benchmark image that contains the runtime and all driver plugins;
2. start Kafka, two benchmark workers and a coordinator container;
3. run a four-minute workload and read its result;
4. clean up.

## Requirements

- Docker with Compose v2 and about 4 GB of memory available to Docker.
- Git, to clone the repository.
- No JDK or Maven is required on the host.

> **Note:** The fork does not currently publish a Java 27 benchmark image. Older tags on Docker Hub
> predate the plugin runtime and do not work with the configurations in this repository. The first
> step therefore builds the image locally.

## 1. Build the benchmark image

```bash
git clone https://github.com/mihkels/open-messaging-benchmark.git
cd open-messaging-benchmark
docker build -t openmessaging-benchmark:local -f docker/Dockerfile.build .
```

`docker/Dockerfile.build` compiles the project inside a Maven container and produces a small Java 27
runtime image with the benchmark installed in `/benchmark`. The first build downloads all
dependencies and takes several minutes; later builds reuse the Maven cache.

> **Note:** Build from a regular clone. In a linked Git worktree, the license check inside the
> build cannot read the Git metadata and fails. See
> [Building the project](../development/building-the-project.md) for alternatives, including
> building the image from a distribution built on the host.

## 2. Start the environment

The [`local-test`](https://github.com/mihkels/open-messaging-benchmark/tree/main/local-test)
directory contains a Compose file with:

|            Service             |                            Role                             |
|--------------------------------|-------------------------------------------------------------|
| `kafka`                        | Apache Kafka 4.4.0 in KRaft mode, reachable as `kafka:9092` |
| `omb-worker-1`, `omb-worker-2` | Benchmark workers listening on port 8080                    |
| `omb-driver`                   | Idle container used to run the coordinator                  |

It mounts `local-test/drivers`, `local-test/workloads` and `local-test/payload` into the
coordinator container as `/drivers`, `/workloads` and `/payload`.

```bash
cd local-test
docker compose up -d
docker compose ps
```

All four containers should be running. Wait until Kafka and both workers are ready before starting
the benchmark:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
docker exec omb-worker-1 wget -qO /dev/null http://127.0.0.1:8080/metrics
docker exec omb-worker-2 wget -qO /dev/null http://127.0.0.1:8080/metrics
```

The topic list may be empty; all three commands should succeed. If one fails, wait and retry it.

## 3. Run a benchmark

```bash
docker exec -it omb-driver bash -lc \
  'bin/benchmark --drivers /drivers/kafka.yaml \
     --workers http://omb-worker-1:8080,http://omb-worker-2:8080 \
     -o first-result.json /workloads/simple.yaml'
```

The coordinator runs in `/benchmark` inside the container. The workload
[`simple.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/local-test/workloads/simple.yaml)
publishes 500 messages per second of 1 KiB to one topic with 10 partitions, with one producer and
one consumer, for a one-minute warm-up followed by three measured minutes.

While it runs, the coordinator prints a line every 10 seconds:

```text
Pub rate   500.1 msg/s /  0.5 MB/s | Pub err     0.0 err/s | Cons rate   500.1 msg/s /  0.5 MB/s | Backlog:  0.0 K | Pub Latency (ms) avg:  1.8 - 50%:  1.7 - 99%:  5.8 - 99.9%: 11.2 - Max: 15.5 | Pub Delay Latency (us) avg: 536.3 - 50%: 453.0 - 99%: 4203.0 - 99.9%: 9190.0 - Max: 13438.0
```

At the end it prints aggregated latency percentiles for the measured period and writes the result:

```text
----- Aggregated Pub Latency (ms) avg:  1.8 - 50%:  1.7 - 95%:  3.1 - 99%:  5.5 - ...
Writing test result into first-result.json
```

The numbers above are from a laptop and only illustrate the format.

## 4. Look at the result

Copy the JSON result to the host:

```bash
docker cp omb-driver:/benchmark/first-result.json .
```

The file contains one value per 10-second sample for rates, backlog and latency percentiles, and
aggregated percentiles for the whole measured period. A quick sanity check with `jq`:

```bash
jq '{publishRate, consumeRate, backlog, aggregatedPublishLatency99pct, aggregatedEndToEndLatency99pct}' first-result.json
```

For a healthy run, publish and consume rates are close to 500, the backlog stays near zero and
publish errors are zero. [Understanding your first run](understanding-your-first-run.md) explains
each value, and [Results](../guides/results.md) covers CSV conversion and charts.

## 5. Try variations

Run from the same container; every file below is already in the image.

```bash
# Local mode: no workers, the coordinator runs the driver itself
docker exec -it omb-driver bash -lc \
  'bin/benchmark --drivers /drivers/kafka.yaml -o local.json /workloads/simple.yaml'

# One of the bundled workloads (paths relative to /benchmark)
docker exec -it omb-driver bash -lc \
  'bin/benchmark --drivers /drivers/kafka.yaml \
     --workers http://omb-worker-1:8080,http://omb-worker-2:8080 \
     workloads/1-topic-16-partitions-1kb.yaml'
```

Bundled workloads under `workloads/` use longer durations and higher rates than `simple.yaml`.
Check `testDurationMinutes` and `producerRate` before running them on a laptop. To change what is
measured, see [Workloads](../guides/workloads.md).

## 6. Clean up

```bash
docker compose down -v
```

Run it from `local-test`. The command removes the containers, the network and Kafka's data.

## Next steps

- [Using your broker](using-your-broker.md): point a driver at your own broker, for any of the 15
  plugins.
- [Distributed runs](distributed-runs.md): run workers on separate machines.
- [Benchmark design](../methodology/benchmark-design.md): before drawing conclusions from numbers.
- To check a driver end to end without writing any configuration, the
  [Compose regression suite](../development/testing-a-driver.md) starts the matching broker for
  each plugin, for example `e2e-tests/compose/run-e2e.sh --stacks redis`.

