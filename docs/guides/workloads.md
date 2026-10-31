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

# Workloads

A workload is a YAML file that describes the traffic pattern of a run. It is independent of the
driver, so the same file can be run against every supported system. This page explains how to build
one; the [Workload schema](../reference/workload-schema.md) lists every field and default.

## Anatomy of a workload

```yaml
name: 1 producer / 1 consumer on 1 topic / 16 partitions

# Topology
topics: 1
partitionsPerTopic: 16
subscriptionsPerTopic: 1
consumerPerSubscription: 1
producersPerTopic: 1

# Messages
messageSize: 1024
payloadFile: payload/payload-1Kb.data
keyDistributor: NO_KEY

# Load and duration
producerRate: 50000
consumerBacklogSizeGB: 0
warmupDurationMinutes: 1
testDurationMinutes: 15
```

`name` is stored in the result's `workload` field, and charts group results by it. The result
**file** is named after the workload file name without `.yaml`. Give both descriptive values, and
keep `name` unique across the workloads you compare.

## Topology

- `topics` and `partitionsPerTopic` decide how many topics are created and how each is partitioned.
  Drivers without partitions map partitions to their own concept or ignore them; see each driver
  page.
- `producersPerTopic` producers are created for every topic. The total number of producers is
  `topics × producersPerTopic`.
- `subscriptionsPerTopic` independent subscriptions (consumer groups) are created per topic, each
  with `consumerPerSubscription` consumers that share its messages. Every subscription receives
  every message, so the expected consume rate is the publish rate times `subscriptionsPerTopic`.

Large topic counts stress metadata handling and connection counts as much as the data path. The
bundled `*k-topic-*` workloads go up to one million topics.

## Choosing the rate

`producerRate` is the total target rate in messages per second across all producers and topics. It
is divided evenly among producer workers, and each producer sends at a uniform rate.

There are three common ways to use it:

|           Goal            |                                       Setting                                       |
|---------------------------|-------------------------------------------------------------------------------------|
| Latency at a known load   | A fixed rate below the system's capacity, for example `producerRate: 50000`         |
| Saturation behavior       | A fixed rate well above capacity; the bundled `max-rate-*` workloads use `10000000` |
| Find the sustainable rate | `producerRate: 0`                                                                   |

With `producerRate: 0`, producers start at 10,000 msg/s and the coordinator adjusts the rate every
3 seconds. While producers keep up and consumers keep pace, the rate grows by a ramping factor that
doubles after each healthy step. When either falls behind, the rate drops to what was actually
achieved and the ramping factor halves, so the run converges on the highest rate the system
sustains without queueing. The thresholds come from environment variables on the coordinator:

|        Variable         | Default |                             Meaning                             |
|-------------------------|---------|-----------------------------------------------------------------|
| `PUBLISH_BACKLOG_LIMIT` | 1000    | Messages the producers may fall short of the target in one step |
| `RECEIVE_BACKLOG_LIMIT` | 1000    | Messages published but not yet received                         |
| `MIN_RAMPING_FACTOR`    | 0.01    | Smallest relative rate increase per step                        |
| `MAX_RAMPING_FACTOR`    | 1       | Largest relative rate increase per step                         |

Rate discovery cannot be combined with `consumerBacklogSizeGB`.

> **Note:** At a fixed rate above capacity, latency reflects queueing in the client and broker, not
> the system's latency at a usable load. Report such runs as throughput tests.

## Duration and warm-up

- `warmupDurationMinutes` (default 1) runs the full load and then discards its statistics. It lets
  JIT compilation, connection pools, caches and partition leadership settle.
- `testDurationMinutes` is the measured period. It must be positive for the run to end
  automatically; the default of `0` runs indefinitely. Statistics are sampled every 10 seconds, so
  a 5-minute run has about 30 samples per series.

Use longer runs for tail percentiles and for systems with periodic background work such as log
compaction, segment rolling or garbage collection. See
[Benchmark design](../methodology/benchmark-design.md).

## Backlog workloads

`consumerBacklogSizeGB` measures how a system behaves when consumers fall behind and then catch up:

```yaml
producerRate: 100000
consumerBacklogSizeGB: 100
backlogDrainRatio: 1.0
testDurationMinutes: 5
```

Consumers are paused until the requested amount of data is queued, then resumed while producers keep
sending. The backlog operation runs in the measured phase, so the result covers the build-up, the
drain and then `testDurationMinutes` after the backlog has drained to the level set by
`backlogDrainRatio`. The run lasts longer than `testDurationMinutes`. See
[Latency and backlog](../methodology/latency-and-backlog.md).

## Bundled workloads

The `workloads/` directory has about 130 files. Their names encode the shape:

|              Pattern              |                   Example                    |                          Shape                          |
|-----------------------------------|----------------------------------------------|---------------------------------------------------------|
| `<n>-topic-<p>-partitions-<size>` | `1-topic-16-partitions-1kb.yaml`             | Fixed rate on a few topics                              |
| `...-4p-4c-<rate>`                | `1-topic-100-partitions-1kb-4p-4c-500k.yaml` | 4 producers and 4 consumers per topic at the given rate |
| `<count>-topic-1kb-4p-4c-<rate>`  | `10k-topic-1kb-4p-4c-500k.yaml`              | Many topics                                             |
| `max-rate-*`                      | `max-rate-1-topic-16-partitions-1kb.yaml`    | Offered load far above capacity                         |
| `backlog-*`                       | `backlog-1-topic-16-partitions-1kb.yaml`     | Backlog build-up and drain                              |
| `1m-10-topics-*`                  | `1m-10-topics-16-partitions-100b.yaml`       | 10 topics with small messages                           |

Most run for 5 minutes, and some for 15 minutes or more; many use high rates. Read the file before
running it, and copy it under a new name before changing it.

## Running several workloads

Pass several files to run them one after another with the same drivers:

```bash
bin/benchmark --drivers driver-kafka/kafka-latency.yaml --workers-file workers.yaml \
  workloads/1-topic-16-partitions-1kb.yaml workloads/1-topic-100-partitions-1kb.yaml
```

For a parameter sweep, generate the files from a template; see
[Workload generation](workload-generation.md). For message contents and keys, see
[Payloads and keys](payloads-and-keys.md).
