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

# Understanding your first run

This page walks through what the coordinator does during a run and what each number in the console
and the JSON result means. The examples come from a one-minute run of the Quickstart setup.

## Phases of a run

For each workload file and each driver configuration, the coordinator:

1. **Initializes the driver** on every worker, or in a local child JVM in local mode.
2. **Creates topics**, then **consumers**, then **producers**. Subscriptions get names like
   `sub-000-<random>`.
3. **Waits for consumers to be ready.** Producers send a probe message to every topic and the
   coordinator polls every 2 seconds until each subscription has received one, for up to 60
   seconds.
4. **Warms up** for `warmupDurationMinutes` (default 1). Statistics are printed but then reset.
5. **Measures** for `testDurationMinutes`, sampling statistics every 10 seconds.
6. **Writes the result** and stops all producers and consumers.

With `consumerBacklogSizeGB` set, the measured phase also includes a backlog build-up and drain; see
[Latency and backlog](../methodology/latency-and-backlog.md). With `producerRate: 0`, the
coordinator searches for the maximum sustainable rate; see [Workloads](../guides/workloads.md).

## Reading the console line

```text
Pub rate   500.0 msg/s /  0.5 MB/s | Pub err     0.0 err/s | Cons rate   500.2 msg/s /  0.5 MB/s | Backlog:  0.0 K | Pub Latency (ms) avg:  1.8 - 50%:  1.7 - 99%:  5.8 - 99.9%: 16.5 - Max: 24.7 | Pub Delay Latency (us) avg: 584.6 - 50%: 452.0 - 99%: 6428.0 - 99.9%: 16562.0 - Max: 22517.0
```

|         Field          |                                                   Meaning                                                    |
|------------------------|--------------------------------------------------------------------------------------------------------------|
| Pub rate               | Messages and megabytes acknowledged by the broker per second, across all producers, over the last 10 seconds |
| Pub err                | Failed sends per second                                                                                      |
| Cons rate              | Messages and megabytes received per second across all consumers                                              |
| Backlog                | Messages sent but not yet received, in thousands, counted per subscription                                   |
| Pub Latency (ms)       | Time from the send call to the broker acknowledgement                                                        |
| Pub Delay Latency (us) | How late each send started compared with its scheduled time                                                  |

The consume rate is the publish rate multiplied by `subscriptionsPerTopic` when consumers keep up.

## The three latencies

**Publish latency** is measured by the producer from the moment it hands a message to the client
library until the send is acknowledged. It reflects the client, the network and the broker's write
path, including any batching (`linger.ms` in Kafka, for example).

**Publish delay latency** measures the load generator. Messages are scheduled at a uniform rate; if
a producer cannot send on time because the client blocks or the worker is overloaded, the delay
grows. A rising publish delay means the requested rate is not being offered, and the other numbers
no longer describe the requested workload. It is reported in microseconds.

**End-to-end latency** is the time between the producer timestamp and the consumer receiving the
message. It is computed from wall-clock milliseconds, so it has millisecond resolution, and when
producers and consumers run on different hosts their clocks must be synchronized. See
[Latency and backlog](../methodology/latency-and-backlog.md).

## The result file

At the end of the run the coordinator prints aggregated values for the measured period and writes a
JSON file. Without `-o`, the name is `<workload file name>-<driver name>-<timestamp>.json` in the
current directory.

```json
{
  "workload": "quick",
  "driver": "Kafka",
  "messageSize": 1024,
  "topics": 1,
  "partitions": 10,
  "producersPerTopic": 1,
  "consumersPerTopic": 1,
  "publishRate": [493.9, 505.59, 500.03],
  "consumeRate": [494.0, 505.39, 500.23],
  "backlog": [0, 2, 0],
  "publishErrorRate": [0.0, 0.0, 0.0],
  "publishLatency99pct": [6.952, 3.971, 5.751],
  "aggregatedPublishLatency99pct": 5.486,
  "aggregatedPublishDelayLatency99pct": 4009,
  "aggregatedEndToEndLatency99pct": 6.0
}
```

The full file has more fields: per-sample arrays hold one value per 10-second sample, and
`aggregated*` values cover the whole measured period. The
[Results schema](../reference/results-schema.md) lists every field and unit.

## Is the run valid?

Check these before using any number:

- **The offered rate was achieved.** `publishRate` is close to `producerRate` and the publish delay
  stays low. If not, the system or the load generator was saturated.
- **Consumers kept up.** `consumeRate` matches the publish rate times subscriptions and `backlog`
  does not grow, unless the workload builds a backlog on purpose.
- **No errors.** `publishErrorRate` is zero.
- **The run is long enough.** A short run has too few samples for stable tail percentiles. The
  Quickstart workload is a smoke test, not a measurement.
- **The coordinator did not log** `Failed to run the workload`. When one workload fails, the
  coordinator logs the error and continues with the next workload or driver, so a multi-workload run
  can still exit normally.

## Where logs go

The coordinator, workers and driver child JVMs log to the console and to rolling files in their
current working directory: `benchmark-worker.log` for the coordinator and workers and
`benchmark-driver.log` for driver processes. See [Monitoring](../guides/monitoring.md) for metrics.
