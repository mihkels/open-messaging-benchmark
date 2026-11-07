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

# Results schema

Each workload and driver combination writes one JSON file. See [Results](../guides/results.md) for
how to read and chart it.

New files use schema version 2. Existing metric fields remain unchanged, and readers can continue to
read version 1 files, which have no `schemaVersion`, `metadata`, or `summary` fields. The
[JSON Schema](results-schema.json) describes the version 2 document.

## Version and run metadata

|          Field           |  Type   |                                                   Description                                                    |
|--------------------------|---------|------------------------------------------------------------------------------------------------------------------|
| `schemaVersion`          | integer | Result schema version; currently `2`                                                                             |
| `metadata.runId`         | string  | UUID version 7 created by the coordinator and propagated to workers                                              |
| `metadata.startedAtUtc`  | string  | Run start time in UTC, ISO 8601 format                                                                           |
| `metadata.finishedAtUtc` | string  | Run finish time in UTC, ISO 8601 format                                                                          |
| `metadata.ombVersion`    | string  | OMB version, or `unknown` when unavailable                                                                       |
| `metadata.gitCommit`     | string  | Commit from `omb.gitCommit`, `GIT_COMMIT`, `GITHUB_SHA`, or the current Git checkout; `unknown` when unavailable |
| `metadata.jdk`           | string  | Java version                                                                                                     |
| `metadata.jvmOptions`    | string  | JVM input options; credential-like `-D` properties are redacted                                                  |
| `metadata.workerCount`   | integer | Number of workers used                                                                                           |
| `metadata.workload`      | object  | Resolved workload configuration                                                                                  |
| `metadata.driverConfig`  | object  | Driver YAML parsed as JSON; password-, token- and secret-like keys are replaced with `[REDACTED]`                |

Set the `omb.gitCommit` system property, `GIT_COMMIT` environment variable or `GITHUB_SHA`
environment variable to preserve the source commit in result files.

## Summary

The `summary` object contains headline values for the measured window, after warm-up.

|        Field        |  Type   |                             Description                             |
|---------------------|---------|---------------------------------------------------------------------|
| `publishRateAvg`    | number  | Mean of the measured publish-rate samples, messages per second      |
| `consumeRateAvg`    | number  | Mean of the measured consume-rate samples, messages per second      |
| `throughputMbSec`   | number  | Mean consume rate multiplied by message size, decimal MB per second |
| `totalMessagesSent` | integer | Successfully sent messages in the measured window                   |
| `totalErrors`       | integer | Failed sends in the measured window                                 |
| `maxBacklog`        | integer | Maximum sampled backlog during the measured window                  |
| `targetRate`        | number  | Configured or discovered target publish rate, messages per second   |

## Threshold evaluation

The optional `thresholds` object is present when the workload defines threshold checks. It contains
the overall `status` (`PASS` or `FAIL`) and one entry per configured check with its `limit`,
`observed` value and individual `status`. The latency check uses aggregated publish p99 in
milliseconds; the throughput ratio is measured average publish rate divided by target rate.

## Optional histograms

`histograms` is omitted unless the coordinator is run with `--include-histograms`. When present, it
contains base64-encoded compressed HdrHistograms for `publish`, `publishDelay`, and `endToEnd`.
Histograms are aggregated over the measured window; publish and end-to-end values use microseconds
internally, while the existing result fields retain their documented units.

## Units

|            Measurement             |        Unit         |                             Precision                              |
|------------------------------------|---------------------|--------------------------------------------------------------------|
| Publish latency                    | milliseconds        | microseconds                                                       |
| Schedule-corrected publish latency | milliseconds        | microseconds                                                       |
| Publish delay                      | **microseconds**    | microseconds                                                       |
| End-to-end latency                 | milliseconds        | Millisecond clock differences; some drivers use coarser timestamps |
| Rates                              | messages per second |                                                                    |
| Backlog                            | messages            |                                                                    |

## Run description

|        Field        |  Type  |                                            Description                                            |
|---------------------|--------|---------------------------------------------------------------------------------------------------|
| `workload`          | string | The workload's `name` field, not its file name                                                    |
| `driver`            | string | The driver configuration's `name` field                                                           |
| `messageSize`       | long   | `messageSize`, or for a `messageSizeDistribution` the integer weighted average of range midpoints |
| `topics`            | int    | `topics`                                                                                          |
| `partitions`        | int    | `partitionsPerTopic`                                                                              |
| `producersPerTopic` | int    | `producersPerTopic`                                                                               |
| `consumersPerTopic` | int    | `consumerPerSubscription`. It is **not** multiplied by `subscriptionsPerTopic`                    |

## Period series

These lists have one entry per 10-second sampling period of the measured phase. Warm-up is not
included. For backlog workloads the measured phase includes the build-up and drain; see
[Latency and backlog](../methodology/latency-and-backlog.md).

|                                  Field                                   |     Type     |                                                                      Description                                                                      |
|--------------------------------------------------------------------------|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `publishRate`                                                            | number list  | Messages published per second                                                                                                                         |
| `publishErrorRate`                                                       | number list  | Failed publishes per second                                                                                                                           |
| `consumeRate`                                                            | number list  | Messages received per second, across all subscriptions                                                                                                |
| `backlog`                                                                | int list     | `subscriptionsPerTopic × total successfully published − total received` since load started (warm-up included), at the end of the period, floored at 0 |
| `publishLatency{Avg,50pct,75pct,95pct,99pct,999pct,9999pct,Max}`         | number lists | Publish latency in ms                                                                                                                                 |
| `publishDelayLatency{Avg,50pct,...,Max}`                                 | lists        | Publish delay in µs; percentiles are integers                                                                                                         |
| `publishScheduleLatency{Avg,50pct,75pct,95pct,99pct,999pct,9999pct,Max}` | number lists | Intended send to successful completion in ms (schedule lag + service time, recorded per message)                                                      |
| `endToEndLatency{Avg,50pct,...,Max}`                                     | number lists | End-to-end latency in ms                                                                                                                              |

## Aggregates

Computed over the whole measured phase, not averaged from the periods.

|                                       Field                                        |  Type  | Description |
|------------------------------------------------------------------------------------|--------|-------------|
| `aggregatedPublishLatency{Avg,50pct,75pct,95pct,99pct,999pct,9999pct,Max}`         | number | ms          |
| `aggregatedPublishDelayLatency{Avg,50pct,...,Max}`                                 | number | µs          |
| `aggregatedPublishScheduleLatency{Avg,50pct,75pct,95pct,99pct,999pct,9999pct,Max}` | number | ms          |
| `aggregatedEndToEndLatency{Avg,50pct,...,Max}`                                     | number | ms          |

## Quantile maps

|                    Field                    | Values |
|---------------------------------------------|--------|
| `aggregatedPublishLatencyQuantiles`         | ms     |
| `aggregatedPublishDelayLatencyQuantiles`    | µs     |
| `aggregatedPublishScheduleLatencyQuantiles` | ms     |
| `aggregatedEndToEndLatencyQuantiles`        | ms     |

Keys are percentiles from 0 to 100, written as strings, for example `"99.90234375"`. Values are the
latency at that percentile. The keys are the HdrHistogram iteration points, so they are not round
numbers and differ between results. Use them to draw full latency distributions; interpolate when
comparing two results.

## Excerpt

```json
{
  "workload": "quick",
  "driver": "Kafka",
  "messageSize": 1024,
  "topics": 1,
  "partitions": 10,
  "producersPerTopic": 1,
  "consumersPerTopic": 1,
  "publishRate": [493.9, 505.6, 500.0],
  "consumeRate": [494.0, 505.4, 500.2],
  "backlog": [0, 2, 0],
  "publishLatency99pct": [6.952, 3.971, 5.751],
  "publishDelayLatency99pct": [3050, 2073, 6428],
  "endToEndLatency99pct": [10.0, 5.0, 6.0],
  "aggregatedPublishLatency99pct": 5.486,
  "aggregatedPublishDelayLatency99pct": 4009,
  "aggregatedEndToEndLatency99pct": 6.0,
  "aggregatedPublishLatencyQuantiles": { "50.0": 1.679, "99.0": 5.486 }
}
```

The excerpt is shortened. Real files contain every field above.

The `publishScheduleLatency*` and `aggregatedPublishScheduleLatency*` fields are additive;
older result files do not contain them. Existing fields retain their names, units and meanings.
See [Latency and backlog](../methodology/latency-and-backlog.md) for the distinction between service
time, schedule lag and schedule-corrected latency.
