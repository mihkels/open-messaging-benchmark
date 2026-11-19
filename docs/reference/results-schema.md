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

New files use schema version 3, which adds [`validity`](#validity) to version 2. Existing metric
fields remain unchanged, and readers can continue to read version 2 files, which have no `validity`,
and version 1 files, which also have no `schemaVersion`, `metadata`, or `summary` fields. The
[JSON Schema](results-schema.json) describes the version 3 document.

## Version and run metadata

|          Field           |  Type   |                                                                   Description                                                                    |
|--------------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `schemaVersion`          | integer | Result schema version; currently `3`                                                                                                             |
| `metadata.runId`         | string  | UUID version 7 created by the coordinator and propagated to workers                                                                              |
| `metadata.startedAtUtc`  | string  | Run start time in UTC, ISO 8601 format                                                                                                           |
| `metadata.finishedAtUtc` | string  | Run finish time in UTC, ISO 8601 format                                                                                                          |
| `metadata.ombVersion`    | string  | OMB version, or `unknown` when unavailable                                                                                                       |
| `metadata.gitCommit`     | string  | Commit from `omb.gitCommit`, `GIT_COMMIT`, `GITHUB_SHA`, or the current Git checkout; `unknown` when unavailable                                 |
| `metadata.jdk`           | string  | Java version                                                                                                                                     |
| `metadata.jvmOptions`    | string  | JVM input options; credential-like `-D` properties are redacted                                                                                  |
| `metadata.workerCount`   | integer | Number of workers used                                                                                                                           |
| `metadata.workload`      | object  | Resolved workload configuration                                                                                                                  |
| `metadata.driverConfig`  | object  | Driver YAML parsed as JSON; password-, token- and secret-like keys are replaced with `[REDACTED]`                                                |
| `metadata.clockOffsets`  | array   | Optional. Per-worker `worker`, `role`, `offsetMs`, `rttMs` and `driftMs`; see [Clock offset](../methodology/latency-and-backlog.md#clock-offset) |

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
| `targetRate`        | number  | Configured or discovered target rate, msg/s; staged: measured mean  |

## Threshold evaluation

The optional `thresholds` object is present when the workload defines threshold checks. It contains
the overall `status` (`PASS` or `FAIL`) and one entry per configured check with its `limit`,
`observed` value and individual `status`. The latency check uses aggregated publish p99 in
milliseconds; the throughput ratio is measured average publish rate divided by target rate.

## Findings

`findings` lists what the built-in [run checks](../guides/results.md#run-checks) found, each with
`check`, `severity` (`info`, `warning` or `invalid`), `message` and, for numeric checks, `value` and
`limit`. It is omitted when nothing was found and in files written before the checks existed.

## Validity

`validity` is the run's verdict: `status` is `valid`, `warning` or `invalid`, and `findings` lists
the findings it is based on, in the same form as `findings`. Any `invalid` finding makes the run
`invalid`; otherwise any `warning` finding makes it `warning`. See
[Validity verdict](../guides/results.md#validity-verdict). Files older than version 3 have no
`validity`.

## Retries

`attempts` is how many attempts the run took with [`--retries`](cli.md); the last one produced the
file. `failedAttempts` lists why each earlier attempt failed, oldest first, and is omitted when the
first attempt succeeded. Files written before retries existed have neither field. A retried run gets a
`retried` warning finding: its numbers are valid, but the system under test or a worker failed at
least once.

## Delivery

`delivery` is present when the workload sets `verifyDelivery: true`. The counts cover the whole
run, warm-up included; see [Delivery verification](../guides/results.md#delivery-verification).

|    Field     |  Type   |                               Description                               |
|--------------|---------|-------------------------------------------------------------------------|
| `sent`       | integer | Acknowledged messages that carried a delivery stamp                     |
| `received`   | integer | Stamped messages received for the first time, summed over subscriptions |
| `lost`       | integer | `sent` × subscriptions per topic − `received`, at least 0               |
| `duplicated` | integer | Stamped messages received again on the same subscription                |
| `outOfOrder` | integer | Stamped messages received after a later message from the same producer  |

## Consumer pause

`consumerPause` is present when the workload has a [`consumerPause`](../guides/workloads.md#consumer-pause)
and the run reached it.

|     Field      |      Type      |                                      Description                                       |
|----------------|----------------|----------------------------------------------------------------------------------------|
| `startPeriod`  | integer        | Index in the period series of the first period recorded while consumers were paused    |
| `endPeriod`    | integer        | Index of the last period recorded while consumers were paused                          |
| `peakBacklog`  | integer        | Backlog in messages when consumers resumed                                             |
| `drainSeconds` | number or null | Seconds from resume until the backlog drained; `null` if it had not drained by the end |

## Rate probe

`rateProbe` is present for `producerRate: 0` runs, which search for the highest sustainable rate.
It has one entry per probe step, every few seconds, warm-up included.

|           Field            |      Type       |                                             Description                                             |
|----------------------------|-----------------|-----------------------------------------------------------------------------------------------------|
| `seconds`                  | number          | Seconds since the probe started                                                                     |
| `rate`                     | number          | Publish rate in msg/s before the step                                                               |
| `nextRate`                 | number          | Publish rate in msg/s the step chose                                                                |
| `receiveBacklog`           | integer         | Messages published but not yet received, one per subscription; 0 without subscriptions              |
| `publishBacklog`           | integer         | Messages the producers fell short of `rate` by in the step                                          |
| `publishDelayLatency99pct` | integer, absent | p99 publish delay in µs of the latest period; absent before the first period                        |
| `decision`                 | string          | `rampUp`, or the backlog over its limit that made it back off: `receiveBacklog` or `publishBacklog` |

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

|           Field           |  Type  |                                                                                                                      Description                                                                                                                       |
|---------------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `workload`                | string | The workload's `name` field, not its file name                                                                                                                                                                                                         |
| `driver`                  | string | The driver configuration's `name` field                                                                                                                                                                                                                |
| `messageSize`             | long   | `messageSize`, or for a `messageSizeDistribution` the integer weighted average of range midpoints                                                                                                                                                      |
| `topics`                  | int    | `topics`                                                                                                                                                                                                                                               |
| `partitions`              | int    | `partitionsPerTopic`                                                                                                                                                                                                                                   |
| `producersPerTopic`       | int    | `producersPerTopic`                                                                                                                                                                                                                                    |
| `consumersPerTopic`       | int    | `consumerPerSubscription`. It is **not** multiplied by `subscriptionsPerTopic`                                                                                                                                                                         |
| `payloadCompressionRatio` | number | zstd level 3 compressed size divided by original size of the generated payloads, weighted by how often each is sent; only present for workloads with a [`payloadGenerator`](../guides/payloads-and-keys.md#4-payloads-with-a-target-compression-ratio) |

## Period series

These lists have one entry per 10-second sampling period. Warm-up periods come first, tagged
`WARMUP` in `phase`; `summary` and the `aggregated*` values leave them out. For backlog workloads
the measured window includes the build-up and drain, tagged `BACKLOG_BUILD` and `BACKLOG_DRAIN`;
see [Latency and backlog](../methodology/latency-and-backlog.md). A mid-run consumer pause uses the
same two tags between `MEASURE` periods. Files written before `phase`
existed have no warm-up periods.

|                                  Field                                   |     Type     |                                                                      Description                                                                      |
|--------------------------------------------------------------------------|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `phase`                                                                  | string list  | Run phase of the period: `WARMUP`, `BACKLOG_BUILD`, `BACKLOG_DRAIN` or `MEASURE`                                                                      |
| `targetRate`                                                             | number list  | Target publish rate in msg/s at the end of the period; absent in older files                                                                          |
| `stage`                                                                  | int list     | Index of the running workload [stage](../guides/rate-profiles.md); only present for staged workloads                                                  |
| `workerHealth`                                                           | object list  | Highest worker `cpu` (share of its cores) and `gc` (share of the period in GC pauses), 0 to 1; absent when workers do not report it                   |
| `publishRate`                                                            | number list  | Messages published per second                                                                                                                         |
| `publishErrorRate`                                                       | number list  | Failed publishes per second                                                                                                                           |
| `consumeRate`                                                            | number list  | Messages received per second, across all subscriptions                                                                                                |
| `backlog`                                                                | int list     | `subscriptionsPerTopic × total successfully published − total received` since load started (warm-up included), at the end of the period, floored at 0 |
| `publishLatency{Avg,50pct,75pct,95pct,99pct,999pct,9999pct,Max}`         | number lists | Publish latency in ms                                                                                                                                 |
| `publishDelayLatency{Avg,50pct,...,Max}`                                 | lists        | Publish delay in µs; percentiles are integers                                                                                                         |
| `publishScheduleLatency{Avg,50pct,75pct,95pct,99pct,999pct,9999pct,Max}` | number lists | Intended send to successful completion in ms (schedule lag + service time, recorded per message)                                                      |
| `endToEndLatency{Avg,50pct,...,Max}`                                     | number lists | End-to-end latency in ms                                                                                                                              |

## Partition telemetry

Present only for workloads with `partitionTelemetry: true`; see
[ADR 0001](../development/adr/0001-partition-telemetry.md) for the design and its memory cost.

|          Field           |    Type     |                                                              Description                                                               |
|--------------------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `partitionSkew`          | object list | One entry per period in which partitions reported traffic; `period` is the index into the per-period lists                             |
| `rebalanceEvents`        | object list | Consumer assignment changes: `period`, `timestampMillis`, `worker`, `topic`, `kind` (`ASSIGNED`, `REVOKED` or `LOST`) and `partitions` |
| `droppedRebalanceEvents` | int         | Events a worker dropped because more than 1,000 arrived in one period; absent when zero                                                |

Each `partitionSkew` entry has:

|                    Field                     |    Type     |                                                       Description                                                        |
|----------------------------------------------|-------------|--------------------------------------------------------------------------------------------------------------------------|
| `partitions`                                 | int         | Partitions that published or consumed during the period                                                                  |
| `publishMaxMeanRatio`, `consumeMaxMeanRatio` | number      | Busiest partition's message count divided by the mean; 1 is even                                                         |
| `publishGini`, `consumeGini`                 | number      | Gini coefficient of per-partition message counts: 0 is even, close to 1 is one partition taking everything               |
| `topPublishP99`, `topEndToEndP99`            | object list | Up to five `{topic, partition, p99Ms}` with the highest p99; per-partition histograms keep one significant digit (≈10 %) |

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
  "phase": ["MEASURE", "MEASURE", "MEASURE"],
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
