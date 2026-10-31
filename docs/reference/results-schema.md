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

## Units

|    Measurement     |        Unit         |                             Precision                              |
|--------------------|---------------------|--------------------------------------------------------------------|
| Publish latency    | milliseconds        | microseconds                                                       |
| Publish delay      | **microseconds**    | microseconds                                                       |
| End-to-end latency | milliseconds        | Millisecond clock differences; some drivers use coarser timestamps |
| Rates              | messages per second |                                                                    |
| Backlog            | messages            |                                                                    |

## Run description

|        Field        |  Type  |                                  Description                                   |
|---------------------|--------|--------------------------------------------------------------------------------|
| `workload`          | string | The workload's `name` field, not its file name                                 |
| `driver`            | string | The driver configuration's `name` field                                        |
| `messageSize`       | int    | `messageSize`, or the weighted average for a `messageSizeDistribution`         |
| `topics`            | int    | `topics`                                                                       |
| `partitions`        | int    | `partitionsPerTopic`                                                           |
| `producersPerTopic` | int    | `producersPerTopic`                                                            |
| `consumersPerTopic` | int    | `consumerPerSubscription`. It is **not** multiplied by `subscriptionsPerTopic` |

## Period series

These lists have one entry per 10-second sampling period of the measured phase. Warm-up is not
included. For backlog workloads the measured phase includes the build-up and drain; see
[Latency and backlog](../methodology/latency-and-backlog.md).

|                              Field                               |     Type     |                                            Description                                            |
|------------------------------------------------------------------|--------------|---------------------------------------------------------------------------------------------------|
| `publishRate`                                                    | number list  | Messages published per second                                                                     |
| `publishErrorRate`                                               | number list  | Failed publishes per second                                                                       |
| `consumeRate`                                                    | number list  | Messages received per second, across all subscriptions                                            |
| `backlog`                                                        | int list     | `subscriptionsPerTopic × total published − total received` at the end of the period, floored at 0 |
| `publishLatency{Avg,50pct,75pct,95pct,99pct,999pct,9999pct,Max}` | number lists | Publish latency in ms                                                                             |
| `publishDelayLatency{Avg,50pct,...,Max}`                         | lists        | Publish delay in µs; percentiles are integers                                                     |
| `endToEndLatency{Avg,50pct,...,Max}`                             | number lists | End-to-end latency in ms                                                                          |

## Aggregates

Computed over the whole measured phase, not averaged from the periods.

|                                   Field                                    |  Type  | Description |
|----------------------------------------------------------------------------|--------|-------------|
| `aggregatedPublishLatency{Avg,50pct,75pct,95pct,99pct,999pct,9999pct,Max}` | number | ms          |
| `aggregatedPublishDelayLatency{Avg,50pct,...,Max}`                         | number | µs          |
| `aggregatedEndToEndLatency{Avg,50pct,...,Max}`                             | number | ms          |

## Quantile maps

|                  Field                   | Values |
|------------------------------------------|--------|
| `aggregatedPublishLatencyQuantiles`      | ms     |
| `aggregatedPublishDelayLatencyQuantiles` | µs     |
| `aggregatedEndToEndLatencyQuantiles`     | ms     |

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
