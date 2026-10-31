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

# Workload schema

A workload file is YAML. Unknown fields are ignored, so a misspelled field silently keeps its
default. See [Workloads](../guides/workloads.md) and [Payloads and keys](../guides/payloads-and-keys.md)
for explanations and examples.

## Fields

|            Field            |  Type   | Default  |                                          Description                                           |
|-----------------------------|---------|----------|------------------------------------------------------------------------------------------------|
| `name`                      | string  | none     | Stored as `workload` in the result and used to group charts                                    |
| `topics`                    | int     | 0        | Number of topics to create                                                                     |
| `partitionsPerTopic`        | int     | 0        | Partitions per topic; interpretation depends on the driver                                     |
| `subscriptionsPerTopic`     | int     | 0        | Independent subscriptions per topic; each receives every message                               |
| `consumerPerSubscription`   | int     | 0        | Consumers sharing each subscription                                                            |
| `producersPerTopic`         | int     | 0        | Producers per topic                                                                            |
| `producerRate`              | int     | 0        | Total target rate in msg/s across all producers; `0` searches for the maximum sustainable rate |
| `messageSize`               | int     | 0        | Message size in bytes, for payload files and randomized payloads                               |
| `payloadFile`               | path    | none     | File used as every message's payload; its length must equal `messageSize`                      |
| `useRandomizedPayloads`     | boolean | false    | Generate a pool of random payloads instead of using `payloadFile`                              |
| `randomBytesRatio`          | number  | 0.0      | Fraction of each randomized payload that is random; the rest is zeros                          |
| `randomizedPayloadPoolSize` | int     | 0        | Number of randomized payloads generated                                                        |
| `messageSizeDistribution`   | map     | none     | Size ranges and weights; overrides `messageSize`, `payloadFile` and the payload pool           |
| `keyDistributor`            | enum    | `NO_KEY` | `NO_KEY`, `KEY_ROUND_ROBIN` or `RANDOM_NANO`                                                   |
| `consumerBacklogSizeGB`     | long    | 0        | Build a backlog of this size during measurement; `0` disables it                               |
| `backlogDrainRatio`         | number  | 1.0      | Desired fraction to drain before the post-drain interval, subject to a 1,000-message floor     |
| `warmupDurationMinutes`     | int     | 1        | Warm-up length; its statistics are discarded                                                   |
| `testDurationMinutes`       | int     | 0        | Measured period in minutes; values at or below `0` have no automatic end                       |

Set the topology and duration fields explicitly. In particular, a missing or non-positive
`testDurationMinutes` causes the measured run to continue indefinitely until interrupted; it does
not produce a zero-length run. Use positive topic, producer and consumer counts for a meaningful
benchmark.

## Payload precedence

1. `messageSizeDistribution`, if present and not empty.
2. Otherwise, randomized payloads if `useRandomizedPayloads` is `true`.
3. Otherwise, `payloadFile`.

## messageSizeDistribution

```yaml
messageSizeDistribution:
  "<min>-<max>": <weight>
```

- `min` and `max` are byte counts with an optional `B`, `KB` or `MB` suffix (case-insensitive,
  `1KB = 1024`). `max` must be at least `min`.
- Weights are non-negative integers and must sum to more than zero.
- Each range contributes one payload at its midpoint, chosen with probability proportional to its
  weight.
- The result's `messageSize` and the backlog size computation use the weighted average of the
  midpoints.

## Constraints

|                     Condition                      |                             Result                             |
|----------------------------------------------------|----------------------------------------------------------------|
| `producerRate: 0` with `consumerBacklogSizeGB > 0` | `Cannot probe producer sustainable rate when building backlog` |
| Backlog requested with a message size of zero      | `Message size must be positive when building a backlog`        |
| `payloadFile` length differs from `messageSize`    | `Payload length mismatch`                                      |
| Invalid distribution range or weight               | An error naming the range or weight                            |

## Example

```yaml
name: 4 topics, 16 partitions, mixed sizes
topics: 4
partitionsPerTopic: 16
subscriptionsPerTopic: 1
consumerPerSubscription: 2
producersPerTopic: 2
producerRate: 20000
keyDistributor: KEY_ROUND_ROBIN
messageSizeDistribution:
  "100B-1KB": 6
  "1KB-10KB": 3
  "10KB-100KB": 1
useRandomizedPayloads: true
randomBytesRatio: 0.5
consumerBacklogSizeGB: 0
warmupDurationMinutes: 2
testDurationMinutes: 15
```

## Workers file

`--workers-file` and the default `workers.yaml` use this format:

```yaml
workers:
  - http://worker-1:8080
  - http://worker-2:8080
```

