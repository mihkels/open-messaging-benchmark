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

|            Field            |  Type   | Default  |                                                                                                  Description                                                                                                  |
|-----------------------------|---------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`                      | string  | none     | Stored as `workload` in the result and used to group charts                                                                                                                                                   |
| `topics`                    | int     | 0        | Number of topics to create                                                                                                                                                                                    |
| `partitionsPerTopic`        | int     | 0        | Partitions per topic; interpretation depends on the driver                                                                                                                                                    |
| `subscriptionsPerTopic`     | int     | 0        | Independent subscriptions per topic; each receives every message                                                                                                                                              |
| `consumerPerSubscription`   | int     | 0        | Consumers sharing each subscription                                                                                                                                                                           |
| `producersPerTopic`         | int     | 0        | Producers per topic                                                                                                                                                                                           |
| `producerRate`              | int     | 0        | Total target rate in msg/s across all producers; `0` searches for the maximum sustainable rate                                                                                                                |
| `messageSize`               | int     | 0        | Message size in bytes, for payload files and randomized payloads                                                                                                                                              |
| `payloadFile`               | path    | none     | File used as every message's payload, relative to the current directory; its length must equal `messageSize`. Required unless `payloadGenerator`, `useRandomizedPayloads` or `messageSizeDistribution` is set |
| `useRandomizedPayloads`     | boolean | false    | Generate a pool of random payloads instead of using `payloadFile`                                                                                                                                             |
| `randomBytesRatio`          | number  | 0.0      | Fraction of each randomized payload that is random; the rest is zeros                                                                                                                                         |
| `randomizedPayloadPoolSize` | int     | 0        | Number of randomized payloads generated; must be positive when `useRandomizedPayloads` is true and no distribution is set                                                                                     |
| `messageSizeDistribution`   | map     | none     | Size ranges and weights; overrides `messageSize`, `payloadFile` and the payload pool                                                                                                                          |
| `payloadGenerator`          | object  | none     | Generated payloads with a target compression ratio; excludes `payloadFile` and `useRandomizedPayloads`. See below                                                                                             |
| `keyDistributor`            | enum    | `NO_KEY` | `NO_KEY`, `KEY_ROUND_ROBIN`, `RANDOM_NANO` or `ZIPFIAN`                                                                                                                                                       |
| `keyDistribution`           | object  | none     | Optional `ZIPFIAN` key count and exponent; see below                                                                                                                                                          |
| `headers`                   | object  | none     | Generated headers sent with every message; needs a driver that supports them. See [headers](#headers)                                                                                                         |
| `consumerBacklogSizeGB`     | long    | 0        | Build a backlog of this size during measurement; `0` disables it                                                                                                                                              |
| `backlogDrainRatio`         | number  | 1.0      | Desired fraction to drain before the post-drain interval, subject to a 1,000-message floor                                                                                                                    |
| `warmupDurationMinutes`     | int     | 1        | Warm-up length; its statistics are discarded                                                                                                                                                                  |
| `testDurationMinutes`       | int     | 0        | Measured period in minutes; values at or below `0` have no automatic end                                                                                                                                      |
| `thresholds`                | object  | none     | Optional measured-window SLO checks; see [CI gating](../guides/ci-gating.md)                                                                                                                                  |
| `stages`                    | list    | none     | Rate profile of `durationMinutes` and `targetRate` entries; replaces `producerRate` and `testDurationMinutes`. See [Rate profiles](../guides/rate-profiles.md)                                                |
| `consumerPause`             | object  | none     | Pause consumers `afterMinutes` into the measured run for `durationMinutes`; see [Consumer pause](../guides/workloads.md#consumer-pause)                                                                       |
| `repeat`                    | int     | 1        | Run the workload this many times with each driver and summarize the spread; `--repeat` overrides it. See [Repeated runs](../guides/results.md#repeated-runs)                                                  |
| `checks`                    | object  | defaults | Limits for the built-in run checks that write `findings` to the result; see [Run checks](#run-checks)                                                                                                         |
| `verifyDelivery`            | boolean | false    | Stamp each message with its producer and sequence number and count lost, duplicated and reordered messages; see [Delivery verification](../guides/results.md#delivery-verification)                           |

Set the topology and duration fields explicitly. In particular, a missing or non-positive
`testDurationMinutes` causes the measured run to continue indefinitely until interrupted; it does
not produce a zero-length run. Use positive topic, producer and consumer counts for a meaningful
benchmark.

## Payload precedence

1. `payloadGenerator`, if present, sized by `messageSizeDistribution` or `messageSize`.
2. Otherwise, `messageSizeDistribution`, if present and not empty.
3. Otherwise, randomized payloads if `useRandomizedPayloads` is `true`.
4. Otherwise, `payloadFile`.

## payloadGenerator

```yaml
payloadGenerator:
  type: COMPRESSIBLE
  targetCompressionRatio: 0.45
  poolSize: 1000
```

All three fields are required. `type` is `COMPRESSIBLE`. `targetCompressionRatio` is the zstd
level 3 compressed size divided by the original size and must be greater than 0 and at most 1.
`poolSize` is the number of distinct payloads per message size and must be positive; with
`messageSizeDistribution` each range gets `poolSize` payloads carrying its weight. The achieved
ratio is recorded as `payloadCompressionRatio` in the result. See
[Payloads and keys](../guides/payloads-and-keys.md#4-payloads-with-a-target-compression-ratio).

## Zipfian keys

```yaml
keyDistributor: ZIPFIAN
keyDistribution:
  keys: 10000
  exponent: 0.99
```

`keys` must be between 1 and 1,000,000; `exponent` must be finite and non-negative.
Rank `r` (starting at 1) has probability proportional to `1 / r^exponent`. Zero gives a
uniform distribution; larger exponents concentrate updates on fewer keys. If the entire
`keyDistribution` block is omitted, `ZIPFIAN` uses 10,000 keys and exponent 0.99.
If the block is present, both fields are required.
The same deterministic keys and ranks are used by every worker and producer. Each worker
precomputes and shares an O(keys) alias table; sampling takes constant time per message.
`keyDistribution` is ignored for other distributors.

## headers

```yaml
headers:
  count: 4
  valueSize: 64
```

Both fields are required. Every message carries `count` headers named `omb_h0` to
`omb_h<count-1>`, each with a `valueSize`-byte ASCII alphanumeric value. Workers pre-generate 64
header sets before the load starts and each producer cycles through them, so the send path does not
allocate header values and batch compression cannot collapse identical headers. Header bytes are
not counted in the throughput byte counters, which stay payload-only on both sides; the overhead
shows up as lower throughput and higher latency. See
[Payloads and keys](../guides/payloads-and-keys.md#message-headers) and the
[driver support matrix](../drivers/index.md#message-headers).

## messageSizeDistribution

```yaml
messageSizeDistribution:
  "<min>-<max>": <weight>
```

- `min` and `max` are byte counts with an optional `B`, `KB` or `MB` suffix (case-insensitive,
  `1KB = 1024`). `max` must be at least `min`.
- Weights are non-negative integers and must sum to more than zero.
- Each range contributes one payload at its midpoint, chosen with probability proportional to its
  weight. The payload is all zeros unless `useRandomizedPayloads` is `true`, in which case
  `randomBytesRatio` applies; `randomizedPayloadPoolSize` and `payloadFile` are ignored. With a
  `payloadGenerator`, each range contributes `poolSize` generated payloads instead.
- The result's `messageSize` and the backlog size computation use the weighted average of the
  midpoints.

## Constraints

|                                              Condition                                              |                                                  Result                                                   |
|-----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `producerRate: 0` with `consumerBacklogSizeGB > 0`                                                  | `Cannot probe producer sustainable rate when building backlog`                                            |
| Backlog requested with a message size of zero                                                       | `Message size must be positive when building a backlog`                                                   |
| `payloadFile` length differs from `messageSize`                                                     | `Payload length mismatch`                                                                                 |
| Invalid distribution range or weight                                                                | An error naming the range or weight                                                                       |
| `useRandomizedPayloads: true` without `messageSizeDistribution` and `randomizedPayloadPoolSize` ≤ 0 | `Randomized payload pool size must be positive`                                                           |
| `payloadGenerator` missing a field                                                                  | `payloadGenerator requires type, targetCompressionRatio and poolSize`                                     |
| `targetCompressionRatio` ≤ 0, > 1 or not a number                                                   | `payloadGenerator targetCompressionRatio must be in (0, 1]`                                               |
| `payloadGenerator.poolSize` ≤ 0                                                                     | `payloadGenerator poolSize must be positive`                                                              |
| `payloadGenerator` with `payloadFile`                                                               | `payloadGenerator and payloadFile are mutually exclusive`                                                 |
| `payloadGenerator` with `useRandomizedPayloads: true`                                               | `payloadGenerator and useRandomizedPayloads are mutually exclusive`                                       |
| `payloadGenerator` without `messageSizeDistribution` and `messageSize` ≤ 0                          | `Message size must be positive with payloadGenerator`                                                     |
| `payloadGenerator.poolSize` × the distribution weights above 2,147,483,647                          | `payloadGenerator poolSize times the distribution weights exceeds the supported total`                    |
| `headers` missing a field                                                                           | `headers requires count and valueSize`                                                                    |
| `headers.count` ≤ 0                                                                                 | `headers count must be positive`                                                                          |
| A negative `headers.valueSize`                                                                      | `headers valueSize must not be negative`                                                                  |
| `headers` with a driver that does not support them                                                  | `Driver <class> does not support message headers; remove 'headers' from the workload`, before load starts |
| `thresholds.minThroughputRatio` with `producerRate: 0`                                              | `minThroughputRatio requires a positive producerRate`                                                     |
| `stages` with `producerRate` other than `0`                                                         | `stages and producerRate are mutually exclusive`                                                          |
| `stages` with `testDurationMinutes` other than `0`                                                  | `stages and testDurationMinutes are mutually exclusive`                                                   |
| `stages` with `consumerBacklogSizeGB > 0`                                                           | `stages cannot be combined with a consumer backlog`                                                       |
| A stage `targetRate` ≤ 0                                                                            | `Stage targetRate must be positive`                                                                       |
| A negative stage `durationMinutes`                                                                  | `Stage durationMinutes must not be negative`                                                              |
| Stages adding up to 0 minutes                                                                       | `stages must last at least one minute in total`                                                           |
| `consumerPause` with `consumerBacklogSizeGB > 0`                                                    | `consumerPause cannot be combined with a consumer backlog`                                                |
| `consumerPause` with `producerRate: 0` and no `stages`                                              | `consumerPause requires a fixed producerRate or stages`                                                   |
| A negative `consumerPause.afterMinutes`                                                             | `consumerPause afterMinutes must not be negative`                                                         |
| `consumerPause.durationMinutes` ≤ 0                                                                 | `consumerPause durationMinutes must be positive`                                                          |
| `afterMinutes + durationMinutes` ≥ the measured minutes                                             | `consumerPause must end before the measured run ends`                                                     |
| `repeat` below 1                                                                                    | `repeat must be at least 1`                                                                               |
| `verifyDelivery: true` with no producers or subscriptions per topic                                 | `verifyDelivery needs at least one producer and one subscription per topic`                               |
| `verifyDelivery: true` with a payload shorter than 20 bytes                                         | `verifyDelivery needs payloads of at least 20 bytes` when the load starts                                 |
| `thresholds.maxLostMessages` or `maxDuplicatedMessages` without `verifyDelivery: true`              | `maxLostMessages requires verifyDelivery: true`                                                           |

## Thresholds

The optional `thresholds` block fails the run when any configured check is not met:

```yaml
thresholds:
  maxPublishLatency99Ms: 25
  minThroughputRatio: 0.95
```

`maxPublishLatency99Ms` is compared with the aggregated measured publish p99 latency in milliseconds.
`minThroughputRatio` is the minimum measured average publish rate divided by the configured
`producerRate`; it requires a positive `producerRate`. Warm-up statistics are excluded.

With [`verifyDelivery: true`](../guides/results.md#delivery-verification), `maxLostMessages` and
`maxDuplicatedMessages` limit the `delivery.lost` and `delivery.duplicated` counts of the result.
They cover the whole run, warm-up included, and fail when no delivery counts were recorded:

```yaml
verifyDelivery: true
thresholds:
  maxLostMessages: 0
  maxDuplicatedMessages: 0
```

## Run checks

The built-in run checks (see [Results](../guides/results.md#run-checks)) always run with these
defaults. Set any of them in an optional `checks` block, or list checks to skip in `disabled`:

```yaml
checks:
  disabled: [shortWindow]
  maxRateShortfall: 0.05        # rateShortfall: fraction below a fixed producerRate
  maxBacklogGrowthSeconds: 10   # backlogGrowth: growth in seconds of publishing
  minMeasuredMinutes: 5         # shortWindow
  minTailSamples: 10            # tailSamples: samples beyond each reported percentile
  maxSendErrors: 0              # sendErrors
  maxPublishDelayGrowthMs: 10   # publishDelayGrowth
  maxClockOffsetFraction: 0.1   # clockOffset: worker clock spread as a fraction of p50 end-to-end
  maxRunToRunCv: 0.1            # runToRunVariance: coefficient of variation across --repeat runs
  maxWorkerCpu: 0.85            # generatorSaturation: worker CPU as a fraction of its cores
  maxWorkerGc: 0.1              # generatorSaturation: worker GC pause time as a fraction of a period
```

`minTailSamples: 10` means p99 needs 1,000 messages, p99.9 10,000 and p99.99 100,000. Unknown
check names and negative limits are rejected before the run starts. `retried` and `delivery` have no
limit; disable `retried` to keep a retried run `valid`. Findings fail a run only with `--fail-on-invalid` or
`--fail-on-warning` (see [Exit codes](cli.md#exit-codes)).

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

