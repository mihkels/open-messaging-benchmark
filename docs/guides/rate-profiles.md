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

# Rate profiles

A flat `producerRate` never exercises the moments when clusters tend to fail: ramp-ups, bursts and
leader moves under changing load. A workload can instead list `stages`, a rate profile the
coordinator follows during the measured window.

## Stages

Each stage has a `durationMinutes` and a `targetRate` in msg/s across all producers. Over the
stage, the rate moves linearly from the previous stage's target to this one:

- A stage with the same target as the previous one holds the rate steady.
- A stage with `durationMinutes: 0` jumps straight to its target, a step or burst.
- The first stage starts at its own target, so it holds. The warm-up also runs at that rate.

The measured window lasts the sum of the stage durations. The coordinator recomputes the rate every
second and sends it to the workers only when it changes; as with `producerRate`, it is divided
evenly among the producer workers.

## Example: ramp

```yaml
name: ramp 50k to 200k
topics: 1
partitionsPerTopic: 16
subscriptionsPerTopic: 1
consumerPerSubscription: 1
producersPerTopic: 1
messageSize: 1024
payloadFile: payload/payload-1Kb.data
warmupDurationMinutes: 1

stages:
  - durationMinutes: 5      # hold at 50k
    targetRate: 50000
  - durationMinutes: 10     # ramp 50k -> 150k
    targetRate: 150000
  - durationMinutes: 5      # ramp 150k -> 200k
    targetRate: 200000
```

## Example: burst

```yaml
stages:
  - durationMinutes: 5      # baseline
    targetRate: 20000
  - durationMinutes: 0      # step up
    targetRate: 100000
  - durationMinutes: 2      # hold the burst
    targetRate: 100000
  - durationMinutes: 0      # step back down
    targetRate: 20000
  - durationMinutes: 5      # recovery
    targetRate: 20000
```

Sine waves or diurnal curves can be approximated with many short stages.

## Rules

`Workload.validate()` rejects a workload that sets `stages` together with:

- `producerRate` or `testDurationMinutes`; the stages replace both.
- `consumerBacklogSizeGB > 0`.
- A stage `targetRate` of `0` or less; rate discovery is not available within stages.
- A negative `durationMinutes`, or stages that add up to zero minutes.

`thresholds.minThroughputRatio` requires a positive `producerRate`, so it cannot be used with
stages.

## Results

Every result has a `targetRate` series, the target at the end of each period. Staged runs also have
a `stage` series with the index of the running stage; warm-up periods are tagged with stage `0`.
`summary.targetRate` is the average of the measured target, so it is comparable with
`summary.publishRateAvg`. See the [Results schema](../reference/results-schema.md).

The [HTML report](html-report.md) draws a staged run's target as a dashed line over the achieved
publish rate.

## Workload generation

A [workload template](workload-generation.md) can set `stages`; it is copied into every generated
workload, and the template's `producerRate` and `testDurationMinutes` are not used.
