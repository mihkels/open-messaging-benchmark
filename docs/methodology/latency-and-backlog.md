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

# Latency and backlog

This page explains exactly what each latency number measures and how backlog workloads run.

## Publish latency

The time from the moment the driver's send call starts until its send future completes successfully,
measured on the producer's monotonic clock. Only successful sends are recorded. Values are capped at
60 seconds.

Completion semantics depend on the driver and its configuration: for example, completion can
represent client acceptance or a broker acknowledgment, with different replication and persistence
guarantees. See the relevant [driver page](../drivers/index.md) and
[Fair comparisons](fair-comparisons.md).

## Publish delay

Producers send on a schedule derived from the target rate. Publish delay is how late each send
started compared with its scheduled time, in microseconds, also capped at 60 seconds.

Publish latency is measured from the actual send, not the scheduled one. If the client blocks, for
example because its buffer is full, later messages start late and their wait does not appear in
publish latency. Publish delay helps expose this form of coordinated omission.

Low delay and an achieved publish rate close to the target suggest producers are keeping up with
their schedule. Large or growing delay signals that sends are starting late, but does not by itself
prove the broker is saturated: inspect worker CPU, GC, errors and backlog too.

For an individual successful send, the time from scheduled send to completion is its publish delay
plus its publish latency. The result contains separate histograms, not paired per-message values;
adding their p99 values does not yield the p99 of that combined time.

## End-to-end latency

The time from when the producer created the message to when the consumer received it, in
milliseconds. The producer's wall clock time is carried with the message, as a broker timestamp or
inside the payload depending on the driver, and the consumer subtracts it from its own wall clock.

Consequences:

- **Clock synchronization.** In distributed runs, producer and consumer are usually on different
  machines. Any clock offset between them is added to or subtracted from every value. Synchronize
  clocks with NTP or chrony and check the offset before trusting sub-10 ms results.
- **Millisecond resolution.** Values are whole milliseconds. Messages delivered within the same
  millisecond have a latency of 0 and are **not recorded**, and negative values from clock skew are
  dropped too. On fast local setups this biases the end-to-end distribution upward. Compare
  end-to-end numbers only between runs with similar latency ranges, and use publish latency for
  sub-millisecond comparisons.
- **Driver differences.** Some drivers use a broker-assigned timestamp, some use the producer's,
  and some have coarser resolution. See the "Semantics" section of each
  [driver page](../drivers/index.md).
- **Range.** Values up to 12 hours are recorded, so backlog drains are measured.

## Backlog

The backlog is the number of messages published but not yet received, counted per subscription:

```text
backlog = subscriptionsPerTopic × messages published − messages received
```

It is reported every 10 seconds in the console and in the result's `backlog` series. A backlog that
grows during a fixed-rate run means consumers cannot keep up with that rate.

## Backlog workloads

A workload with `consumerBacklogSizeGB` greater than 0 measures how the system behaves while
consumers catch up. It requires a fixed `producerRate`.

1. After warm-up, the coordinator starts backlog handling in the background and resets statistics
   for measurement. Pausing consumers and resetting statistics can overlap.
2. Consumers are paused while producers continue at the target rate. Each consumer stops after
   receiving one more message, so the backlog grows by almost the full publish rate.
3. When `backlog × message size` reaches `consumerBacklogSizeGB` (1 GB = 1024³ bytes), consumers
   resume. Producers keep publishing at the same rate throughout.
4. The coordinator waits until the backlog falls to
   `max(1000, (1 − backlogDrainRatio) × backlog capacity in messages)`.
5. Measurement continues for `testDurationMinutes` after the drain, then the run ends.

The result therefore covers the build-up, the drain and the steady state after it. During the
drain, consumers read older data, often from disk rather than cache, and end-to-end latency rises to
the age of the oldest messages. Read the time series, not only the aggregates:

- A `consumeRate` above `publishRate × subscriptionsPerTopic` indicates that the backlog is
  shrinking; the difference is the catch-up rate.
- The time for `backlog` to return to near zero is the drain time.
- `publishLatency` during the drain shows whether catch-up reads hurt producers.

> **Note:** If consumers cannot read faster than producers write, the backlog never drains and the
> run does not end. Choose a `producerRate` well below the maximum consume rate, or a
> `backlogDrainRatio` below 1.0.

## Rate discovery

With `producerRate: 0`, the rate starts at 10,000 msg/s and is adjusted every 3 seconds based on how
far producers fell short and how many messages remain unreceived. Latency is measured while the rate
changes, so the result mixes several rates. See [Workloads](../guides/workloads.md).
