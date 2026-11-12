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
60 seconds. This is the send's service time (`publishLatency`).

Completion semantics depend on the driver and its configuration: for example, completion can
represent client acceptance or a broker acknowledgment, with different replication and persistence
guarantees. See the relevant [driver page](../drivers/index.md) and
[Fair comparisons](fair-comparisons.md).

## Publish delay

Producers send on a schedule derived from the target rate. Publish delay is how late each send
started compared with its scheduled time, in microseconds, also capped at 60 seconds.
This is the schedule lag (`publishDelayLatency`).

Publish latency is measured from the actual send, not the scheduled one. If the client blocks, for
example because its buffer is full, later messages start late and their wait does not appear in
publish latency. Publish delay helps expose this form of coordinated omission.

Low delay and an achieved publish rate close to the target suggest producers are keeping up with
their schedule. Large or growing delay signals that sends are starting late, but does not by itself
prove the broker is saturated: inspect worker CPU, GC, errors and backlog too.

## Schedule-corrected publish latency

`publishScheduleLatency` measures the time from the intended send to successful completion on the
producer's monotonic clock. For each successful send, this is its schedule lag plus its service
time, recorded directly into a third histogram **before** computing percentiles. Adding the p99
values of the two separate histograms does not yield the p99 of this combined time.

Use this metric to see the latency a client on the intended schedule would experience, including
late starts that `publishLatency` omits. For example, a send starting 100 ms late and completing
1 ms later contributes 1 ms to service time, 100 ms to schedule lag and 101 ms to schedule-corrected
latency. Near saturation the corrected tail can grow even when service-time p99 stays flat.

Results and console logs report it in milliseconds, with microsecond precision, capped at 60 seconds.
The cap applies to the combined duration, not to the sum of the separately capped metrics.
With no schedule lag, it equals `publishLatency`. The counter-only producer readiness probe is
unchanged. Rate discovery (`producerRate: 0`) still schedules the measured load at its current
target rate, so that load can have schedule lag too.

## End-to-end latency

The time from when the producer created the message to when the consumer received it, in
milliseconds. The producer's wall clock time is carried with the message, as a broker timestamp or
inside the payload depending on the driver, and the consumer subtracts it from its own wall clock.

Consequences:

- **Clock synchronization.** In distributed runs, producer and consumer are usually on different
  machines. Any clock offset between them is added to or subtracted from every value. Synchronize
  clocks with NTP or chrony and check the offset before trusting sub-10 ms results.
- **Millisecond resolution.** Values are whole milliseconds. Messages delivered within the same
  millisecond have a latency of 0 and are recorded. Negative values from clock skew are dropped.
  A zero sample cannot distinguish sub-millisecond delivery times; use publish latency for
  sub-millisecond comparisons.
- **Driver differences.** Some drivers use a broker-assigned timestamp, some use the producer's,
  and some have coarser resolution. See the "Semantics" section of each
  [driver page](../drivers/index.md). NATS Streaming uses the server's store time, so its
  end-to-end latency excludes the producer-to-server leg and is not comparable to other drivers.
- **Range.** End-to-end values are capped at 12 hours, so backlog drains are measured without an
  oversized value failing the run. All three producer latency metrics are capped at 60 seconds.

## Backlog

The backlog is the number of messages published but not yet received, counted per subscription:

```text
backlog = subscriptionsPerTopic × messages published − messages received
```

Messages published counts only successful sends. Both totals count from the start of load,
including warm-up and the readiness probe; resetting statistics for measurement resets only the
latency histograms.

It is reported every 10 seconds in the console and in the result's `backlog` series. A backlog that
grows during a fixed-rate run means consumers cannot keep up with that rate.

## Backlog workloads

A workload with `consumerBacklogSizeGB` greater than 0 measures how the system behaves while
consumers catch up. It requires a fixed `producerRate`.

1. After warm-up, the coordinator starts backlog handling in the background and resets statistics
   for measurement. Pausing consumers and resetting statistics can overlap.
2. Consumers are paused while producers continue at the target rate: each consumer's receive
   callback blocks after recording its next message, so the backlog grows by almost the full
   publish rate. Messages already prefetched by the client library wait in the client, not in the
   broker.
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
