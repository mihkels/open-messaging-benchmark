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

# ADR 0001: Per-partition skew telemetry and consumer rebalance events

- **Status:** Accepted, implemented
- **Issue:** [#36](https://github.com/mihkels/open-messaging-benchmark/issues/36)
- **Scope:** `driver-api`, `benchmark-framework` (worker and coordinator), `driver-kafka`

## Context

Each worker folds every publish and every delivery into one set of histograms per reporting period
(10 s by default), whatever the topic or partition. When throughput tops out or the tail latency
spikes, a result cannot say whether a few partitions, or the brokers leading them, are responsible,
or whether a consumer group rebalance caused the spike.

The driver SPI cannot carry the information today:

- `ConsumerCallback.messageReceived(payload, publishTimestamp)` has no partition.
- `BenchmarkProducer.sendAsync(...)` returns `CompletableFuture<Void>`, so nothing comes back from
  an acknowledgement.
- `BenchmarkConsumer` is a marker interface; there is no way to report group membership changes.

Kafka already has all of it in hand and throws it away: `ConsumerRecord.partition()` is used only
for the offset-commit map, `RecordMetadata` is ignored in the send callback, and the consumer's
`ConsumerRebalanceListener` only flips a readiness flag.

## Decision

Add an opt-in, additive extension to the driver SPI, a workload flag that switches it on, compact
per-partition metrics in the worker, and a per-period skew summary in `TestResult`. Kafka is the
reference implementation; other drivers can follow without framework changes.

### Driver API

Every addition is a `default` method or a new type, so existing drivers compile and link unchanged
and `DriverApiVersion.CURRENT` stays at 2. This is the same pattern the message-header support
used.

```java
public interface BenchmarkDriver {
    /** Whether producers and consumers report partitions and rebalances. */
    default boolean supportsPartitionTelemetry() {
        return false;
    }
}

public interface ConsumerCallback {
    /** Partition value for a message whose partition the driver does not know. */
    int UNKNOWN_PARTITION = -1;

    default void messageReceived(byte[] payload, long publishTimestamp, int partition) {
        messageReceived(payload, publishTimestamp);
    }

    default void messageReceived(ByteBuffer payload, long publishTimestamp, int partition) {
        messageReceived(payload, publishTimestamp);
    }

    /** Called by drivers with group membership when this consumer's assignment changes. */
    default void partitionsChanged(RebalanceEvent event) {}
}

public record RebalanceEvent(Kind kind, long timestampMillis, List<Integer> partitions) {
    public enum Kind { ASSIGNED, REVOKED, LOST }
}

public interface BenchmarkProducer {
    default CompletableFuture<SendResult> sendAsyncWithResult(
            Optional<String> key, byte[] payload, @Nullable Map<String, byte[]> headers) {
        CompletableFuture<Void> send =
                headers == null ? sendAsync(key, payload) : sendAsync(key, payload, headers);
        return send.thenApply(ignored -> SendResult.UNKNOWN);
    }
}

public record SendResult(int partition) {
    public static final SendResult UNKNOWN = new SendResult(ConsumerCallback.UNKNOWN_PARTITION);
}
```

Design points:

- **Rebalance events ride on `ConsumerCallback`.** The callback is already passed to
  `createConsumer` and is already per consumer, so no new `createConsumer` overload or
  `ConsumerInfo` field is needed. The topic is implied by the consumer, so the event does not carry
  it.
- **Drivers call the hook on whatever thread their client uses.** For Kafka that is the poll thread
  inside `poll()`. The framework only appends to a bounded queue there, so the callback never
  blocks the client.
- **The framework calls `sendAsyncWithResult` only when telemetry is on.** With the flag off, the
  send path is unchanged and keeps today's allocation profile.
- **The leader broker is deferred.** Kafka would need a `partitionsFor(topic)` lookup per send,
  which is not cheap enough for the hot path. `SendResult` can grow a field later without breaking
  anyone.

### Workload flag

A new workload field turns the feature on:

```yaml
partitionTelemetry: true # default false
```

It travels to the workers with the rest of the assignment. Like `headers`, a worker refuses to
start the load, before sending anything, when the flag is set and the driver's
`supportsPartitionTelemetry()` returns `false`. Silently reporting nothing would be worse than
failing.

### Worker

- `LocalWorker` wraps each consumer's callback with its topic when the flag is on.
  `DeliveryVerifier`, which also wraps the callback, forwards the new overloads and
  `partitionsChanged`; otherwise the defaults would silently drop the partition.
- `MessageProducer` sends with `sendAsyncWithResult` when the flag is on and passes the partition
  and topic to `RunCallbacks.completeSend`.
- `WorkerStats` keeps a `ConcurrentHashMap` keyed by topic and partition. Each entry holds:
  - published and consumed message counters (`LongAdder`);
  - a publish-latency and an end-to-end-latency HdrHistogram `Recorder` with **one significant
    digit** (about 10% value resolution) and a fixed 60 s range. Values above 60 s are clamped. The
    run-wide end-to-end histogram keeps its 12 h skew-tolerant range; only the per-partition copy
    is clamped.
- Messages with `UNKNOWN_PARTITION` are still counted in the run-wide metrics and skipped in the
  per-partition map.
- Rebalance events go into a bounded per-period queue (1,000 events). Overflow is counted rather
  than kept, so a rebalance storm cannot grow memory without limit.

One significant digit is enough here. The per-partition histograms exist to rank partitions and
spot outliers, not to report exact percentiles; the run-wide histograms keep four digits.

### Wire format

`PeriodStats` gains two optional components, `@JsonInclude(NON_EMPTY)`, following the existing
pattern for `sendErrorReasons` and `load`:

- `partitions`: a map from topic and partition to counts and the two interval histograms;
- `rebalances`: the period's rebalance events, each with its topic, and the overflow count.

A secondary constructor with the current arity keeps existing call sites unchanged.
`PeriodStats.aggregate` merges per key, summing counters and adding histograms, and concatenates
events tagged with the worker id. Old coordinators ignore the new fields because
`FAIL_ON_UNKNOWN_PROPERTIES` is off, and old workers simply do not send them.

### Result

The coordinator reduces the merged per-partition data to a short summary each period. Raw
per-partition histograms are **not** stored in the result: at thousands of partitions they would
dwarf everything else in it.

`TestResult` gains two optional lists, `@JsonInclude(NON_EMPTY)`, so results without telemetry are
unchanged and `schemaVersion` stays at 3:

- `partitionSkew`, one entry per period:

  |                    Field                     |                                                 Meaning                                                  |
  |----------------------------------------------|----------------------------------------------------------------------------------------------------------|
  | `partitions`                                 | Partitions that published or consumed during the period                                                  |
  | `publishMaxMeanRatio`, `consumeMaxMeanRatio` | Busiest partition's rate divided by the mean rate                                                        |
  | `publishGini`, `consumeGini`                 | Gini coefficient of per-partition message counts (0 = even, close to 1 = one partition takes everything) |
  | `topPublishP99`, `topEndToEndP99`            | The five partitions with the highest p99, as `{topic, partition, p99Ms}`                                 |

- `rebalanceEvents`, a flat list of `{period, timestampMillis, worker, topic, kind, partitions}`, so
  latency spikes can be lined up against assignment changes.

`ResultsToCsv` and the HTML report read fields by name and ignore the new ones. Charting them is out
of scope for the first implementation; `docs/reference/results-schema.md` documents them.

## Memory and wire cost

Measured with HdrHistogram 2.2.2, the version the framework uses, for a one-digit histogram with a
60 s range in microseconds:

|                                 Item                                 |     Size     |
|----------------------------------------------------------------------|--------------|
| One histogram (`long` counts, as a fixed-range `Recorder` allocates) | 3,456 B      |
| One `Recorder` (active and inactive histogram)                       | about 6.9 KB |
| One partition (two `Recorder`s, two counters, map entry)             | about 14 KB  |
| One compressed interval histogram, 100 samples                       | 90 B         |
| One compressed interval histogram, 10,000 samples                    | 225 B        |

For comparison, one run-wide end-to-end histogram (12 h range, four digits) is about 3 MB.

Per worker, memory grows only with the partitions that worker actually produces to or consumes
from:

| Partitions touched by a worker |     Heap     | `PeriodStats` growth per period |
|--------------------------------|--------------|---------------------------------|
| 100                            | about 1.4 MB | about 20–60 KB                  |
| 1,000                          | about 14 MB  | about 0.2–0.6 MB                |
| 10,000                         | about 140 MB | about 2–6 MB                    |

The wire figures assume both histograms are non-empty and include base64 encoding. Partitions idle
during a period send no histograms. `StatsTransferBenchmark` in the performance harness measures
`PeriodStats` serialization and aggregation and will show the growth.

The 10,000-partition row is the reason the feature is opt-in. If it turns out to matter, a
`Recorder` backed by an auto-resizing packed histogram measured about 1 KB per interval histogram
for the same data, roughly a third of the memory, at some cost to recording speed. The first
implementation uses the fixed-range recorder, which has predictable memory and the same recording
path as the run-wide metrics.

## Compatibility

- **Drivers:** none has to change. A driver that does not override the new methods reports
  `UNKNOWN_PARTITION` and no rebalances, and `supportsPartitionTelemetry()` returns `false`, so a
  workload asking for telemetry fails before load instead of producing an empty summary.
- **Plugins:** `DriverApiVersion` stays at 2; existing bundles keep loading.
- **Mixed versions:** an old worker sends no per-partition data and an old coordinator ignores it.
  A new coordinator with only old workers produces no `partitionSkew` entries.
- **Results:** `schemaVersion` stays at 3; readers that do not know the new fields ignore them.

## Kafka reference implementation

- `KafkaBenchmarkConsumer` passes `ConsumerRecord.partition()` to the three-argument
  `messageReceived`.
- Its existing `ConsumerRebalanceListener` also calls `partitionsChanged` from
  `onPartitionsAssigned`, `onPartitionsRevoked` and `onPartitionsLost`, with the wall-clock time.
- `KafkaBenchmarkProducer` overrides `sendAsyncWithResult` and completes it with
  `RecordMetadata.partition()`.
- `KafkaBenchmarkDriver.supportsPartitionTelemetry()` returns `true`. KoP reuses the Kafka producer
  and consumer classes, so it gets the plumbing, but keeps the flag `false` until it is verified
  against a KoP broker.

## Implementation notes

Small refinements made while implementing the decision:

- Each `partitionSkew` entry carries its `period` index, because periods without per-partition
  traffic have no entry.
- `TestResult.droppedRebalanceEvents` reports the rebalance events workers dropped at the
  per-period cap.
- The Kafka listener skips empty assignment changes, which the cooperative protocol reports on
  every rebalance.
- Merged per-partition histograms are auto-resizing, so histograms from workers with another range
  still add.

## Alternatives considered

- **Bump `DriverApiVersion` to 3 and change the signatures.** It would break every bundle and every
  `plugin.yaml` for a feature most drivers will not implement.
- **A separate rebalance listener parameter on `createConsumer`.** It needs a new overload and a new
  `ConsumerInfo` field and gives nothing the existing per-consumer callback does not.
- **Full per-partition histograms in `TestResult`.** Result size would grow with partition count;
  the summary keeps the answer to "which partitions" without the bulk.
- **Always on when the driver supports it.** Every Kafka run would pay for the map lookup per
  message and the extra `PeriodStats` payload, including runs that do not need it.

## Test plan

- `driver-api`: the default overloads delegate, and `sendAsyncWithResult` completes with
  `SendResult.UNKNOWN`.
- Framework: `WorkerStats` per-partition recording and clamping; `PeriodStats` JSON round trip with
  and without the new fields and per-key aggregation; skew maths (max/mean ratio, Gini, top N) as a
  unit test; `TestResultJsonTest` golden file; the worker gate when the driver does not support the
  flag; `DeliveryVerifier` forwarding.
- Kafka: `KafkaBenchmarkConsumerPollingTest` (partition passed through),
  `KafkaBenchmarkConsumerReadinessTest` (assigned, revoked and lost events),
  `KafkaBenchmarkProducerTest` (partition from `RecordMetadata`), and a `KafkaE2eIT` case with
  `partitionTelemetry: true`.

