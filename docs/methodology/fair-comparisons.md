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

# Fair comparisons

Comparing two messaging systems is only meaningful if both provide the same guarantees under the
same conditions. The shipped driver configurations are starting points with different defaults;
they are not tuned to be equivalent.

## Same workload, same environment

- Run every system with the same workload files, worker count and worker machines.
- Use the same broker node count, instance type, disks and network. Record them.
- Run the systems one after another on the same hardware, or on identical hardware, not on shared
  machines with unrelated load.
- Pass all driver files in one `--drivers` list so every system runs each workload under the same
  coordinator; see [CLI](../reference/cli.md).

## Equivalent durability

Durability settings have the largest effect on latency. Compare like with like.

|    Consideration     |                       Kafka                       |                  Pulsar                   |       RabbitMQ        |       Others        |
|----------------------|---------------------------------------------------|-------------------------------------------|-----------------------|---------------------|
| Replicas             | `replicationFactor`                               | `ensembleSize`, `writeQuorum`             | Quorum queue replicas | See the driver page |
| Acknowledgments      | `acks=all`, `min.insync.replicas`                 | `ackQuorum`                               | Publisher confirms    | See the driver page |
| Persistence controls | `flush.messages` or `log.flush.interval.messages` | `journalSyncData` on bookies (default on) | Queue type and mode   | See the driver page |

Kafka by default acknowledges after replication to memory and relies on the page cache, while
BookKeeper syncs its journal before acknowledging. Comparing defaults compares different guarantees.
Kafka's `acks=all` does not itself require every in-sync replica to fsync before acknowledging, and
its flush settings should not be assumed to provide that guarantee. Check each system's actual
failure and persistence semantics rather than treating similarly named settings as equivalent; record
the differences in your results. See the [Kafka producer configuration](https://kafka.apache.org/35/configuration/producer-configs/)
and [topic configuration](https://kafka.apache.org/35/configuration/topic-level-configs/).

Each [driver page](../drivers/index.md) lists the configuration keys that control durability,
batching and acknowledgment for that system.

## Equivalent client behavior

- **Batching and linger.** Producer batching trades latency for throughput. Use comparable maximum
  batch delays, for example Kafka `linger.ms` and Pulsar `batchingMaxPublishDelayMs`.
- **Compression.** Enable it for all systems or none, with comparable codecs.
- **Delivery semantics.** Idempotent producers, transactions and effectively-once settings add work.
  Compare them only with the equivalent feature. Several shipped `exactly-once` configurations enable
  only part of it; read the driver page before using them.
- **Consumer acknowledgment.** Auto-commit, per-message acknowledgment and batch acknowledgment
  have different costs.
- **Ordering.** Per-partition ordering versus shared or unordered consumption changes how work
  spreads across consumers.

## Equivalent topology mapping

`partitionsPerTopic` and `subscriptionsPerTopic` mean different things to different drivers. Some
systems have no partitions, and some drivers map a subscription to a queue, consumer group or
durable subscription in different ways. Check the "Semantics" section of each driver page
before comparing results for a given workload.

## Report everything

Publish the workload files, driver files, broker configuration, versions, hardware and the raw
result JSON with any comparison. A comparison that cannot be reproduced cannot be checked. See
[Reproducibility](reproducibility.md).
