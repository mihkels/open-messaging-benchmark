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

# Benchmark design

A benchmark answers one question. Decide the question first, then choose the workload, drivers and
environment that answer it and nothing else.

## Common questions

|                        Question                         |                                                     Approach                                                     |
|---------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| What latency do I get at my expected load?              | Fixed `producerRate` at the expected rate, then at 1.5× and 2× for headroom                                      |
| What is the maximum throughput?                         | `producerRate: 0` for an estimate, then confirm with fixed rates around it                                       |
| How does latency change with load?                      | A series of fixed rates, one workload file per rate; see [Workload generation](../guides/workload-generation.md) |
| How does the system recover when consumers fall behind? | A backlog workload; see [Latency and backlog](latency-and-backlog.md)                                            |
| What does a configuration change cost?                  | Two driver files that differ only in that setting, same workload                                                 |

## Fixed rate versus rate discovery

With `producerRate: 0`, a controller raises the rate while producers and consumers keep up and
lowers it when they fall behind. The result is the rate the controller settled near, measured
together with the latency at that point, which is usually close to saturation. Use it to find the
range of interest, not as the headline number.

Latency results are only comparable at the same offered load. For latency comparisons, always use a
fixed `producerRate` that every system sustains, and check that the measured `publishRate` matches
it.

## Model the real traffic

- **Topology.** Match the number of topics, partitions, producers and consumers. Per-partition
  ordering, consumer group rebalancing and connection counts change results more than raw message
  size.
- **Message size.** Use your real size, or a `messageSizeDistribution` that matches it.
- **Keys.** Keyed traffic changes how messages spread across partitions. Use `KEY_ROUND_ROBIN` or
  `RANDOM_NANO` if your producers use keys.
- **Compressibility.** With compression enabled, all-zero or constant payloads overstate throughput.
  Use randomized payloads with a `randomBytesRatio` that matches your data. See
  [Payloads and keys](../guides/payloads-and-keys.md).
- **Fan-out.** Each subscription receives every message, so consume traffic is
  `subscriptionsPerTopic` times the publish traffic.

## Make the clients not the bottleneck

The benchmark measures the whole path, including its own workers. If a worker runs out of CPU,
network or heap, the result describes the worker.

- Run distributed with enough workers, on machines separate from the brokers.
- Watch worker CPU, GC and network during the run; see [Monitoring](../guides/monitoring.md).
- `publishDelayLatency` growing into milliseconds means producers cannot send on schedule. Add
  workers or producers before trusting the result.

## Run length

- **Warm-up.** JIT compilation, connection setup, leader election and page-cache filling make the
  first minutes unrepresentative. Use at least the default 1 minute; use several minutes for
  systems with tiered storage, large caches or autoscaling.
- **Test duration.** Long enough to include periodic work such as log segment rolls, compaction,
  checkpoints and GC cycles. 15 minutes is a reasonable minimum for published numbers; most bundled
  workloads use 5 minutes, which suits exploration. Use an hour or more for capacity planning.
- **Tail percentiles.** p99.99 needs at least tens of thousands of samples per period to be stable.
  At low rates, prefer p99 and p99.9.

## Next

- [Fair comparisons](fair-comparisons.md)
- [Reproducibility](reproducibility.md)
- [Latency and backlog](latency-and-backlog.md)

