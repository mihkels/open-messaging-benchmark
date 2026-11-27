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

# Framework resource efficiency

This tooling measures the **load generator**, not a message broker. It has two complementary
layers: JMH benchmarks of actual framework hot paths, and synthetic-driver runs through the
framework's normal workers, workload controller, statistics serialization, and child JVMs.
No broker, Docker, Prometheus server, or Python package installation is required.

## Build and smoke checks

Use the repository's JDK and Maven from `mise.toml`, plus Python 3:

```bash
mvn --no-transfer-progress --batch-mode -Pperformance -pl performance-harness -am install
python3 -m unittest discover -s performance-harness/tests -v
bin/framework-performance run --smoke --topology process --rate 1000 \
  --processors 2 --repetitions 1 --output /tmp/omb-resource-smoke
```

The `performance` profile adds the harness module. JMH and the synthetic driver are not runtime
dependencies of the normal framework or bundled production drivers.
The launcher is source-tree tooling; it uses the module's generated `target/classpath.txt` and
`target/omb-plugin`. Rebuild after changing framework code; do not measure stale installed jars.

Smoke checks use short, explicitly configured Worker lifecycle windows. They exercise accounting,
process startup, HTTP requests, snapshots, and cleanup, but **cannot be comparison baselines**.
PR CI runs these checks without a timing regression gate.
The usual Git-aware license-plugin limitation in linked worktrees applies; validate license
checks in a separate regular clone if needed, rather than altering the main checkout.

## Whole-framework runs

Full runs use `WorkloadGenerator` with its existing readiness, warm-up, reset, periodic reports,
and measured traffic. They retain the ordinary result in `benchmark-result.json` without changing
the public benchmark result schema.

```bash
bin/framework-performance run --topology process --rate 10000 --producers 4 \
  --payload-bytes 1024 --processors 2 --repetitions 3 \
  --output /tmp/omb-resource-baseline
```

The default full workload has a warm-up and measurement phase, each controlled by
`--warmup-minutes` and `--measure-minutes`. Use separate fresh output directories for each experiment;
the launcher never overwrites an existing baseline.

|   Topology    |                                                  What is exercised                                                   |
|---------------|----------------------------------------------------------------------------------------------------------------------|
| `local`       | `LocalWorker` in a fresh harness JVM; useful for isolating process/HTTP overhead                                     |
| `process`     | Coordinator, `ProcessWorker`, actual `DriverProcessMain`, isolated driver plugin, HTTP statistics                    |
| `http`        | An additional loopback supervisor JVM using `WorkerHandler` and `HttpWorkerClient`                                   |
| `distributed` | Multiple supervisor/driver JVMs and `DistributedWorkersEnsemble` assignment, polling, serialization, and aggregation |

`--mode sync` delivers inside `sendAsync` and completes immediately.
`--mode async` uses a bounded executor; `--threads` and `--queue-capacity` control fixture capacity.
Queue rejection is a failed send, not a dropped message disguised as success.
`--mode send-only --smoke` isolates producer work without consumers; the full workload controller
requires consumer readiness and therefore does not support this mode.
`--consumers` sets the consumers on the one subscription per topic (default 1). The fixture
round-robins each message to one of them, so more consumers spread the receive callbacks without
changing how many messages arrive.

The fixture routes within a JVM only. Distributed scenarios deliberately use **independent
synthetic consumer streams**, not a transport between producer and consumer processes.
Their receive rate, end-to-end latency, and backlog are not broker-delivery measurements.
Use distributed scenarios to investigate control-plane/resource scaling, not delivery correctness.
Synchronous and asynchronous fixture overhead is included in whole-framework measurements.

Start with a small matrix: low and demanding fixed rates, 128-byte and 1024-byte payloads,
one and multiple producers, synchronous and asynchronous completion. Add `--weighted` for
3:1 payload buckets of the selected size and twice that size, and `--key KEY_ROUND_ROBIN` or
`--key RANDOM_NANO` or `--key ZIPFIAN` to exercise key generation. For distributed scaling, set `--workers` and
enough `--topics` to assign meaningful work to producer workers.

Saturation experiments are separate from fixed-load comparisons: run a series of increasing
`--rate` values into separate directories and inspect achieved rate, invalid-run reasons,
CPU/message, and scheduling delay. A lower resource bill caused by missing the offered load is
not an improvement. Do not compare two different offered rates as an efficiency regression test.

## Metrics and artifacts

Every repetition runs in a fresh JVM and retains:

|                           Artifact                            |                                           Meaning                                           |
|---------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `scenario.json`, `command.json`, `requested-jvm-options.json` | Effective scenario and requested process arguments                                          |
| `jvm-<pid>.txt`                                               | JDK `jcmd VM.command_line` output before warm-up for each JVM                               |
| `samples.jsonl`                                               | Phase-labeled per-role JVM resource samples                                                 |
| `rss-samples.jsonl`                                           | External, phase-labeled resident-memory samples                                             |
| `result.json`                                                 | Window counters, histogram sample counts, latencies, resource endpoints/peaks, and validity |
| `console.log`, supervisor logs                                | Framework, driver, and startup/cleanup diagnostics                                          |
| `failure.json`                                                | Explicit repetition failure; never silently excluded                                        |

The experiment's `manifest.json` records schema version, JDK, OS/architecture, hostname, available
processors, source revision/dirty state, source diff/untracked hashes, profiling mode, and failures.
Store output outside the checkout; outputs and JFR files are local artifacts, not source files.
Review JVM arguments and logs before sharing artifacts, especially if supplying custom properties.

JVM resource instruments use `omb_resource_*` names, separate from message instruments, and
`role="driver"` or `role="supervisor"` labels. Coordinator measurements are collected by the harness.
CPU is cumulative process CPU **seconds**, normalized as nanoseconds per successfully completed
send for comparison. Heap used/committed, non-heap, direct buffers, and sampled RSS use **bytes**.
RSS is sampled with `/proc/<pid>/status` on Linux and `ps` on macOS; it is not heap consumption.
Summed RSS counts shared pages more than once and is not unique physical memory usage.

GC collection count and collection time are management-counter deltas. Collection time is **not**
an exact GC-pause distribution. Thread counts cover platform threads, not all virtual threads.
Unsupported readings are explicitly unavailable, never zero. CPU totals include coordinator,
supervisors, and driver JVMs; the separate external Python/RSS observer is not included.

Snapshot/counter boundaries are sequential RPCs, not an atomic barrier across JVMs. The validity
check allows a boundary tolerance for histogram/delivery counts: the greater of 2% of completed
work, two messages per producer, and the offered work during the measured final snapshot/RPC
boundary (`snapshotBoundarySeconds`). This matters especially for short smoke windows, which
cannot be used as performance baselines. Exact fixture accounting
and histogram ownership are covered by correctness tests. The default required achieved-rate
ratio is 0.98; `--min-rate-ratio` changes it explicitly and becomes part of comparison compatibility.
Send failures, substantial histogram-count mismatch, lost loopback deliveries, missed fixed
load, or a publish delay (schedule lag) p99 above 100 ms make the run invalid. The delay limit
matches the framework's `publishDelay` run check
([ADR 0022](adr/0022-invalid-on-schedule-lag.md)): a run that falls behind and replays its late
sends can still meet the rate. Distributed independent receives are not checked against producer totals.

## Baseline/candidate procedure

Capture an unchanged baseline, apply one focused change, rebuild, and repeat the same command:

```bash
bin/framework-performance run --topology process --rate 10000 --producers 4 \
  --payload-bytes 1024 --processors 2 --repetitions 3 --output /tmp/omb-resource-candidate
bin/framework-performance compare /tmp/omb-resource-baseline \
  /tmp/omb-resource-candidate --output /tmp/omb-resource-comparison.json
```

Use the same machine, JDK, scenario, sampling/profiling mode, and resource budget. Run without other
heavy work. Where practical alternate fresh baseline and candidate runs to expose temperature
and host drift. Start with at least three repetitions and increase them when spread is large.
The comparison reports medians, min/max, relative changes, and a noise warning rather than a
binary performance pass. It refuses incompatible environments, smoke checks, incomplete
experiments, or invalid repetitions. Revision and deliberate JVM option changes are recorded
differences; JDK changes require a separate experiment, not an automatic same-environment comparison.

Inspect the raw per-role deltas and latency alongside CPU/message and RSS. Reduced CPU with worse
scheduling delay, growing backlog, failed sends, or lost measurements is not a win. Shared GitHub
Actions runners are suitable for smoke checks and diagnostic artifacts, not reliable performance
gates. The manually dispatched **Framework performance** workflow uploads full-run artifacts.

## JMH hot paths

```bash
bin/framework-performance micro --output /tmp/omb-resource-jmh -- \
  '.*Worker.*Benchmark.*' -prof gc -f 3
```

To compare per-message key generation against round-robin, run
`bin/framework-performance micro --output /tmp/omb-key-jmh -- '.*KeyDistributorBenchmark.*' -prof gc`.
The Zipfian case uses the default 10,000 keys and exponent 0.99; initialization is outside measurement.

Use `-l` to list benchmarks, then select a narrow expression. JMH owns warm-up, measurement,
forks, result consumption, and concurrency controls. `-prof gc` provides normalized allocation
bytes/op where supported. Results and raw output are retained as `jmh.json` and `console.log`.
Keep fork JVM options and parameters identical between revisions. Use separate single-thread
and contended runs rather than comparing one to the other.
Per-thread statistics states include substantial HDR buffers. When increasing thread counts,
provide an adequate fork heap with JMH's `-jvmArgsAppend` and retain the same budget on both sides;
do not interpret an out-of-memory or failed fork as a fast measurement. The launcher rejects
failed or missing JMH measurements.

Compare matching JMH runs, including normalized allocation metrics, with:

```bash
bin/framework-performance compare --micro /tmp/omb-resource-jmh-baseline \
  /tmp/omb-resource-jmh-candidate --output /tmp/omb-resource-jmh-comparison.json
```

This checks machine/JDK, benchmark parameters, fork/iteration settings, units, and JVM arguments
before reporting raw fork/iteration distributions. It does not impose a universal allocation or
timing threshold.

These benchmarks measure real statistics recording, callback gates/completions, rate-limiter
acquisition, histogram snapshots/aggregation, and production serialization. They do not prove
whole-framework CPU or memory savings by themselves; verify candidate changes in full runs.

## JVM experiments and profiling

The tooling intentionally uses explicit modest heap budgets, not the normal launchers' 4 GiB
driver defaults. They are **harness profiles, not recommendations for broker clients**.
Compare collectors at equal heap/processor budgets first:

```bash
bin/framework-performance run --collector parallel --heap 512m --initial-heap 128m \
  --processors 2 --output /tmp/omb-resource-parallel
bin/framework-performance run --collector zgc --heap 512m --initial-heap 128m \
  --processors 2 --output /tmp/omb-resource-zgc
```

Then vary heap size/initial commitment independently using `--heap`, `--initial-heap`,
`--supervisor-heap`, and `--supervisor-initial-heap`. `--processors` sets an explicit JVM CPU
availability budget, not OS CPU affinity. `--scheduler-parallelism` is an opt-in virtual-thread
scheduler experiment. Inspect actual process arguments in the artifacts.

`--driver-jvm-opts` and `--coordinator-jvm-opts` accept additional quoted JVM options, parsed
without shell evaluation. Required native-access/module-open options are preserved. Unsupported
collectors/options fail explicitly rather than falling back.

`--profile` enables JFR profile recordings and GC logs with distinct PID-based filenames.
Profiled runs include instrumentation overhead; compare only equally profiled runs, and repeat
without profiling to validate a proposed win. JFR allocation events are sampled estimates,
not exact bytes/op or a complete view of virtual-thread allocations. Use JMH allocation profiling
for isolated allocation questions and JFR for locating CPU/lock/allocation hotspots.

Producers run on virtual threads. A virtual thread blocked on a contended monitor unmounts instead
of pinning its carrier, so lock contention barely shows as `jdk.JavaMonitorEnter` or
`jdk.VirtualThreadPinned` events. It shows as `jdk.ThreadPark` events of idle `ForkJoinPool`
carriers and as execution samples in the locked method. To lower the event thresholds, add a second
recording to the driver, for example
`--driver-jvm-opts="-XX:StartFlightRecording=filename=<dir>/locks-%p.jfr,settings=profile,jdk.JavaMonitorEnter#threshold=20us,jdk.ThreadPark#threshold=20us"`.

### Consumer ceilings and the 4M msg/s target

In sync mode every producer thread runs the consumer callback inline, under that consumer's
monitor. With one consumer, all producers queue on one lock, so the consumer path caps throughput.
Measured with 4 producers, 128-byte payloads, sync sends and the `process` topology on an M1 Max
(the highest valid rate in 0.2M to 0.4M steps; issue #600):

|                              Fixture                              | 1 consumer | 4 consumers |
|-------------------------------------------------------------------|------------|-------------|
| Before #600 (shared monitor, `Phaser`, per-message map snapshots) | 1.2M       | 1.6M        |
| After #600 (share-nothing send path)                              | 1.4M       | 2.8M        |

JFR at a 1.6M target with one consumer: the Micrometer timer (`TimeWindowMax.record`) has 48 to 58%
of the samples, and the one consumer's `deliver` monitor has 15% once the fixture's own shared state
is gone.

What a 4M msg/s per worker target means:

- **Through one consumer**, the consumer callback path must fit in about 250 ns per message,
  because nothing runs it in parallel. It costs about 700 ns today (1.4M). Removing the
  Micrometer timer is the largest single lever, but halving the path still leaves it above
  250 ns.
- **Across several consumers**, 4M is a CPU budget question. At 2.8M the process tree used
  about 1.6 µs of CPU per message (4.5 cores), so 4M needs about 6.5 cores at today's cost per
  message.

Investigate callback synchronization, histogram ownership/copying/aggregation, completion
allocation, statistics duplication, and producer scheduling. Preserve callback isolation,
bounded shutdown, interruption, reset/pause behavior, histogram precision, and latency semantics.
No safe measured improvement is also a useful result; do not change production defaults simply
because a synthetic fixture uses less memory under one collector.
