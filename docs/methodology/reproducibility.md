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

# Reproducibility

A result is useful only if someone else, or you next month, can produce the same result again.

## Record the inputs

Keep these with every set of result files:

- The workload files and driver files exactly as run, including the `name` fields, because results
  identify them only by name.
- The benchmark version or Git commit, and the Docker image tag if one was used.
- Broker versions, broker configuration and the number and type of broker nodes.
- Worker count, machine type, `HEAP_OPTS`, `JVM_OPTS`, `SUPERVISOR_HEAP_OPTS` and
  `OMB_DRIVER_JVM_OPTS` if set.
- Operating system, kernel and filesystem settings that affect I/O, and the network between workers
  and brokers.
- The full command line, and `benchmark-worker.log` and `benchmark-driver.log` from the coordinator
  and every worker.

> **Note:** Give every driver file and workload a distinct `name`. Several shipped driver files share
> a name such as `Kafka`, so results produced with them cannot be told apart afterwards.

## Control the environment

- **Start from a clean state.** Delete topics from previous runs, or recreate the cluster. Leftover
  data changes disk usage, cache behavior and leader placement.
- **Pin versions.** Use fixed image tags, not `latest`. The Helm chart's default image is not
  maintained; see [Containers and Kubernetes](../guides/containers-and-kubernetes.md).
- **Avoid noisy neighbors.** Shared or burstable instances, other tenants and background jobs make
  results vary between runs.
- **Synchronize clocks.** End-to-end latency compares the producer's clock with the consumer's
  clock. Run NTP or chrony on all worker machines; see [Latency and backlog](latency-and-backlog.md).

## Repeat runs

Run each combination at least three times and report the spread, not only the best run. Large
differences between repeats point to an uncontrolled factor such as disk caches, background
compaction, JIT warm-up, garbage collection or noisy neighbours on shared cloud machines. A single
run gives one number with no sense of its noise, so two single runs cannot tell a real difference
from variance.

`--repeat N`, or `repeat: N` in the workload, does this for you:

```bash
bin/benchmark --drivers driver-kafka/kafka.yaml --repeat 3 -o result.json workloads/simple-workload.yaml
```

Each repetition creates fresh topics and removes them afterwards, so no repetition reads another's
data. The coordinator writes `result-1.json` to `result-3.json` and a `result-summary.json` with the
median, min, max and coefficient of variation (CV) of throughput and the main latency percentiles;
see [Repeated runs](../guides/results.md#repeated-runs).

- **Read the median.** It ignores one outlier repetition, so workload thresholds are judged on the
  medians when repeating.
- **Check the CV.** A metric with a CV above `checks.maxRunToRunCv` (default `0.1`, 10%) gets a
  `runToRunVariance` warning in the summary. Differences between two setups smaller than that
  spread are noise. Tail percentiles such as p99.9 vary more than averages; raise the limit or run
  longer rather than ignore the warning.
- **Failed repetitions.** `--retries` applies inside each repetition. A repetition that still fails
  is listed in the summary and left out of the statistics; with fewer than two successes the
  summary reports no CV.
- **Verdict.** The summary has its own `validity`: the worst of every repetition's verdict, the
  `runToRunVariance` warnings and a `failedRepetitions` finding. `--fail-on-warning` therefore fails
  a noisy repeated run even when each repetition is valid on its own.
- **How many.** Three repetitions show whether results are stable; use five or more when the
  difference you want to detect is close to the CV.

Without `--repeat`, results files carry a timestamp in their name, so separate invocations do not
overwrite each other unless you use `--output`.

## Order effects

The coordinator runs workloads in file name order and drivers in the order given. When several
systems share the same machines, earlier runs can leave behind disk, cache or network state. Run
systems on separate clusters, or rotate the driver order between repeats.

## Automate

The [workload generation tool](../guides/workload-generation.md) produces families of workload
files from one template, and a script that runs `bin/benchmark` for a list of drivers and stores the
result files with their inputs is enough for most studies. The
[Docker images and Helm chart](../guides/containers-and-kubernetes.md) help recreate the
environment.
