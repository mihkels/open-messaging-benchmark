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
- Worker count, machine type, `HEAP_OPTS` and any `JVM_OPTS`.
- Operating system, kernel and filesystem settings that affect I/O, and the network between workers
  and brokers.
- The full command line and the `benchmark-worker.log` from the coordinator.

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
compaction or other load.

Results files carry a timestamp in their name, so repeated runs do not overwrite each other unless
you use `--output`.

## Order effects

The coordinator runs workloads in file name order and drivers in the order given. When several
systems share the same machines, earlier runs can leave behind disk, cache or network state. Run
systems on separate clusters, or rotate the driver order between repeats.

## Automate

The [workload generation tool](../guides/workload-generation.md) produces families of workload
files from one template, and a script that runs `bin/benchmark` for a list of drivers and stores the
result files with their inputs is enough for most studies. The
[Helm chart](../guides/containers-and-kubernetes.md) and the per-driver `deploy/` directories help
recreate the environment.
