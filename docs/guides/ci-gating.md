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

# CI performance gates

Use the workload's optional `thresholds` block to make a completed benchmark run fail when its
measured steady-state results miss an SLO:

```yaml
name: service-performance
topics: 1
partitionsPerTopic: 16
subscriptionsPerTopic: 1
consumerPerSubscription: 1
producersPerTopic: 1
producerRate: 50000
messageSize: 1024
payloadFile: payload/payload-1Kb.data
warmupDurationMinutes: 1
testDurationMinutes: 10
thresholds:
  maxPublishLatency99Ms: 25
  minThroughputRatio: 0.95
```

The latency limit is checked against the aggregated measured publish p99 in milliseconds. The
throughput ratio is average measured publish rate divided by `producerRate`; rate-discovery
workloads (`producerRate: 0`) cannot use this check. Warm-up measurements are discarded before
threshold evaluation.

Run the workload as usual in CI and use its process status as the gate:

```bash
bin/benchmark --drivers driver-kafka/kafka-latency.yaml \
  --output result.json ci-workload.yaml
```

Exit status `0` means all configured checks passed. Status `2` means the run completed but a
threshold failed; other non-zero statuses indicate a run, configuration or cleanup error. The
result JSON is written before the threshold exit and includes a `thresholds` object with the overall
status and each check's limit, observed value and status. The coordinator also prints a pass/fail
table. Without a `thresholds` block, behavior is unchanged.

Keep the workload, driver configuration and result JSON as CI artifacts to make regressions
reproducible. To gate on a change relative to an earlier run, rather than on a fixed limit, compare
the result files with `bin/benchmark-report diff --fail-on-regression`; see
[Comparing runs](comparing-runs.md). Choose limits based on stable repeated measurements and use a sufficiently long
measured window for reliable tail latency; see [reproducibility](../methodology/reproducibility.md).
