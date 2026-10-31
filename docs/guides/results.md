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

# Results

Every workload and driver combination produces one JSON file. This page covers naming, quick
inspection, CSV conversion and charts. The [Results schema](../reference/results-schema.md)
describes every field.

## File names

Without `-o`, the coordinator writes to the current directory:

```text
<workload file name>-<driver name>-<yyyy-MM-dd-HH-mm-ss>.json
```

The workload part is the workload file name without `.yaml`; the driver part is the `name` field
of the driver configuration. Give each driver configuration a distinct `name` (for example
`Kafka-acks-all` and `Kafka-acks-1`) so results and charts can be told apart.

`-o <file>` writes to a fixed path. Use it only for a single workload and driver, because each
combination overwrites the same file.

## Inspecting a result

Per-sample arrays have one value per 10-second sample of the measured period; `aggregated*` values
cover the whole period.

```bash
jq '{driver, workload,
     avgPublishRate: ((.publishRate | add) / (.publishRate | length)),
     maxBacklog: (.backlog | max),
     errors: (.publishErrorRate | add),
     pubP99: .aggregatedPublishLatency99pct,
     pubP999: .aggregatedPublishLatency999pct,
     e2eP99: .aggregatedEndToEndLatency99pct}' result.json
```

Use the aggregated percentiles to summarize a run. Do not average per-sample percentiles; an
average of 10-second p99 values is not the p99 of the run.

## Converting to CSV

`--csv` converts every `*.json` file in a directory into one CSV file and exits without running a
benchmark:

```bash
bin/benchmark --csv results/
```

The output is `results-<epoch seconds>.csv` in the current directory, with one row per result:

```text
topics,partitions,message-size,producers-per-topic,consumers-per-topic,prod-rate-min,prod-rate-avg,prod-rate-std-dev,prod-rate-max,con-rate-min,con-rate-avg,con-rate-std-dev,con-rate-max
```

The CSV contains rates only. For latency, read the JSON files directly.

## Charts

`bin/create_charts.py` draws SVG charts with [pygal](https://www.pygal.org/):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install pygal
python3 bin/create_charts.py results/*.json
```

Results are grouped by their `workload` field, which is the `name` in the workload file, and each
driver `name` becomes one series. For every workload the script writes these files to the current
directory:

|                         File suffix                          |                   Content                   |
|--------------------------------------------------------------|---------------------------------------------|
| `Publish latency 99pct.svg`                                  | Publish p99 per sample, ms                  |
| `Publish Delay latency 99pct.svg`                            | Publish delay p99 per sample, µs            |
| `Publish rate.svg`, `Consume rate.svg`                       | Rates per sample, msg/s                     |
| `Publish Error rate.svg`                                     | Errors per sample                           |
| `End To End Latency 95pct.svg`, `End To End Latency Avg.svg` | End-to-end latency per sample, ms           |
| `Publish Latency Quantiles.svg`                              | Aggregated publish latency by percentile    |
| `Publish Delay Latency Quantiles.svg`                        | Aggregated publish delay by percentile      |
| `End To End Latency Quantiles.svg`                           | Aggregated end-to-end latency by percentile |

Each file name starts with `<workload name> - `, with `/` replaced by `-`. To compare drivers on one
chart, pass their results for the same workload together.

## Keeping results

Store each result file with the driver configuration, workload file, broker configuration and a
description of the environment used to produce it. See
[Reproducibility](../methodology/reproducibility.md).
