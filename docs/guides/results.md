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

Per-sample arrays have one value per 10-second sample, warm-up first; `phase` tags each sample
(`WARMUP`, `BACKLOG_BUILD`, `BACKLOG_DRAIN`, `MEASURE`). `summary` and `aggregated*` values cover
the measured period only, so skip the warm-up samples when you compute your own:

```bash
jq '((.phase // []) | map(select(. == "WARMUP")) | length) as $w
    | {driver, workload,
     avgPublishRate: (.publishRate[$w:] | add / length),
     maxBacklog: (.backlog[$w:] | max),
     maxPublishErrorRate: (.publishErrorRate[$w:] | max),
     pubP99: .aggregatedPublishLatency99pct,
     pubP999: .aggregatedPublishLatency999pct,
     e2eP99: .aggregatedEndToEndLatency99pct}' result.json
```

Use the aggregated percentiles to summarize a run. Do not average per-sample percentiles; an
average of 10-second p99 values is not the p99 of the run. `publishErrorRate` is a rate in errors
per second, not a count of errors.

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

Result files include run metadata and a measured-window summary. Add `--include-histograms` to
`bin/benchmark` to include compressed raw latency histograms; they are omitted by default to keep
files smaller. See the [results schema](../reference/results-schema.md) for field definitions.

## Charts

`bin/benchmark-report html` turns one or more result files into an HTML page with interactive
charts: latency percentile spectrum, throughput and backlog over time, latency over time, and a
throughput-vs-latency curve across runs. See [HTML report](html-report.md).

```bash
bin/benchmark-report html results/*.json -o report.html
```

## Comparing two setups

`bin/benchmark-report diff baseline.json candidate.json` prints a table of throughput, latency
percentiles, errors and backlog with the delta of each, as text, Markdown or JSON. It accepts
repeated runs per side and can fail a CI job on a regression. See
[Comparing runs](comparing-runs.md).

## Keeping results

Store each result file with the driver configuration, workload file, broker configuration and a
description of the environment used to produce it. See
[Reproducibility](../methodology/reproducibility.md).
