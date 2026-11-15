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

## Run checks

A run that completes is not automatically a valid measurement. After each run the coordinator
checks the result and writes what it found to `findings`, one entry per problem, and logs each one
at the end of the run. The field is omitted when nothing was found:

```json
"findings": [
  {
    "check": "rateShortfall",
    "severity": "warning",
    "message": "Average publish rate 612.4 msg/s is 38.8% below the target 1000.0 msg/s",
    "value": 0.388,
    "limit": 0.05
  }
]
```

`severity` is `info`, `warning` or `invalid`; `value` and `limit` are present when the check
compares a number with a limit. The built-in checks are:

|        Check         |             Severity             |                                                            Finds                                                            |
|----------------------|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `rateShortfall`      | invalid                          | Average publish rate below a fixed `producerRate` by more than the limit; value is the shortfall fraction                   |
| `backlogGrowth`      | warning                          | Backlog growth over the measured window, in seconds of publishing; skipped for backlog and pause workloads                  |
| `shortWindow`        | warning                          | Measured window shorter than the minimum, in minutes                                                                        |
| `tailSamples`        | warning, or info for p99.99 only | Too few messages for the reported tail percentiles; value is the samples, limit what the lowest unreliable percentile needs |
| `sendErrors`         | warning                          | Failed sends in the measured window, with the last reason reported by each worker                                           |
| `publishDelayGrowth` | warning                          | Rise in average publish delay from the first to the second half of the measured window, in ms                               |
| `retried`            | warning                          | The run needed more than one [`--retries`](../reference/cli.md) attempt; the message lists the earlier failures             |
| `clockOffset`        | warning                          | End-to-end latency is unreliable: producer and consumer worker clocks differ by more than a fraction of the p50, in ms      |

The `clockOffset` check reads the per-worker offsets the coordinator measures in distributed runs,
recorded in `metadata.clockOffsets`; [Clock offset](../methodology/latency-and-backlog.md#clock-offset)
explains how to read them.

Runs that stall or lose a worker fail outright instead of producing a finding. Tune or disable the
checks with the workload's [`checks`](../reference/workload-schema.md#run-checks) block.

## Validity verdict

`validity` combines the findings into one verdict, so a script or CI job does not have to inspect
each check:

```json
"validity": {
  "status": "invalid",
  "findings": [ ... ]
}
```

`status` is `invalid` when any finding is `invalid`, otherwise `warning` when any finding is
`warning`, otherwise `valid`; `info` findings do not change it. `findings` repeats the findings the
verdict is based on. After the last run the coordinator logs one verdict line per result file, for
example `result.json: invalid (rateShortfall, shortWindow)`. Gate on it with
[`--fail-on-invalid` or `--fail-on-warning`](../reference/cli.md#exit-codes). Files older than
schema version 3 have no `validity`.

## Converting to CSV

`--csv` converts every `*.json` file in a directory into one CSV file and exits without running a
benchmark:

```bash
bin/benchmark --csv results/
```

The output is `results-<epoch seconds>.csv` in the current directory, with one row per result:

```text
topics,partitions,message-size,producers-per-topic,consumers-per-topic,prod-rate-min,prod-rate-avg,prod-rate-std-dev,prod-rate-max,con-rate-min,con-rate-avg,con-rate-std-dev,con-rate-max,validity
```

The CSV contains rates and the validity `status`, empty for files older than schema version 3. For
latency, read the JSON files directly.

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
