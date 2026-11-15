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
combination overwrites the same file. With `--repeat`, each repetition adds its number before the
extension and the summary adds `-summary`; see [Repeated runs](#repeated-runs).

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

|        Check         |               Severity                |                                                            Finds                                                             |
|----------------------|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `rateShortfall`      | invalid                               | Average publish rate below a fixed `producerRate` by more than the limit; value is the shortfall fraction                    |
| `backlogGrowth`      | warning                               | Backlog growth over the measured window, in seconds of publishing; skipped for backlog and pause workloads                   |
| `shortWindow`        | warning                               | Measured window shorter than the minimum, in minutes                                                                         |
| `tailSamples`        | warning, or info for p99.99 only      | Too few messages for the reported tail percentiles; value is the samples, limit what the lowest unreliable percentile needs  |
| `sendErrors`         | warning                               | Failed sends in the measured window, with the last reason reported by each worker                                            |
| `publishDelayGrowth` | warning                               | Rise in average publish delay from the first to the second half of the measured window, in ms                                |
| `retried`            | warning                               | The run needed more than one [`--retries`](../reference/cli.md) attempt; the message lists the earlier failures              |
| `clockOffset`        | warning                               | End-to-end latency is unreliable: producer and consumer worker clocks differ by more than a fraction of the p50, in ms       |
| `runToRunVariance`   | warning                               | In a [repetition summary](#repeated-runs) only: a metric whose coefficient of variation across repetitions exceeds the limit |
| `failedRepetitions`  | warning, or invalid if none succeeded | In a [repetition summary](#repeated-runs) only: repetitions that failed after their retries or were skipped                  |

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

## Repeated runs

`--repeat N` (or `repeat: N` in the workload) runs each combination N times and writes one result
per repetition plus a summary of the spread:

|        Without `--repeat`         |                                     With `--repeat 3`                                      |
|-----------------------------------|--------------------------------------------------------------------------------------------|
| `-o result.json`: `result.json`   | `result-1.json` … `result-3.json`, `result-summary.json`                                   |
| `<workload>-<driver>-<time>.json` | `<workload>-<driver>-<time>-1.json` … `-3.json`, `<workload>-<driver>-<time>-summary.json` |

The summary has this shape:

```json
{
  "workload": "1-topic-1-partition-1kb",
  "driver": "Kafka",
  "repetitions": 3,
  "succeeded": 2,
  "results": [
    {"repetition": 1, "succeeded": true, "file": "result-1.json", "validity": "valid"},
    {"repetition": 2, "succeeded": false, "failure": "java.io.IOException: ..."},
    {"repetition": 3, "succeeded": true, "file": "result-3.json", "validity": "valid"}
  ],
  "metrics": {
    "publishRateAvg": {"median": 9998.1, "min": 9990.2, "max": 10006.0, "cv": 0.0011},
    "aggregatedPublishLatency99pct": {"median": 12.4, "min": 11.9, "max": 12.9, "cv": 0.057}
  },
  "thresholds": {"checks": {"maxPublishLatency99Ms": {"limit": 25.0, "observed": 12.4, "status": "PASS"}}, "status": "PASS"},
  "findings": [
    {"check": "failedRepetitions", "severity": "warning", "message": "...", "value": 1.0, "limit": 0.0}
  ],
  "validity": {"status": "warning", "findings": [ ... ]}
}
```

|     Field     |                                                                                                                                  Meaning                                                                                                                                   |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `repetitions` | Repetitions requested                                                                                                                                                                                                                                                      |
| `succeeded`   | Repetitions that produced a result after their retries; fewer than `repetitions` when one failed or the rest were skipped because a worker could not be reused                                                                                                             |
| `results`     | One entry per repetition that ran: its number, whether it `succeeded`, its result `file` (relative to the summary) and `validity` status, or the `failure` that ended it                                                                                                   |
| `metrics`     | `median`, `min`, `max` and `cv` over the successful repetitions of `publishRateAvg`, `consumeRateAvg`, `throughputMbSec` and the p50, p99 and p99.9 publish and end-to-end latencies (ms), named as in the result file. Absent when none succeeded                         |
| `cv`          | Coefficient of variation: sample standard deviation divided by the mean. Absent with fewer than two successful repetitions or a mean of zero                                                                                                                               |
| `thresholds`  | The workload thresholds evaluated against the medians; they decide the exit status instead of the per-repetition results                                                                                                                                                   |
| `findings`    | The summary's own findings: a `runToRunVariance` warning for each metric whose `cv` exceeds `checks.maxRunToRunCv` (default `0.1`), and a `failedRepetitions` finding when a repetition failed or was skipped (`invalid` when none succeeded); omitted when there are none |
| `validity`    | The summary verdict, worst-of: the summary's own findings plus every finding of each successful repetition, whose messages start with `repetition <n>:`. The same rule as a result's [validity](#validity-verdict)                                                         |

Each repetition's own result file still has its own `findings`, `validity` and `thresholds`.
`--fail-on-invalid` and `--fail-on-warning` judge every repetition's result and the summary; as the
summary verdict is never better than any repetition's, a `runToRunVariance` warning alone fails a
run with `--fail-on-warning`. See
[Reproducibility](../methodology/reproducibility.md#repeat-runs) for when to repeat.

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
