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

# Comparing runs

`bin/benchmark-report diff` compares baseline result files with candidate result files, for
example before and after a broker upgrade, a driver configuration change or a driver change. It
prints one row per metric with both values, the absolute and relative delta and a status, and can
fail a CI job when a metric regresses.

## Comparing two runs

Build the project once, then pass the baseline file first and the candidate file second:

```bash
mvn -DskipTests install
bin/benchmark-report diff baseline.json candidate.json
```

The binary distribution ships the same script in `bin/`.

The report starts with a header:

- the workload name and the files of each side;
- whether latencies come from raw histograms or the aggregated quantiles (see below);
- a count of rows per status;
- a table of the settings that differ between the sides. These are the driver, the worker count,
  the JDK, the OMB version, the git commit and each key of the driver configuration, from the run
  metadata.

Each table row compares one metric:

|                    Metric                    | Unit  | Better |
|----------------------------------------------|-------|--------|
| Publish rate, consume rate                   | msg/s | higher |
| Publish latency p50, p95, p99, p99.9, max    | ms    | lower  |
| Publish delay p50, p95, p99, p99.9, max      | ms    | lower  |
| End-to-end latency p50, p95, p99, p99.9, max | ms    | lower  |
| Publish errors                               | count | lower  |
| Max backlog                                  | count | lower  |

The status of a row is one of:

- `improved` or `regressed`: the median changed by at least `--min-change` (default `1%`) in the
  better or worse direction.
- `unchanged`: the change is smaller than `--min-change`.
- `within noise`: both sides have repeated runs and their ranges overlap; see below.
- `n/a`: a side does not have the metric. For example, version 1 result files have no error count.

A change from a zero baseline, such as new publish errors, has no relative delta. It is reported as
`regressed`.

## Repeated runs and noise

A single run cannot tell a real change from run-to-run variation. Repeat each setup, ideally at
least three times, and pass all files of each side:

```bash
bin/benchmark-report diff \
  --baseline baseline-1.json baseline-2.json baseline-3.json \
  --candidate candidate-1.json candidate-2.json candidate-3.json
```

Each run contributes one value per metric. The report shows each side as
`median [min – max]` and computes deltas from the medians. When both sides have at least two runs
and their `[min, max]` ranges overlap, the row is marked `within noise` instead of `improved` or
`regressed`, and `--fail-on-regression` ignores it.

The run is the unit of replication. The tool deliberately applies no significance test, such as
Mann-Whitney, to the latency samples inside the histograms. Samples within one run are
autocorrelated, and with millions of them every difference looks significant. See
[reproducibility](../methodology/reproducibility.md) for how to make runs repeatable.

## Latency source

When every file on both sides has raw histograms, written by
`bin/benchmark --include-histograms`, the latency percentiles are read from the histograms.
Otherwise every file is read from its aggregated quantile fields. A mix of both is never compared,
and the header names the source that was used.

## Mismatched workloads

Runs of different workloads are not comparable. The tool refuses with exit status `1` when the
workload name, `producerRate`, `messageSize` or `partitionsPerTopic` differ between any two files,
on either side, and names the files with each value. Pass `--allow-workload-mismatch` to compare
anyway; the differences are then printed as warnings in the report. Version 1 result files carry no
workload definition, so only their name, message size and partition count are checked.

## Output formats

|    `--format`    |                                    Use                                    |
|------------------|---------------------------------------------------------------------------|
| `text` (default) | Aligned columns for a terminal                                            |
| `markdown`       | A GitHub table, ready to append to `$GITHUB_STEP_SUMMARY` or a PR comment |
| `json`           | Machine-readable rows plus each side in Bencher Metric Format             |

`--output <file>` writes the report to a file instead of stdout.

The JSON output has a `schemaVersion`, the files, the `settingDifferences`, the `warnings` and one
entry in `rows` per metric. Each row has the `baseline` and `candidate` spreads (`median`, `min`,
`max`, `runs`), the `delta`, the `deltaPercent` and the `status`. Unavailable values are `null`.
The `baseline` and `candidate` objects hold each side in
[Bencher Metric Format](https://bencher.dev/docs/reference/bencher-metric-format/), keyed by
workload and metric id, with the median as `value` and the range as `lower_value` and `upper_value`.
A benchmark tracker can ingest them directly:

```json
{
  "candidate": {
    "kafka-1-topic": {
      "publish-latency-p99": { "value": 6.5, "lower_value": 6.4, "upper_value": 6.6 }
    }
  }
}
```

## Exit status

| Status |                                     Meaning                                     |
|--------|---------------------------------------------------------------------------------|
| `0`    | The comparison was printed, and no metric regressed past `--fail-on-regression` |
| `1`    | Bad arguments, an unreadable file or a workload mismatch                        |
| `2`    | At least one metric regressed by `--fail-on-regression` or more                 |

`--fail-on-regression 10%` (or `10`) makes the tool exit with status `2` when any `regressed` row
changed by at least 10%. A regression from a zero baseline always counts. The report is written
first, and the failing metrics are listed on stderr. This matches the threshold exit status of
`bin/benchmark` described in [CI performance gates](ci-gating.md).

## GitHub Actions example

This workflow runs the benchmark three times on every push to `main` and on every pull request, and
keeps the result files as an artifact. On pull requests it compares the results with those of the
latest successful `main` run, appends the table to the job summary and fails the job on a regression
of 10% or more. Broker setup is left out.

```yaml
name: performance

on:
  push:
    branches: [main]
  pull_request:

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "27"
      - run: mvn -B -DskipTests install

      # Start the broker under test here

      - name: Run the benchmark three times
        run: |
          for i in 1 2 3; do
            bin/benchmark --drivers driver-kafka/kafka-latency.yaml \
              --output "result-$i.json" ci-workload.yaml
          done

      - uses: actions/upload-artifact@v4
        with:
          name: benchmark-results
          path: result-*.json

      - name: Download the baseline results of main
        if: github.event_name == 'pull_request'
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          run_id=$(gh run list --workflow "${{ github.workflow }}" --branch main --status success \
            --limit 1 --json databaseId --jq '.[0].databaseId')
          gh run download "$run_id" --name benchmark-results --dir baseline

      - name: Compare with main
        if: github.event_name == 'pull_request'
        run: |
          bin/benchmark-report diff \
            --baseline baseline/result-*.json \
            --candidate result-*.json \
            --format markdown --fail-on-regression 10% >> "$GITHUB_STEP_SUMMARY"
```

> **Note:** Shared CI runners vary in CPU and network performance from job to job. Prefer
> dedicated runners for benchmark gates, and see [fair comparisons](../methodology/fair-comparisons.md).

Overlaid charts of the compared runs are not part of this command.
