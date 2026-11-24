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

# ADR 0011: Version the result format and change it only by addition

- **Status:** Accepted, implemented
- **Date:** 2026-11-06
- **Issue:** [#43](https://github.com/mihkels/open-messaging-benchmark/issues/43), implemented in
  [#124](https://github.com/mihkels/open-messaging-benchmark/pull/124)
- **Scope:** `benchmark-framework` (`TestResult`), `docs/reference/results-schema.json`, every
  reader of result files (`tool`, CSV export, HTML report)

## Context

Weeks after a run nobody could tell which driver settings, JVM, worker count or code version had
produced a result file. Every consumer, such as charts, run comparison and CI gates, had to
recompute the headline numbers itself, and nothing could merge runs or recompute a percentile
exactly.

## Decision drivers

- A result file explains itself: what ran, where and with which code.
- Existing readers keep working when the format grows.
- Exact recomputation is possible without making every file large.

## Options considered

- **Change existing fields in place.** Rejected: old readers would break.
- **Always embed raw histograms.** Rejected because of file size; they are opt-in.
- **Add new top-level blocks behind a schema version.** Chosen.

## Decision

Result files carry a `schemaVersion`, a `metadata` block (run ID, timestamps, OMB version and git
commit, JDK and JVM options, worker count, the resolved workload, and the driver configuration with
secrets redacted) and a `summary` of the measured window after warm-up. `--include-histograms`
adds the raw HdrHistograms. The format changes only by addition: existing fields keep their names,
units and meanings, readers must keep reading older versions, and the version is bumped once per
change set rather than once per field.

## Consequences

- Files from every earlier version still load; readers treat missing blocks as absent.
- Exact merging and percentile recomputation need files written with `--include-histograms`.
- `docs/reference/results-schema.json` is written by hand and rejects unknown nested fields, so every
  new field needs a schema update in the same change.
- The `tool` JSON report has its own, separate schema version.
- **Enforced by:** `ResultsSchemaTest`, which validates a golden result against
  `results-schema.json`, and `TestResultJsonTest`, which reads version 1 and 2 files and round-trips
  the current one. The rules are in [Results schema](../../reference/results-schema.md).

