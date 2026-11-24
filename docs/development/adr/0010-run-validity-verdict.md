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

# ADR 0010: Give every run one validity verdict

- **Status:** Accepted, implemented
- **Date:** 2026-11-15
- **Issue:** epic [#237](https://github.com/mihkels/open-messaging-benchmark/issues/237) and
  [#239](https://github.com/mihkels/open-messaging-benchmark/issues/239), implemented in
  [#398](https://github.com/mihkels/open-messaging-benchmark/pull/398); fed by
  [#238](https://github.com/mihkels/open-messaging-benchmark/issues/238),
  [#225](https://github.com/mihkels/open-messaging-benchmark/issues/225),
  [#226](https://github.com/mihkels/open-messaging-benchmark/issues/226),
  [#47](https://github.com/mihkels/open-messaging-benchmark/issues/47) and
  [#224](https://github.com/mihkels/open-messaging-benchmark/issues/224)
- **Scope:** `benchmark-framework` (run checks, result, CLI exit codes), result schema

## Context

A completed run is not automatically a valid measurement. Signals that a measurement cannot be
trusted were arriving as separate features: load-generator saturation, worker clock offset,
run-to-run variance, delivery verification and built-in checks such as a rate shortfall. Users had
to inspect each one, and CI could not gate on "the measurement is trustworthy". SLO thresholds
answer a different question: whether the system met its target.

## Decision drivers

- One place where every check reports, and one answer per run.
- A CI gate that is separate from SLO thresholds, because an untrustworthy measurement makes its
  threshold result meaningless.
- No duplicate reporting of failures that already abort a run, such as stall detection and the
  coordinator watchdog.

## Options considered

- **A warning per feature.** Rejected: saturation and the other checks report through findings
  instead of separate warnings.
- **A schema version bump per feature.** Rejected: the epic bumps the result schema once.
- **One verdict built from findings.** Chosen.

## Decision

Every check emits a finding with a check name, a severity (`info`, `warning` or `invalid`), a
message, a value and a limit. The verdict is `invalid` if any finding is invalid, otherwise `warning`
if any finding is a warning, otherwise `valid`; `info` findings do not count. `--fail-on-invalid`
and `--fail-on-warning` exit with code 3, kept apart from threshold breaches, which exit with 2; when
both apply, 3 wins. Workloads tune or disable checks in their `checks` block.

## Consequences

- The result `schemaVersion` went from 2 to 3 once; version 2 files read back without a verdict
  ([ADR 0011](0011-versioned-result-format.md)).
- `--repeat` summaries carry the worst verdict of their runs, and it is gated the same way.
- Probing worker clocks never fails a run on its own; a large offset is a warning.
- Delivery verification overwrites a payload prefix, so it stays opt-in.
- A new check reports a finding with a severity instead of adding its own flag or warning.
- **Enforced by:** `RunChecksTest`, `BenchmarkOutcomesTest`, `BenchmarkExitCodeTest`,
  `TestResultJsonTest` and `ResultsToCsvTest`. Users see it in [Results](../../guides/results.md),
  [CI performance gates](../../guides/ci-gating.md) and the
  [CLI exit codes](../../reference/cli.md#exit-codes).

