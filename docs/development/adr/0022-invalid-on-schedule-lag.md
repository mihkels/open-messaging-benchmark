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

# ADR 0022: Mark runs that fall behind their schedule as invalid

- **Status:** Accepted, implemented
- **Date:** 2026-11-26
- **Issue:** [#598](https://github.com/mihkels/open-messaging-benchmark/issues/598), found while
  measuring harness overhead for [#590](https://github.com/mihkels/open-messaging-benchmark/issues/590)
- **Scope:** `benchmark-framework` (`RunChecks`), workload `checks` block, `performance-harness`
  validity. Extends
  [ADR 0010](0010-run-validity-verdict.md) and builds on [ADR 0009](0009-schedule-corrected-latency.md).

## Context

A saturated run came out `valid`. With the synthetic driver in sync mode, 128 B payloads, 4
producers and a target of 1.6M msg/s, producers fell seconds behind their schedule and then replayed
the late sends as a burst. The average publish rate reached 1.68M msg/s, so `rateShortfall` did not
fire, and `publishDelayGrowth` compares the averages of the two halves of the window, so a lag that
rises and falls passes it too. The publish delay (schedule lag) p99 was 6.0 s; healthy runs show
under 1 ms. A run at a worker's ceiling is exactly where people read benchmark limits, so `valid`
must not be shown there.

## Decision drivers

- Catch a run whose sends started late, whether or not it caught up later.
- Do not fail healthy runs, including short pauses such as a GC pause.
- No new result fields: the verdict already carries findings
  ([ADR 0011](0011-versioned-result-format.md)).

## Options considered

- **Backlog of late sends per period.** Workers do not report it; adding it changes the worker
  period stats and the result format.
- **Lower `maxPublishDelayGrowthMs` or make it invalid.** Still misses a lag that rises and falls,
  and a steady lag from the start.
- **Limit the p99 of the measured-window publish delay.** Uses the existing aggregate histogram, and
  a p99 ignores pauses that cover less than 1% of the window. Chosen.

## Decision

The `publishDelay` check reports an `invalid` finding when the aggregate publish delay p99 of the
measured window exceeds `checks.maxPublishDelayMs`, 100 ms by default. It runs for fixed-rate and
staged workloads and is skipped for rate discovery (`producerRate: 0` without `stages`; staged workloads also set
`producerRate: 0` but are checked). Workloads can raise the limit
or disable the check like any other. The framework performance harness applies the same 100 ms
limit to its own run validity, since its verdict is computed separately.

## Consequences

- Runs that were `valid` before can now be `invalid`; results from before the change are not
  re-judged. The default sits two orders of magnitude above the healthy runs measured for #590.
- A run that meets its target rate only by replaying a backlog no longer counts as a measurement of
  that rate; its reported latency is subject to coordinated omission (ADR 0009).
- `publishDelayGrowth` stays as an earlier warning for lag that is still rising.
- **Enforced by:** `RunChecksTest`, `YamlSchemaTest` for the new limit in the workload schema, and
  `performance-harness/tests/test_harness.py`.

