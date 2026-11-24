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

# ADR 0009: Record publish latency corrected for schedule lag

- **Status:** Accepted, implemented
- **Date:** 2026-11-06
- **Issue:** [#32](https://github.com/mihkels/open-messaging-benchmark/issues/32), implemented in
  [#125](https://github.com/mihkels/open-messaging-benchmark/pull/125)
- **Scope:** `benchmark-framework` (worker statistics, result, Prometheus, console log)

## Context

The producer already knew each message's intended send time and recorded two histograms:
`publishLatency`, the service time from send to acknowledgement, and `publishDelayLatency`, the lag
between the intended and the actual send. Near saturation `publishLatency` p99 stays flat while the
real cost moves into the delay histogram. Reports and charts led with `publishLatency`, so
coordinated omission stayed hidden in practice.

## Decision drivers

- Show the latency a client on the intended schedule would see.
- Keep existing fields and their meaning, so old readers keep working.

## Options considered

- **Add the percentiles of the two existing histograms.** Rejected: percentiles of separate
  histograms cannot be added to recover the corrected tail.
- **Record a third histogram per send.** Chosen.

## Decision

For every successful send, record `ackTime − intendedSendTime` into its own histogram,
`publishScheduleLatency`, and report it everywhere `publishLatency` is reported: worker period
stats, per-period and aggregate result percentiles, Prometheus and the console log. New latency
views are added as new fields; existing fields keep their names, units and meanings.

## Consequences

- With no schedule lag the value equals `publishLatency`. A send that starts 100 ms late and
  completes 1 ms later records 1 ms service time, 100 ms lag and 101 ms corrected latency.
- Rate discovery (`producerRate: 0`) can show schedule lag too; see
  [Latency and backlog](../../methodology/latency-and-backlog.md#schedule-corrected-publish-latency).
- Values are capped at 60 s, like the other latency histograms.
- Result files written before the change do not have the field.
- **Enforced by:** `WorkerStatsTest` (fake clock), `PeriodStatsTest`, `CumulativeLatenciesTest`,
  `WorkerHttpRoundTripTest` and the `TestResultJsonTest` golden file.

