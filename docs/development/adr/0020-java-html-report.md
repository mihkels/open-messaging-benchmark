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

# ADR 0020: Render reports as interactive HTML from Java

- **Status:** Accepted, implemented
- **Date:** 2026-11-10
- **Issue:** [#44](https://github.com/mihkels/open-messaging-benchmark/issues/44), implemented in
  [#334](https://github.com/mihkels/open-messaging-benchmark/pull/334)
- **Scope:** `benchmark-framework` (`HtmlReport`), `tool` (`benchmark-report html`), `bin/benchmark`

## Context

Charts came from `create_charts.py`, which needed Python and pygal and produced static SVG files. It
could not overlay runs, zoom or show the full latency tail, and a normal Java run produced no report.

## Decision drivers

- A report straight from the benchmark run, with no second toolchain.
- Overlay several runs and show the latency tail.

## Options considered

- **uPlot, inlined into the page so it works offline.** Preferred in the issue.
- **Apache ECharts loaded from a CDN, pinned with Subresource Integrity.** Chosen at the
  maintainer's request. No reason was recorded for preferring it.
- **Keep `create_charts.py` for one release.** Rejected; it was removed in the same change.

## Decision

Reports are rendered in Java by `HtmlReport`, through `bin/benchmark-report html` and
`bin/benchmark --report-html`. The result JSON is embedded in the page and the chart library is
loaded from a pinned CDN URL with an integrity hash.

## Consequences

- Python is no longer needed for reports.
- Opening a report needs network access to load the chart library. This differs from the issue's
  acceptance criterion; the page shows a message when the library cannot load.
- `HtmlReport` lives in the framework's root package so the ArchUnit no-cycles rule holds.
- **Enforced by:** `HtmlReportTest`, `BenchmarkReportTest` and the ArchUnit rules. Usage is in
  [HTML report](../../guides/html-report.md).

