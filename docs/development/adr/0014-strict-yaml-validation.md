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

# ADR 0014: Validate workload and driver YAML against generated JSON Schemas

- **Status:** Accepted, implemented
- **Date:** 2026-11-19
- **Issue:** [#228](https://github.com/mihkels/open-messaging-benchmark/issues/228), implemented in
  [#493](https://github.com/mihkels/open-messaging-benchmark/pull/493)
- **Scope:** `benchmark-framework` (`Workload`, YAML loading), `docs/reference/*-schema.json`, user
  workload and driver files

## Context

Workload and driver files were described only in prose, with no editor completion. A mistyped key
was dropped silently, a missing `testDurationMinutes` ran forever, and a missing `producerRate`
switched to a maximum-rate search without saying so.

## Decision drivers

- Catch mistakes before any worker is contacted.
- One source of truth for the file format, shared by the CLI and editors.

## Options considered

- **Publish schemas for editors only.** The first scope of [#228](https://github.com/mihkels/open-messaging-benchmark/issues/228).
  Widened before implementation to validate at runtime as well.
- **Write the schemas by hand.** Rejected for generating them from the Java classes.

## Decision

The workload and driver JSON Schemas are generated from the Java classes and published with the docs.
Every workload and driver file is validated against them when it loads, before any worker contact:

- Unknown workload keys fail at every level.
- `name` and the topology counts are required, and the counts must be at least 1.
- `producerRate` and `testDurationMinutes` are required unless `stages` is set, and `messageSize`
  unless `messageSizeDistribution` is set.
- Driver files need `name` and `driverClass`; other driver keys are free-form.
- Rules that link fields stay in `Workload.validate()`.

## Consequences

- Breaking for files with stray keys or that relied on missing fields defaulting to 0. All bundled
  workloads and driver files pass.
- The validator stays out of the worker runtime so it cannot clash with driver dependencies.
- A new workload field needs a schema regeneration in the same change.
- **Enforced by:** `YamlSchemaTest`, which fails when the committed schemas are stale (regenerate
  with `-DupdateSchema=true`), and `YamlValidationTest`. The format is in
  [Workload schema](../../reference/workload-schema.md).

