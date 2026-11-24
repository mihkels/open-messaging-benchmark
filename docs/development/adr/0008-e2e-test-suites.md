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

# ADR 0008: Test drivers with Testcontainers contracts and a Compose regression suite

- **Status:** Accepted, implemented
- **Date:** 2026-11-01
- **Issue:** Compose suite in [#12](https://github.com/mihkels/open-messaging-benchmark/issues/12), contracts for every driver in [#17](https://github.com/mihkels/open-messaging-benchmark/issues/17); CI in
  [#106](https://github.com/mihkels/open-messaging-benchmark/issues/106)
- **Scope:** `e2e-tests`, `e2e-tests/compose`, every `driver-*` module

## Context

Unit tests cannot show that a driver works against its broker. The existing Testcontainers tests
covered only some drivers and used minute-long workloads for basic checks, and nothing exercised the
distributed path from coordinator to workers to driver.

## Decision drivers

- Functional coverage for every shipped driver, fast enough to run while developing.
- A regression check against real brokers through the distributed path before a change merges.

## Options considered

No single-suite alternative was recorded. The two suites answer different questions.

## Decision

Every driver has two end-to-end checks:

- a **Testcontainers contract** in `e2e-tests`: short message exchanges against a broker in a
  container, run with `mvn -pl e2e-tests -am -De2e verify`;
- a **Compose case**: a one-minute regression workload through a coordinator and two workers, run
  with `e2e-tests/compose/run-e2e.sh --build docker --cases <case>` so the distribution is rebuilt
  with the changed driver.

A new driver adds both.

## Consequences

- Both suites are opt-in and need Docker. The full contract matrix takes about 11 minutes, most of it
  build and broker start-up.
- Neither suite runs in CI yet, so a client bump that breaks a driver against its broker can merge
  green ([#106](https://github.com/mihkels/open-messaging-benchmark/issues/106)). Pins in [ADR 0005](0005-frozen-legacy-clients.md) rely on a manual e2e run.
- **Enforced by:** review, through the rule in `CONTRIBUTING.md` and `AGENTS.md` that a new driver
  needs both. See [Testing a driver](../testing-a-driver.md).

