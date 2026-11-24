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

# ADR 0007: Static analysis fails the build

- **Status:** Accepted, implemented
- **Date:** 2026-11-08
- **Issue:** epic [#182](https://github.com/mihkels/open-messaging-benchmark/issues/182) with [#177](https://github.com/mihkels/open-messaging-benchmark/issues/177), [#178](https://github.com/mihkels/open-messaging-benchmark/issues/178), [#179](https://github.com/mihkels/open-messaging-benchmark/issues/179) and
  [#181](https://github.com/mihkels/open-messaging-benchmark/issues/181); coverage floors in [#46](https://github.com/mihkels/open-messaging-benchmark/issues/46); NullAway in every module in [#484](https://github.com/mihkels/open-messaging-benchmark/issues/484)
- **Scope:** root `pom.xml`, `.mvn/jvm.config`, every module

## Context

Checkstyle, SpotBugs, JaCoCo and Enforcer came from upstream, with the JaCoCo minimum at zero.
Coverage was measured but never enforced, no tool enforced module boundaries, and the git history
had seven NullPointerException fixes.

## Decision drivers

- Get rid of NullPointerExceptions.
- Catch reproducibility problems and module boundary violations in the build, not in review.
- Avoid tools that overlap with what already runs.

## Options considered

- **Report only, through SonarCloud and CI logs.** Findings pile up and are easy to ignore.
- **PMD.** Not added: it overlaps heavily with SonarCloud and Checkstyle.
- **OWASP Dependency-Check.** Not added: Dependabot and Trivy already cover it.
- **Error Prone and NullAway.** First deferred in [#181](https://github.com/mihkels/open-messaging-benchmark/issues/181) as costly and partly covered by
  SpotBugs, then adopted to remove NullPointerExceptions. SpotBugs stays, because the two complement
  each other.

## Decision

`mvn verify` fails on findings from Enforcer, Spotless, the license check, Checkstyle, Error Prone
with NullAway in JSpecify mode, SpotBugs, the ArchUnit tests and the JaCoCo floors. Each tool was
checked on the project JDK before it became required ([#177](https://github.com/mihkels/open-messaging-benchmark/issues/177)). Error Prone checks were promoted
to errors one by one after measuring their findings. Coverage floors sit about two points below the
measured coverage of each module and only move up. Findings are fixed, not suppressed.

## Consequences

- Building needs the `jdk.compiler` exports in the committed `.mvn/jvm.config`.
- Tests are excluded from NullAway because they pass `null` on purpose.
- `requireUpperBoundDeps` applies only to host modules, because driver isolation allows different
  client versions ([ADR 0004](0004-driver-plugin-bundles.md)).
- With NullAway in every module, missing driver settings fail in `initialize()` instead of later.
- **Enforced by:** the root `pom.xml` (`errorprone.args`, Checkstyle, the Enforcer
  `enforce-build-rules` execution, JaCoCo `haltOnFailure` with the `jacoco.min.*` floors) and
  `.github/workflows/pr-build-and-test.yml`.

