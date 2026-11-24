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

# ADR 0004: Run drivers as plugin bundles checked by DriverApiVersion

- **Status:** Accepted, implemented
- **Date:** 2026-10-31
- **Issue:** introduced in [#12](https://github.com/mihkels/open-messaging-benchmark/issues/12); version 2 in [#108](https://github.com/mihkels/open-messaging-benchmark/issues/108) and [#155](https://github.com/mihkels/open-messaging-benchmark/issues/155)
- **Scope:** `driver-api`, `benchmark-framework` (`plugin` package), every `driver-*` module,
  external plugins

## Context

Drivers used to share one classpath with each other and with the framework, so upgrading one
broker's client library could conflict with another driver or with the framework itself. External
drivers had no supported way to ship.

## Decision drivers

- Upgrade each messaging client independently.
- Let drivers built outside this repository load without patching the framework.
- Reject an incompatible bundle up front, not with a linkage error inside the driver JVM.

## Options considered

- **One shared classpath.** Simple, but every client upgrade risks a conflict across drivers.
- **Isolated plugin bundles with a declared API version.** Chosen.
- **A version range instead of an exact match** (`MIN_SUPPORTED <= driverApiVersion <= CURRENT`).
  Proposed in [#220](https://github.com/mihkels/open-messaging-benchmark/issues/220) and not implemented yet.

## Decision

Each driver is a plugin bundle with a `plugin.yaml` that declares `driverApiVersion`, and runs in
its own JVM with its own classpath. The framework loads a bundle only when its `driverApiVersion`
equals `DriverApiVersion.CURRENT`. A change that breaks binary compatibility of `driver-api` bumps
`CURRENT`; additive changes, such as new `default` methods, keep it. Bumps happen before 1.0 while
the cost is lowest ([#108](https://github.com/mihkels/open-messaging-benchmark/issues/108)).

## Consequences

- Driver dependencies no longer conflict with each other or with the framework.
- A bump rejects every older bundle, including ones that would work after a rebuild: the move to
  version 2 stopped every external plugin built for version 1 from loading. [#220](https://github.com/mihkels/open-messaging-benchmark/issues/220) tracks
  softening this with a range.
- Additive features, such as message headers and [partition telemetry](0001-partition-telemetry.md),
  arrive as `default` methods so version 2 bundles keep loading.
- Whether a change is binary compatible is decided in review today. [#568](https://github.com/mihkels/open-messaging-benchmark/issues/568) adds a japicmp
  check once `driver-api` is on Maven Central.
- **Enforced by:** `PluginDescriptor.validate()` in `benchmark-framework`, with `PluginDescriptorTest`
  and `PluginRegistryTest`; the rules are in
  [Packaging a plugin](../packaging-a-plugin.md#compatibility-rules).

