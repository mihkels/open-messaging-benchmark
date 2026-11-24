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

# ADR 0016: Keep the distribution tarball only on the latest release

- **Status:** Accepted, implemented
- **Date:** 2026-11-05
- **Issue:** [#60](https://github.com/mihkels/open-messaging-benchmark/issues/60),
  [#68](https://github.com/mihkels/open-messaging-benchmark/issues/68),
  [#470](https://github.com/mihkels/open-messaging-benchmark/issues/470) and
  [#113](https://github.com/mihkels/open-messaging-benchmark/issues/113)
- **Scope:** GitHub Releases, `.github/workflows/docker.yml`, `.github/scripts`, the `package`
  module

## Context

The tarball was about 670 MB. Releases first kept it for the newest ten versions. Separately, the
attach job depended on the image scan, so a base-image CVE left v0.1.1 without a tarball even though
`mvn verify` had checked it.

## Decision drivers

- Keep release storage small.
- A tarball must not depend on an unrelated image scan.
- Shrinking the tarball must not weaken driver isolation
  ([ADR 0004](0004-driver-plugin-bundles.md)).

## Options considered

- **Keep the newest ten tarballs.** Rejected for one.
- **Share one `lib/` across drivers, or ship per-driver assets.** Rejected: a shared `lib/` would
  break isolation.
- **Drop the native TLS libraries.** Rejected: they keep macOS local runs working.
- **Exclude unused native jars and hard-link identical jars.** Chosen; it took the tarball from 688
  to 311 MiB.

## Decision

Only the latest GitHub release carries the tarball and its `SHA256SUMS`. Attaching it needs only
the distribution build, not the image scan. Older versions are available as versioned images or by
building the tag. Every size reduction keeps one `lib/`, one class loader and one child JVM per
driver bundle.

## Consequences

- Older releases keep their notes and tags but no tarball; GitHub's source archives stay.
- Overwriting a hard-linked jar in place changes every bundle that links to it, so replace jars with
  `rm` and copy; see [Packaging a plugin](../packaging-a-plugin.md).
- **Enforced by:** `.github/scripts/prune-release-assets.sh` and `.github/scripts/dedupe-tarball.sh`,
  run from `docker.yml`, and the exclusions in the root `pom.xml`. The policy is in
  [Releasing](../releasing.md) and [Building the project](../building-the-project.md#older-versions).

