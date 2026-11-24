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

# ADR 0013: Move to the io.github.mihkels.omb namespace and publish only driver-api

- **Status:** Accepted, implemented (publishing not yet switched on)
- **Date:** 2026-11-20
- **Issue:** [#112](https://github.com/mihkels/open-messaging-benchmark/issues/112) and
  [#494](https://github.com/mihkels/open-messaging-benchmark/issues/494), implemented in
  [#500](https://github.com/mihkels/open-messaging-benchmark/pull/500)
- **Scope:** every POM and Java package, `.github/workflows/maven-central.yml`, external plugins

## Context

External driver authors had to `mvn install` the whole reactor to compile against `driver-api`.
Publishing it to Maven Central needs a verified namespace. `io.openmessaging` cannot be verified
from this fork, because that needs the upstream domain, and upstream already publishes
`io.openmessaging:*`. Central's free tier limits file count, release size and release count per
month; one release is about 20 files and under 1 MB, so the release count is the only limit within
reach.

## Decision drivers

- A namespace the project can verify and own.
- Stay inside the Central free tier.
- Publish only what external code consumes.

## Options considered

- **A domain the project owns.** Not chosen; `io.github.mihkels` is verified through GitHub.
- **Change the groupId and keep the Java packages.** Acceptable at first and implemented that way,
  then reversed so the packages match the groupId. No further reason was recorded.
- **Publish on every release, or ask Sonatype for higher limits.** Rejected for publishing only when
  `driver-api` changed since the previous tag.

## Decision

The Maven groupId and the Java packages are `io.github.mihkels.omb`, with no compatibility kept
for the old names. Only `driver-api` and the parent POM go to Maven Central. The framework, drivers,
tools and test modules are not published there; the tarball and images ship through GitHub Releases
and Docker Hub ([ADR 0002](0002-docker-hub-only.md)).

## Consequences

- Breaking: Maven coordinates, Java packages and `driverClass` values changed, and external plugins
  must update their imports.
- Distribution jars are named `lib/io.github.mihkels.omb-*.jar`.
- Nothing is published until namespace verification, signing keys and secrets are set up by hand.
- A japicmp check against the published `driver-api` is planned in
  [#568](https://github.com/mihkels/open-messaging-benchmark/issues/568).
- **Enforced by:** the `central` profile in the root `pom.xml` and
  `.github/workflows/maven-central.yml`, which stays inert until `MAVEN_CENTRAL_ENABLED` is set,
  skips tags where `driver-api/` did not change and stops at validation unless
  `MAVEN_CENTRAL_AUTO_PUBLISH` is set. See [Releasing](../releasing.md).

