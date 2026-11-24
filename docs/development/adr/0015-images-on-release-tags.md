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

# ADR 0015: Publish container images only on release tags

- **Status:** Accepted, implemented
- **Date:** 2026-11-08
- **Issue:** [#215](https://github.com/mihkels/open-messaging-benchmark/issues/215), implemented in
  [#242](https://github.com/mihkels/open-messaging-benchmark/pull/242)
- **Scope:** `.github/workflows/docker.yml`, published image tags

## Context

Every merge to `main` built the distribution, checked the image and ran 16 multi-arch publish jobs:
about 23 minutes per run and about 371 runner-minutes in one week. Pull request jobs queued behind
them for 3 to 13 minutes. Nobody needed an `edge` image per merge; images only have to reach
Docker Hub when a version is released.

## Decision drivers

- Free runner capacity for pull requests.
- Keep security scanning of what users actually pull.

## Options considered

No alternative to publishing on tags was recorded. The issue proposed capping the publish jobs at 8
in parallel; the change capped them at 4, which leaves more runner slots for open pull requests.

## Decision

Images are pushed only for `v*.*.*` tags, with semver tags and `latest`. Pull requests build,
smoke-test and scan images without pushing. A weekly scheduled run scans the published `latest`
image and uploads the results to code scanning.

## Consequences

- Breaking: the `edge`, `edge-<driver>` and `sha-<commit>` tags stopped updating. Users pin a
  version or use `latest`.
- Between releases, the scan covers only the published `latest`.
- **Enforced by:** the triggers and the `scan-published` job in `.github/workflows/docker.yml`.

