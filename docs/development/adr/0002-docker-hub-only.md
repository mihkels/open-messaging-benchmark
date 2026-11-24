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

# ADR 0002: Publish images and Helm charts only to Docker Hub

- **Status:** Accepted, implemented
- **Date:** 2026-11-03
- **Issue:** [#24](https://github.com/mihkels/open-messaging-benchmark/issues/24), implemented in [#29](https://github.com/mihkels/open-messaging-benchmark/issues/29)
- **Scope:** `docker`, `deployment`, `.github/workflows/docker.yml`, `helm.yml`, `workflow-lint.yml`

## Context

Until [#24](https://github.com/mihkels/open-messaging-benchmark/issues/24) the fork published no image or Helm chart. The chart's default image,
`openmessaging/openmessaging-benchmark:latest`, was the upstream project's and was not maintained here,
and images were pushed by hand with `local-build.sh`. Users needed one maintained place to pull
both the image and the chart from, and the repository needed a rule that keeps it that way.

## Decision drivers

- No hosting cost for an open source project.
- One registry for images and the OCI Helm chart, so the docs, the chart defaults and the release
  workflow name the same place.
- Publishing from CI only, with credentials scoped to a protected environment.
- A check that fails when a reference to another registry is added.

## Options considered

- **Docker Hub (`docker.io/mihkels`).** Chosen, because it is free for open source projects.
- **GitHub Container Registry, Google Artifact Registry or Container Registry.** Rejected in favour
  of Docker Hub. The cost reason was confirmed by the maintainer when this ADR was written.

## Decision

Container images and Helm charts are published only to Docker Hub under `docker.io/mihkels`. The
repository does not reference the GitHub or Google container registries anywhere.

## Consequences

- Publishing needs Docker Hub repositories, a Read & Write access token and a `dockerhub` GitHub
  environment; see [Releasing](../releasing.md). Pull requests only build, smoke-test and scan
  images.
- The Helm chart is published as an OCI artifact to the same Docker Hub namespace.
- **Enforced by:** the "No other container registries" step in `.github/workflows/workflow-lint.yml`,
  which fails on a GitHub or Google container registry hostname anywhere in the repository, this
  page included. The rule is also in
  the "Hard constraints" section of `AGENTS.md`.

