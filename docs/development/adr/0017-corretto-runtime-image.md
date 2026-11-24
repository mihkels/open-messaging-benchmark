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

# ADR 0017: Run the image on Amazon Corretto

- **Status:** Accepted, implemented
- **Date:** 2026-11-19
- **Issue:** [#482](https://github.com/mihkels/open-messaging-benchmark/issues/482), implemented in
  [#483](https://github.com/mihkels/open-messaging-benchmark/pull/483)
- **Scope:** `docker/Dockerfile`, the e2e runner and Testcontainers images

## Context

The runtime image `eclipse-temurin:27-jre`, on Ubuntu 26.04, was flagged by vulnerability scanners
almost every week. A Trivy scan on 2026-11-19 found 72 CVEs in it, 46 without an Ubuntu fix. The
Amazon Corretto 27 image on Amazon Linux 2023 had none.

## Decision drivers

- Few unfixed CVEs in the shipped image.
- glibc, because Netty's native epoll transport ships for glibc and a fallback to NIO would skew
  benchmark results.
- bash and a package manager for interactive use, amd64 and arm64, an official image pinned by
  digest and updated by Dependabot.

## Options considered

- **Alpine (musl).** Rejected for benchmark fidelity.
- **Docker Hardened Images.** No Temurin 27 JRE with a shell.
- **Chainguard free tier.** Only a mutable `latest` tag.
- **Amazon Corretto on Amazon Linux 2023.** Chosen.

## Decision

The shipped image stages, the e2e runner and the Testcontainers image use Amazon Corretto 27, pinned
by digest and upgraded with `dnf` at build time. The Maven build stage stays on the Temurin Maven
image, because it is not shipped.

## Consequences

- The image adds `shadow-utils`, `procps-ng`, `tar` and `gzip`, which the minimal base lacks.
- A future base image change must keep glibc and both architectures.
- **Enforced by:** `docker/Dockerfile`, `e2e-tests/compose/runner/Dockerfile` and the Trivy scans in
  `.github/workflows/docker.yml`.

