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

# ADR 0018: Attach SBOM and SLSA provenance to every release artifact

- **Status:** Accepted, implemented
- **Date:** 2026-11-18
- **Issue:** [#231](https://github.com/mihkels/open-messaging-benchmark/issues/231), implemented in
  [#453](https://github.com/mihkels/open-messaging-benchmark/pull/453)
- **Scope:** `.github/workflows/docker.yml`, `.github/workflows/helm.yml`, images, the Helm chart and
  the tarball

## Context

Images and the chart were already signed with cosign, but users could not audit the dependencies
bundled in them (15 driver client stacks) or verify that an artifact was built from a given commit.

## Decision drivers

- Users can list what an artifact contains and verify where it was built.
- Keyless signing, with nothing to rotate.

## Options considered

- **BuildKit SBOM and provenance attestations alone.** Not enough: cosign cannot read attestations
  stored in the image index.
- **syft plus `cosign attest`.** Not used; BuildKit's own SBOM and provenance are extracted and
  re-attached with keyless `cosign attest` instead.

## Decision

Every released image gets an SPDX SBOM and a SLSA v1 provenance attestation through keyless
`cosign attest`. The Helm chart and the tarball get build provenance through GitHub artifact
attestations, verifiable with `gh attestation verify`.

## Consequences

- Verification uses the SLSA v1 predicate type (`slsaprovenance1`), not the older one named in the
  issue.
- The SBOM of the image with all drivers is about 15 MB per platform and may hit transparency log
  size limits.
- **Enforced by:** the attestation steps in `.github/workflows/docker.yml` and `helm.yml`.
  Verification commands are in [Releasing](../releasing.md) and `SECURITY.md`.

