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

# ADR 0006: Remove deployment Terraform and Ansible from this repository

- **Status:** Accepted, implemented
- **Date:** 2026-11-18
- **Issue:** [#114](https://github.com/mihkels/open-messaging-benchmark/issues/114), implemented in [#448](https://github.com/mihkels/open-messaging-benchmark/issues/448); supersedes [#104](https://github.com/mihkels/open-messaging-benchmark/issues/104)
- **Scope:** the former `deploy/` directories of the drivers, `package` assembly descriptors

## Context

Each legacy driver carried a `deploy/` directory with Terraform and Ansible. The assets were
untested, used Java 11 and 17 era settings and the 0.0.1 tarball, and suggested to users that they
worked with this fork. A security audit found a hardcoded root password, world-open SSH, and
Prometheus and Grafana with default credentials in them.

## Decision drivers

- Ship nothing that looks supported but is not.
- Remove the audit findings instead of carrying them as known issues.
- Leave room for a redesigned deployment approach.

## Options considered

- **Keep the assets with disclaimer READMEs** ([#104](https://github.com/mihkels/open-messaging-benchmark/issues/104)). Rejected: the assets stay
  misleading and the findings stay open.
- **Patch the assets.** Rejected: the deployment approach is being redesigned, so patching would be
  wasted work.
- **Remove them.** Chosen.

## Decision

The legacy `deploy/` directories, with their Terraform and Ansible, are removed. The supported way
to run the benchmark is the Docker images and the Helm chart
([ADR 0002](0002-docker-hub-only.md)).

## Consequences

- Breaking change: the distribution tarball and the source tree no longer ship deploy assets.
- The audit findings in those files are closed by removal.
- Where new cloud deployment code lives is not settled. [#448](https://github.com/mihkels/open-messaging-benchmark/issues/448) says the Terraform setup
  will move to a separate repository; [#115](https://github.com/mihkels/open-messaging-benchmark/issues/115), still open, proposes it under
  `deployment/cloud/terraform/` in this repository. The ADR for [#115](https://github.com/mihkels/open-messaging-benchmark/issues/115) has to resolve this.
- **Enforced by:** review only. `AGENTS.md` tells agents to ask before auditing such files if they
  reappear.

