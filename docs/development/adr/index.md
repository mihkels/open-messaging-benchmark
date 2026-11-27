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

# Design decisions

An architecture decision record (ADR) states one decision, the reason behind it and what it costs.
Read the ADR before changing what it decides. If you change it anyway, write a new ADR that
supersedes the old one.

|                      ADR                      |                                     Decision                                      |        Status         |
|-----------------------------------------------|-----------------------------------------------------------------------------------|-----------------------|
| [0001](0001-partition-telemetry.md)           | Per-partition skew telemetry and consumer rebalance events                        | Accepted, implemented |
| [0002](0002-docker-hub-only.md)               | Publish images and Helm charts only to Docker Hub                                 | Accepted, implemented |
| [0003](0003-zensical-docs.md)                 | Build the documentation site with Zensical                                        | Accepted, implemented |
| [0004](0004-driver-plugin-bundles.md)         | Run drivers as plugin bundles checked by `DriverApiVersion`                       | Accepted, implemented |
| [0005](0005-frozen-legacy-clients.md)         | Freeze legacy driver clients and pin their transitive libraries                   | Accepted, implemented |
| [0006](0006-remove-deploy-assets.md)          | Remove deployment Terraform and Ansible from this repository                      | Accepted, implemented |
| [0007](0007-static-analysis-gates.md)         | Static analysis fails the build                                                   | Accepted, implemented |
| [0008](0008-e2e-test-suites.md)               | Test drivers with Testcontainers contracts and a Compose regression suite         | Accepted, implemented |
| [0009](0009-schedule-corrected-latency.md)    | Record publish latency corrected for schedule lag                                 | Accepted, implemented |
| [0010](0010-run-validity-verdict.md)          | Give every run one validity verdict                                               | Accepted, implemented |
| [0011](0011-versioned-result-format.md)       | Version the result format and change it only by addition                          | Accepted, implemented |
| [0012](0012-worker-api-secure-by-default.md)  | Secure the worker HTTP API by default                                             | Accepted, implemented |
| [0013](0013-project-owned-namespace.md)       | Move to the `io.github.mihkels.omb` namespace and publish only `driver-api`       | Accepted, implemented |
| [0014](0014-strict-yaml-validation.md)        | Validate workload and driver YAML against generated JSON Schemas                  | Accepted, implemented |
| [0015](0015-images-on-release-tags.md)        | Publish container images only on release tags                                     | Accepted, implemented |
| [0016](0016-latest-only-tarball.md)           | Keep the distribution tarball only on the latest release                          | Accepted, implemented |
| [0017](0017-corretto-runtime-image.md)        | Run the image on Amazon Corretto                                                  | Accepted, implemented |
| [0018](0018-supply-chain-attestations.md)     | Attach SBOM and SLSA provenance to images and provenance to the chart and tarball | Accepted, implemented |
| [0019](0019-accepted-issue-gate.md)           | Close outside pull requests that do not link an accepted issue                    | Accepted, implemented |
| [0020](0020-java-html-report.md)              | Render reports as interactive HTML from Java                                      | Accepted, implemented |
| [0021](0021-rust-worker-behind-worker-api.md) | Add an optional Rust worker behind the existing worker API                        | Accepted              |
| [0022](0022-invalid-on-schedule-lag.md)       | Mark runs that fall behind their schedule as invalid                              | Accepted, implemented |

ADRs 0002 to 0020 were written after the fact from the issues, pull requests and git history named
in each record. Where no reason was written down at the time, the ADR says so.

## When to write one

See "Design decisions" under
[Pull requests](https://github.com/mihkels/open-messaging-benchmark/blob/main/CONTRIBUTING.md#pull-requests)
in `CONTRIBUTING.md` for the triggers.

## How to write one

1. Copy
   [`_template.md`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docs/development/adr/_template.md)
   to `NNNN-short-title.md` with the next free number.
2. Use the short form for most decisions. Use the long form of
   [ADR 0001](0001-partition-telemetry.md) for changes to the `driver-api` contract, the wire format
   or the result format.
3. Add the ADR to the table above and to the "Design decisions" `nav` in `mkdocs.yml`.
4. Set the status to Proposed while the pull request is open and Accepted when it merges.

## Statuses

- **Proposed:** under discussion in an open pull request.
- **Accepted:** in force. Add "implemented" once the code matches.
- **Superseded by NNNN:** replaced by a later ADR, which links back.
- **Deprecated:** no longer applies, and nothing replaces it.

Do not edit an accepted ADR except to change its status. A changed decision gets a new ADR.
