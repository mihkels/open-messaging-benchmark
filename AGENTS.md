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

# Agent instructions

Rules for any coding agent working in this repository (Claude Code, GitHub Copilot, Cursor,
Antigravity and others). Keep this file tool-neutral: no tool-specific paths or commands, and no
per-tool copies such as `CLAUDE.md` or `.cursor/rules`. [`CONTRIBUTING.md`](CONTRIBUTING.md) is the
source of truth for contributors; this file adds what agents get wrong without it. Do not repeat its
content here, link to it.

## Workflow

1. Open or find a GitHub issue before the first code edit (`gh issue create` with a scope checklist).
2. Work on a branch, never on `main`.
3. Commit subjects are lowercase Conventional Commits that reference the issue:
   `fix: close consumers on shutdown, ref: #123`. Breaking changes use `feat!:`.
4. The pull request title follows the same format (it becomes the squash commit and the changelog
   entry) and the description starts with `Closes #<issue>`. Fill in the template checklist.
5. Update documentation in the same pull request. New pages go into the `nav` in `mkdocs.yml`.
6. Add an ADR when a change meets the triggers under "Design decisions" in `CONTRIBUTING.md`. Read
   the ADRs in `docs/development/adr` before changing what they decide.

## Layout

|               Path                |                                 What                                 |
|-----------------------------------|----------------------------------------------------------------------|
| `driver-api`                      | Public driver SPI. Changes here affect every driver and external one |
| `benchmark-framework`             | Coordinator, workers, workload generation, reports, plugin loading   |
| `driver-*`                        | One driver plugin per broker                                         |
| `worker-rs`                       | Optional Rust worker behind the worker HTTP API (ADR 0021), cargo    |
| `e2e-tests`                       | Testcontainers contracts and the Docker Compose regression suite     |
| `metrics-otlp`, `package`, `tool` | OTLP metrics, distribution tarball, CLI tools                        |
| `deployment`, `docker`            | Helm chart, local kind setup, image build                            |
| `docs`, `mkdocs.yml`              | Documentation site (Zensical), ADRs in `docs/development/adr`        |
| `workloads`, `payload`            | Sample workloads and payloads                                        |

## Build and check

Use the tool versions from `mise.toml` (JDK 27, Maven 3.10). `mvn clean verify` must pass; it runs
Enforcer, Spotless, license headers, Checkstyle, Error Prone with NullAway (JSpecify mode), SpotBugs,
the ArchUnit tests and the JaCoCo thresholds. Fix findings, do not suppress them.

- Build only some drivers: `mvn -Domb.selectDrivers -Pdriver-kafka package`.
- In a linked Git worktree the license check fails because JGit cannot read worktrees. Append `-Dlicense.skip` locally, and still add the license header to every new file.
- After editing Markdown run the root formatter, which module builds skip:
  `mvn -q -N -Dlicense.skip spotless:apply`, then `mvn -Dlicense.skip spotless:check`.
- Docs changes: build the site with `zensical build --clean --strict` from a virtual environment
  outside the checkout (see `CONTRIBUTING.md`).
- e2e needs Docker: `mvn -pl e2e-tests -am -De2e verify` and `e2e-tests/compose/run-e2e.sh --cases <case>`.

## Code rules

- Cover behavior changes with unit tests. A new driver also needs a Testcontainers contract and a
  Compose case.
- Prefer core Java over Guava and Commons, and avoid `null` where NullAway would need it.
- Catch `Throwable` only to aggregate, propagate or rethrow; restore the interrupt flag on
  `InterruptedException`. Use `volatile` only to publish immutable or thread-safe objects.
- Do not change the `driver-api` contract without saying so in the pull request; external plugins
  depend on it.

## Hard constraints

- Publish images and Helm charts only to Docker Hub (`docker.io/mihkels`). Never reference the GitHub or
  Google container registries anywhere in the repository; `workflow-lint.yml` fails on their hostnames.
- Local Kubernetes uses the project kubeconfig and the `kind-omb` context only, never Docker Desktop
  (`mise run check`).
- Do not commit secrets, `.env` files or local agent and editor settings.

## Security audits

- CVEs in the unmaintained `nsq-client` of `driver-nsq` are known and triaged out. Mention them in one
  line, do not file issues for them.
- Client jars of legacy drivers (NSQ, NATS Streaming, Pravega, RocketMQ) stay frozen. Their stale
  transitive libraries may be pinned to newer same-major versions in the driver's
  `dependencyManagement`, verified by e2e.
- Deployment Terraform and Ansible were removed from this repository. If they reappear, ask before
  auditing them.

