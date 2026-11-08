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

# Contributing

Thanks for helping improve OpenMessaging Benchmark. This page lists what a change needs. The details
live on the [documentation site](https://mihkels.github.io/open-messaging-benchmark/); follow the links
below.

## Before you start

- Open an issue, or find an existing one, before you start a change. The pull request closes it with
  `Closes #<issue>` in its description.
- If you are not a maintainer, wait until a maintainer labels the issue `accepted` before you open a
  pull request. Comment on the issue to say you want to work on it. Pull requests that don't close an
  open `accepted` issue are closed automatically, without review; adding `Closes #<issue>` for an
  accepted issue to the description reopens them. This includes typo-only changes, unrequested
  refactors and bulk or generated changes.
- Ask questions in [Discussions Q&A](https://github.com/mihkels/open-messaging-benchmark/discussions/categories/q-a)
  and propose features or new drivers in
  [Discussions Ideas](https://github.com/mihkels/open-messaging-benchmark/discussions/categories/ideas).
- Report security problems privately as described in [`SECURITY.md`](SECURITY.md), not in a public
  issue.
- [`SUPPORT.md`](SUPPORT.md) lists where each kind of request goes.
- Everyone taking part in this project follows the [code of conduct](CODE_OF_CONDUCT.md).

## Prerequisites

- JDK 27 and Maven 3.9 or newer (3.10.0 recommended). `mise install` installs the versions pinned in
  [`mise.toml`](mise.toml).
- Python 3, only to preview the documentation site.
- Docker, only to build images and run the end-to-end suites.

See [Building the project](https://mihkels.github.io/open-messaging-benchmark/development/building-the-project/)
for the build profiles and the repository layout.

## Main commands

Build and check everything the way CI does:

```bash
mvn clean verify
```

Format sources and Markdown, and add license headers:

```bash
mvn spotless:apply
mvn license:format
```

In a linked Git worktree the license check fails; append `-Dlicense.skip` and see
[License check in linked Git worktrees](https://mihkels.github.io/open-messaging-benchmark/development/building-the-project/#license-check-in-linked-git-worktrees).

Run the Testcontainers contracts and the Docker Compose regression suite (both need Docker):

```bash
mvn -pl e2e-tests -am -De2e verify
```

```bash
e2e-tests/compose/run-e2e.sh --cases kafka-contract
```

Preview the documentation site, and build it strictly before opening a pull request:

```bash
python3 -m venv .venv
.venv/bin/pip install --require-hashes --only-binary :all: -r docs/requirements.txt
.venv/bin/mkdocs serve
```

```bash
.venv/bin/mkdocs build --strict
```

## Development guides

- [Architecture](https://mihkels.github.io/open-messaging-benchmark/development/architecture/)
- [Building the project](https://mihkels.github.io/open-messaging-benchmark/development/building-the-project/)
- [Implementing a driver](https://mihkels.github.io/open-messaging-benchmark/development/implementing-a-driver/)
- [Packaging a plugin](https://mihkels.github.io/open-messaging-benchmark/development/packaging-a-plugin/)
- [Testing a driver](https://mihkels.github.io/open-messaging-benchmark/development/testing-a-driver/)
- [Documentation](https://mihkels.github.io/open-messaging-benchmark/development/documentation/)
- [External driver plugin example](examples/external-driver-plugin/), built outside this repository

## Pull requests

- **Title.** Pull requests are squash-merged and release-please builds the changelog from the title,
  so the title must be a [Conventional Commit](https://www.conventionalcommits.org/): `feat`, `fix`,
  `perf`, `refactor`, `docs`, `deps`, `build`, `ci`, `chore` or `test`. Write commit subjects the
  same way and reference the issue, for example `fix: Close consumers on shutdown, ref: #123`.
- **Checks.** `mvn verify` must pass: Spotless formatting, license headers, Checkstyle, SpotBugs and
  JaCoCo.
- **Tests.** Cover behavior changes with unit tests. A new driver also needs a Testcontainers contract
  and a Compose case; see
  [Testcontainers end-to-end tests](https://mihkels.github.io/open-messaging-benchmark/development/testing-a-driver/#testcontainers-end-to-end-tests)
  and
  [Adding a stack and a case](https://mihkels.github.io/open-messaging-benchmark/development/testing-a-driver/#adding-a-stack-and-a-case).
- **Static analysis.** SonarCloud analyses every pull request. A few rules are suppressed in the root
  `pom.xml` (`sonar.issue.ignore.multicriteria`) because the code follows them by design: catch
  `Throwable` only to aggregate, propagate or rethrow a failure, and restore the interrupt flag when
  the failure is an `InterruptedException`; mark a field `volatile` only to publish an immutable or
  thread-safe object. Fix any other new finding, or mark it in SonarCloud with a justification.
- **Docs.** Update the documentation in the same pull request. A new page must be added to the `nav`
  in `mkdocs.yml`.
- **Images.** Container images and Helm charts are published only to Docker Hub
  (`docker.io/mihkels`); CI rejects other registries.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License, Version 2.0](LICENSE).
