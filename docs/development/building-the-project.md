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

# Building the project

This page is for contributors, and for anyone who needs to build a runtime: a source-tree build, the
binary distribution tarball, or a container image. If you only want to run a benchmark, follow the
[Quickstart](../getting-started/quickstart.md) instead.

All commands run from the repository root.

## Tools

The tool versions are pinned in
[`mise.toml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/mise.toml):

|  Tool  | Version |        Used for        |
|--------|---------|------------------------|
| Java   | 27      | building and running   |
| Maven  | 3.10.0  | the build              |
| Python | 3.15    | the documentation site |

CI builds the documentation site with Python 3.13 (`docs.yml`); any Python 3 that installs
`docs/requirements.txt` works locally.

```bash
mise install
```

Activate mise in your shell, or prefix commands with `mise exec --` so they use these versions. The
Maven Enforcer plugin in the root POM requires Maven 3.9.0 or newer (3.10.0 recommended) and Java 27
or newer.

Docker is optional. You need it only to build images, run containers and run the end-to-end suites.
The launch and helper scripts (`bin/*`, `setup.sh`, `deployment/kubernetes/local/omb-k8s.sh`, `e2e-tests/compose/run-e2e.sh`) need bash.

## Repository layout

|                Path                 |                                Contents                                |
|-------------------------------------|------------------------------------------------------------------------|
| `benchmark-framework/`              | Coordinator, worker and plugin runtime                                 |
| `driver-api/`                       | Driver SPI that plugins compile against                                |
| `driver-*/`                         | One driver plugin per messaging system, with example driver YAML files |
| `workloads/`, `payload/`            | Example workloads and message payloads                                 |
| `bin/`                              | Launch scripts used by the distribution and for in-repo runs           |
| `package/`                          | Binary distribution (`tar.gz`) with all plugin bundles                 |
| `docker/`                           | Dockerfiles for runnable images                                        |
| `deployment/`                       | Kubernetes manifests and Helm chart                                    |
| `e2e-tests/`                        | Testcontainers contracts and the Docker Compose regression suite       |
| `examples/external-driver-plugin/`  | A driver plugin built outside this repository                          |
| `tool/`                             | Workload generator and other utilities                                 |
| `performance-harness/`              | Framework resource-efficiency harness; built only with `-Pperformance` |
| `local-test/`                       | Docker Compose setup used by the Quickstart                            |
| `docs/`, `mkdocs.yml`, `overrides/` | Documentation site                                                     |

## Full reactor build

```bash
mvn clean install
```

This builds and unit-tests every module, assembles each driver's plugin bundle in
`driver-*/target/omb-plugin`, and creates the distribution in `package/target`. Build the whole
reactor: the distribution collects plugin bundles from the driver modules, so building only the
`package` module can leave drivers out.

During the build, the Maven Enforcer and Checkstyle run in the `validate` phase, the license header
check runs in `process-sources`, and Spotless, SpotBugs (with `etc/findbugsExclude.xml`) and JaCoCo
check in `verify`. Tests use JUnit 6 (Jupiter) and Mockito; metrics use Micrometer and the
Prometheus client libraries.

`performance-harness` is not in the default module list. It is built only with `-Pperformance`; see
[Framework resource efficiency](framework-performance.md).

Common variations:

|                 Command                  |                     Effect                     |
|------------------------------------------|------------------------------------------------|
| `mvn clean install -DskipTests`          | Skip unit tests                                |
| `mvn clean verify -Djacoco.skip`         | Skip JaCoCo coverage                           |
| `mvn clean verify -Dcheckstyle.skip`     | Skip Checkstyle                                |
| `mvn clean verify -Dspotless.check.skip` | Skip the Spotless format check                 |
| `mvn clean verify -Dlicense.skip`        | Skip the license header check                  |
| `mvn clean verify -Dspotbugs.skip`       | Skip SpotBugs                                  |
| `mvn -pl driver-kafka -am install`       | Build one module and the modules it depends on |

After a source-tree build, the `bin/` scripts run straight from the checkout. They find the bundles in
`driver-*/target/omb-plugin` (see [Runtime and plugins](../reference/runtime-and-plugins.md)).

## Formatting and license headers

```bash
mvn spotless:apply
mvn license:format
```

- Spotless formats Java with google-java-format, Markdown (`**/*.md`, excluding `target`) with
  flexmark, and sorts POM files.
- The license plugin adds the Apache header from `etc/APACHE-2.txt`. Markdown files get it as an HTML
  comment.

Run both commands before committing, because the build fails on formatting or header violations.

### License check in linked Git worktrees

The license plugin reads Git metadata. In a linked Git worktree, where `.git` is a file rather than a
directory, it can fail with:

```text
Bare Repository has neither a working tree, nor an index
```

To work around this, append `-Dlicense.skip` to the build command:

```bash
mvn clean install -Dlicense.skip
```

This bypasses the license check, so verify headers in a regular clone or in CI before merging.

## Distribution tarball

The `package` module assembles:

```text
package/target/openmessaging-benchmark-<version>-bin.tar.gz
```

`<version>` is the reactor version (the `revision` property in the root POM, currently
`0.0.6`; release-please updates it, see [Releasing](releasing.md)). The assembly descriptor is
[`package/src/assemble/bin.xml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/package/src/assemble/bin.xml).
Prebuilt tarballs for the newest 10 releases are attached to the
[GitHub releases](https://github.com/mihkels/open-messaging-benchmark/releases) together with a
`SHA256SUMS` file; check a download with `sha256sum -c SHA256SUMS`.

The tarball contains a single `openmessaging-benchmark-<version>/` directory with this layout:

```text
bin/                    launch scripts (benchmark, benchmark-worker, ...)
lib/                    benchmark core runtime jars, named <groupId>-<artifactId>-<version>.jar
drivers/<module>/       one plugin bundle per driver module (plugin.yaml, driver jar, lib/)
workloads/              workload files
payload/                payload files
driver-*/*.yaml         driver configuration files
driver-*/deploy/        deployment material shipped with the drivers
README.md, LICENSE
```

Each `drivers/<module>/` directory is a copy of that module's `target/omb-plugin`. A source tarball is
also assembled next to the binary tarball.

## Building selected drivers

By default the reactor builds and packages every driver. To build only some, pass
`-Domb.selectDrivers` with one `driver-<name>` profile per driver (names as in
[`docker/drivers.txt`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docker/drivers.txt)):

```bash
mvn -Domb.selectDrivers -Pdriver-kafka,driver-pulsar -DskipTests package
```

The reactor then contains the framework, `driver-api`, the selected drivers, `package` and `tool`,
and the distribution's `drivers/` holds only those plugins. `driver-kop` also builds `driver-kafka`
and `driver-pulsar`, which it compiles against. The `e2e-tests` module needs every plugin and is only
part of the full build. Without `-Domb.selectDrivers` the `all-drivers` profile stays active.

## Docker images

[`docker/Dockerfile`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docker/Dockerfile)
is a single multi-stage BuildKit build: Maven compiles the distribution inside the build with the
local Maven repository in a cache mount, and the runtime stage is Eclipse Temurin 27 running as a
non-root user. Build targets are in
[`docker-bake.hcl`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docker-bake.hcl):

```bash
docker buildx bake                                  # openmessaging-benchmark:local, every driver
DRIVERS=kafka docker buildx bake                    # selected drivers only
docker buildx bake multiarch-amd64 multiarch-arm64  # both architectures, separate local tags
```

`DRIVERS` selects the Maven profiles above and removes the other drivers from the image. The image
build skips tests and the license, formatting and static analysis checks, and does not need the
`.git` directory, so it works from linked worktrees. See
[`docker/README.md`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docker/README.md)
for the stages and runtime layout, and
[Containers and Kubernetes](../guides/containers-and-kubernetes.md) for running the images.

## Continuous integration

[`.github/workflows/pr-build-and-test.yml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/.github/workflows/pr-build-and-test.yml)
runs on pull requests and on pushes to `main` and `master`. It checks out the full Git history
(`fetch-depth: 0`), installs the tools with `jdx/mise-action`, and
runs:

```bash
mvn --no-transfer-progress --batch-mode verify
```

If the job fails, the Surefire reports are uploaded as the `test-results` artifact. CI does not run the
Testcontainers or Compose end-to-end suites. See [Testing a driver](testing-a-driver.md).

The same workflow runs a `framework-performance-smoke` job, which builds `performance-harness` with
`-Pperformance` and runs its smoke checks (see [Framework resource efficiency](framework-performance.md)).
`framework-performance.yml` runs only when started manually. `docs.yml` builds the documentation site
(see [Documentation](documentation.md#publishing)).

Other workflows:

|       Workflow       |                                                    What it does                                                     |
|----------------------|---------------------------------------------------------------------------------------------------------------------|
| `Docker image`       | Builds the distribution once, smoke-tests and scans the image; publishes signed images from `main` and release tags |
| `Helm chart`         | Lints the chart and local manifests, runs `kafka-contract` on kind; publishes the signed chart from release tags    |
| `Release`            | Maintains the release-please Release PR; see [Releasing](releasing.md)                                              |
| `Pull request title` | Requires conventional-commit titles                                                                                 |
| `Workflow lint`      | actionlint, zizmor, version consistency and the Docker Hub-only registry check                                      |

