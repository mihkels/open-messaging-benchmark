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

```bash
mise install
```

Activate mise in your shell, or prefix commands with `mise exec --` so they use these versions. The
Maven Enforcer plugin in the root POM requires Maven 3.9.0 or newer and Java 27 or newer.

## Full reactor build

```bash
mvn clean install
```

This builds and unit-tests every module, assembles each driver's plugin bundle in
`driver-*/target/omb-plugin`, and creates the distribution in `package/target`. Build the whole
reactor: the distribution collects plugin bundles from the driver modules, so building only the
`package` module can leave drivers out.

During the build, Checkstyle runs in the `validate` phase, the license header check runs in
`process-sources`, and Spotless checks formatting.

Common variations:

|                 Command                  |                     Effect                     |
|------------------------------------------|------------------------------------------------|
| `mvn clean install -DskipTests`          | Skip unit tests                                |
| `mvn clean verify -Djacoco.skip`         | Skip JaCoCo coverage                           |
| `mvn clean verify -Dcheckstyle.skip`     | Skip Checkstyle                                |
| `mvn clean verify -Dspotless.check.skip` | Skip the Spotless format check                 |
| `mvn clean verify -Dlicense.skip`        | Skip the license header check                  |
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
`0.0.6-SNAPSHOT`). The assembly descriptor is
[`package/src/assemble/bin.xml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/package/src/assemble/bin.xml).
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

## Docker images

Both Dockerfiles are documented in
[`docker/README.md`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docker/README.md)
and use an Amazon Corretto 27 runtime. Inside the image the distribution is at `/benchmark`, which is
also the working directory.

### From a prebuilt tarball

[`docker/Dockerfile`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docker/Dockerfile)
takes the tarball through the `BENCHMARK_TARBALL` build argument. The file must be inside the build
context:

```bash
mvn -q -DskipTests install
export BENCHMARK_TARBALL=package/target/openmessaging-benchmark-0.0.6-SNAPSHOT-bin.tar.gz
docker build -t openmessaging-benchmark:latest \
  --build-arg "BENCHMARK_TARBALL=$BENCHMARK_TARBALL" -f docker/Dockerfile .
```

### Building inside Docker

[`docker/Dockerfile.build`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docker/Dockerfile.build)
needs no local Maven. Its builder stage (`maven:3.10.0-eclipse-temurin-27-alpine`) resolves
dependencies and runs `mvn install` with tests, and the runtime stage extracts the tarball to
`/benchmark`:

```bash
docker build -t openmessaging-benchmark:latest . -f docker/Dockerfile.build
```

The license check runs inside this build too. If the build context comes from a linked worktree, the
`.git` file can point outside the context and the check can fail.

### Multi-platform helper

[`local-build.sh`](https://github.com/mihkels/open-messaging-benchmark/blob/main/local-build.sh)
formats the sources, builds the reactor on the host, and then builds `docker/Dockerfile.build` with
`docker buildx`:

```bash
./local-build.sh [version] [platforms] [push|load]
```

|   Argument    |                           Default                           |                                     Meaning                                      |
|---------------|-------------------------------------------------------------|----------------------------------------------------------------------------------|
| `version`     | `latest`                                                    | Image tag prefix                                                                 |
| `platforms`   | `linux/arm64,linux/amd64` on macOS, `linux/amd64` otherwise | `buildx --platform` value                                                        |
| `push`/`load` | load for one platform; OCI archive for several              | `push` (or `--push`, `true`, `1`) pushes; `load` (or `--load`) loads into Docker |

The image is tagged `mihkels/open-messaging-benchmark:<version>-kafka-4.4-java27`. Setting
`DOCKER_PUSH` to any value other than `0` also enables pushing. A multi-platform build that neither
pushes nor loads is written to `omb-image-<version>-kafka-4.4-java27.oci.tar`. The script attaches an
SBOM attestation and exports the build cache to the registry `mihkels/open-messaging-benchmark:cache`,
even when the image itself is not pushed.

For running the images, see [Containers and Kubernetes](../guides/containers-and-kubernetes.md).

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
