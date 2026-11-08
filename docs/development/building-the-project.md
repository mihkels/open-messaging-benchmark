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
check runs in `process-sources`, Error Prone and NullAway run inside `javac` (see
[Static analysis and null safety](#static-analysis-and-null-safety)), and Spotless, SpotBugs (with
`etc/findbugsExclude.xml`) and JaCoCo check in `verify`. Tests use JUnit 6 (Jupiter) and Mockito; metrics use Micrometer and the
Prometheus client libraries.

`performance-harness` is not in the default module list. It is built only with `-Pperformance`; see
[Framework resource efficiency](framework-performance.md).

Common variations:

|                 Command                  |                      Effect                      |
|------------------------------------------|--------------------------------------------------|
| `mvn clean install -DskipTests`          | Skip unit tests                                  |
| `mvn clean verify -Djacoco.skip`         | Skip JaCoCo coverage                             |
| `mvn clean verify -Dcheckstyle.skip`     | Skip Checkstyle                                  |
| `mvn clean verify -Dspotless.check.skip` | Skip the Spotless format check                   |
| `mvn clean verify -Dlicense.skip`        | Skip the license header check                    |
| `mvn clean verify -Dspotbugs.skip`       | Skip SpotBugs                                    |
| `mvn -pl driver-kafka -am install`       | Build one module and the modules it depends on   |
| `mvn -Pmutation verify -pl driver-api`   | Also run PIT mutation testing on `driver-api`    |
| `mvn -pl driver-api compile`             | Compile one module with Error Prone and NullAway |

After a source-tree build, the `bin/` scripts run straight from the checkout. They find the bundles in
`driver-*/target/omb-plugin` (see [Runtime and plugins](../reference/runtime-and-plugins.md)).

## Static analysis and null safety

[Error Prone](https://errorprone.info) runs as a `javac` plugin on every module, main and test code,
and [NullAway](https://github.com/uber/NullAway) runs inside it. Both fail the compile, so their
findings surface in `mvn compile` instead of later in the SonarCloud scan. The configuration is the
`errorprone.args` property in the root `pom.xml`.

- **Error Prone** runs its default checks plus a set promoted to errors. Some of them match Sonar
  rules that used to fail pull requests: `UnnamedVariable` (S7467), `UnusedVariable` and
  `UnusedMethod` (S1481, S1068), `StreamResourceLeak` (S2095) and `FieldCanBeLocal` (S1450). The rest
  are cheap to keep at zero, such as `MissingOverride`, `DefaultCharset`, `StringCaseLocaleUsage`,
  `UndefinedEquals` and `EqualsGetClass`. Production code also gets `InterruptedExceptionSwallowed`:
  declare the exceptions a method throws instead of `throws Exception`, and restore the interrupt flag
  when you catch an `InterruptedException`. Other Error Prone warnings are printed but don't fail the
  build.
- **NullAway** checks the packages marked `@NullMarked` in their `package-info.java`. Today that is
  every package in `driver-api`, `benchmark-framework` and `tool`. In those packages every type is
  non-null unless it is annotated with JSpecify `@Nullable` (`org.jspecify.annotations.Nullable`).
  Driver modules aren't marked yet and aren't checked.

Common fixes for NullAway errors:

|                       Situation                        |                                      Fix                                       |
|--------------------------------------------------------|--------------------------------------------------------------------------------|
| A field, parameter or return value can be `null`       | Annotate it `@Nullable`; callers then have to check it                         |
| A lifecycle field is set later, e.g. in `initialize()` | Annotate it `@Nullable`, copy it to a local variable and check that before use |
| A YAML/JSON field is optional                          | Annotate it `@Nullable`, or give it a default value                            |
| A YAML/JSON field is required                          | Check it where the file is loaded and throw an error that names the key        |
| Jackson or JCommander sets the field                   | `@SuppressWarnings("NullAway.Init")` on the field or no-arg constructor        |

Suppress a false positive with `@SuppressWarnings("<CheckName>")` on the smallest enclosing element,
with a comment that says why.

Error Prone needs access to `javac` internals. `.mvn/jvm.config` holds the `--add-exports` and
`--add-opens` flags, and Maven reads it automatically, so plain `mvn` commands work. Keep the file
when copying the build elsewhere; the Docker build copies it with the sources. Without it, the
compile fails with `IllegalAccessError: ... does not export com.sun.tools.javac.api`.

## Coverage thresholds

The JaCoCo `check` goal runs in `verify` and fails the build when a module's unit-test coverage drops
below its floor. The PR build runs `mvn verify`, so it enforces the same floors.

The root `pom.xml` defines four properties, all `0.00` by default. Each module with production code
overrides them in its own `pom.xml`:

|      Property       |   Counter (covered ratio of the module)   |
|---------------------|-------------------------------------------|
| `jacoco.min.class`  | Classes with at least one executed method |
| `jacoco.min.method` | Methods                                   |
| `jacoco.min.line`   | Lines                                     |
| `jacoco.min.branch` | Branches                                  |

The floors are a ratchet: each one sits about two points below the coverage the module had when it
was set, so ordinary changes pass but a change that adds untested code or deletes tests fails.
Driver modules only count their unit tests; the Testcontainers suite in `e2e-tests` runs drivers in
separate plugin processes and does not feed the JaCoCo report.

To see a module's coverage, run `mvn verify` and open `<module>/target/site/jacoco/index.html`. The
same numbers are in `jacoco.csv` next to it.

To raise a floor after adding tests, set the module's properties to the new coverage minus about
two points, rounded down to two decimals, and include the change in the same pull request. Lower
a floor only when code is deliberately left untested, and say why in the pull request.

Classes excluded from coverage are listed in the JaCoCo plugin configuration of the root
`pom.xml`, each with the reason.

The coverage and branch badges in the README show instruction and branch coverage summed over every
module. On each push to `main`, the `coverage-badge` job of the build workflow regenerates them from
the modules' `jacoco.csv` files and force-pushes them to the orphan `badges` branch.

## Parallel tests

`benchmark-framework` enables JUnit parallel execution in
`src/test/resources/junit-platform.properties`, but it is opt-in. Classes and methods run in the
same thread unless a class is annotated `@Execution(ExecutionMode.CONCURRENT)`. Today only
`ProcessWorkerTest` is, because its methods spend most of their time waiting on child JVMs.

A concurrent class still runs alongside the other classes in the same JVM. Guard JVM-wide state with
`@ResourceLock(Resources.SYSTEM_PROPERTIES)`:

- A test or class that changes system properties or the process environment takes the lock in the
  default read-write mode.
- `ProcessWorkerTest` takes it in `READ` mode for each method
  (`target = ResourceLockTarget.CHILDREN`), so its methods run together but never alongside a
  writer.

Surefire keeps one fork per module (`forkCount` is not set). Forking per CPU was measured and made
the full reactor build slower: about 4:00 instead of 2:43, because every small module pays for
extra JVM starts.

## Mutation testing

JaCoCo shows which lines the tests run, not whether their assertions would catch a bug. PIT measures
that: it makes small changes ("mutants") to the bytecode, such as flipping a condition or replacing a
return value, and reruns the tests. A mutant that makes a test fail is *killed*; one that survives
points at code whose behaviour no assertion checks.

PIT runs only in the opt-in `mutation` profile. It is not part of the default build or of CI, and no
score is enforced. The profile is defined in `driver-api` and `benchmark-framework`, and only
deterministic code is mutated:

|        Module         |                                              Mutated                                              |
|-----------------------|---------------------------------------------------------------------------------------------------|
| `driver-api`          | The whole driver SPI                                                                              |
| `benchmark-framework` | Stats and histogram math, workload and threshold parsing, plugin descriptors, rate control, utils |

Coordinator orchestration, worker processes, HTTP and process IPC, broker contracts and the wall-clock
`Timer` are left out, together with their tests. Those are slow, need real processes, or turn timing
noise into false survivors. The target classes and tests are listed in each module's `mutation`
profile; the shared settings (versions, threads, report formats) are in the root `pom.xml`
`pluginManagement`.

|                          Command                           |                 Effect                  |
|------------------------------------------------------------|-----------------------------------------|
| `mvn -Pmutation verify -pl driver-api`                     | Mutation-test `driver-api`              |
| `mvn -Pmutation verify -pl driver-api,benchmark-framework` | Mutation-test both modules              |
| `mvn -Pmutation verify -pl driver-api -Dpitest.threads=8`  | Use more PIT worker threads (default 4) |

`benchmark-framework` depends on `driver-api`, so either list both in `-pl` or `install` `driver-api`
first. Expect two to three minutes per module with 4 threads. `-DskipTests` also skips PIT. Append
`-Dlicense.skip` in a linked worktree, as for any build.

The report is written to `<module>/target/pit-reports/`:

- `index.html` gives the mutation score per package and class. Open a class to see each line with its
  mutants: green lines are killed, red lines have a survivor, and the list below explains each mutant.
- `mutations.xml` holds the same data for tooling, one `<mutation>` element per mutant with its
  `status`.

The headline numbers are the *mutation score* (killed out of all mutants) and the *test strength*
(killed out of mutants in covered code). A low test strength means the tests run the code but do not
check its result. Besides `KILLED` and `SURVIVED`, expect `NO_COVERAGE` (no test runs the line) and
`TIMED_OUT`. A timeout comes from a mutated loop that never ends or, often in `driver-api`, a mutated
future that never completes, so the test's bounded wait fails. PIT counts timeouts as detected. An
occasional `RUN_ERROR` is a minion that failed to start.

To improve a score, pick a surviving mutant in the HTML report and add an assertion that would fail
under that change. Not every survivor is worth killing; equivalent mutants (changes with no
observable effect) cannot be killed.

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

The build fails when a module's coverage drops below its floor (see
[Coverage thresholds](#coverage-thresholds)). The JaCoCo CSV reports are uploaded as the `jacoco-csv`
artifact, and on `main` the `coverage-badge` job turns them into the README badges. If the job fails,
the Surefire reports are uploaded as the `test-results` artifact. CI does not run the
Testcontainers or Compose end-to-end suites. See [Testing a driver](testing-a-driver.md).

### SonarCloud

After `verify`, the same job runs the SonarCloud scanner, which reuses the compiled classes and the
modules' `target/site/jacoco/jacoco.xml` reports:

```bash
mvn --no-transfer-progress --batch-mode org.sonarsource.scanner.maven:sonar-maven-plugin:sonar
```

The organization, project key, host and coverage report paths are properties in the root `pom.xml`,
and the scanner version is pinned in its `pluginManagement`. The step needs the `SONAR_TOKEN`
repository secret and is skipped when it is missing, which includes pull requests from forks.
Analyses of `main` are the baseline that pull requests are compared against. Results, the quality
gate and coverage on new code are at
[sonarcloud.io](https://sonarcloud.io/summary/new_code?id=mihkels_open-messaging-benchmark).

To run a scan locally, pass a token from your SonarCloud account. From a `git worktree`, also skip
the license check and SCM blame, which cannot read the worktree's Git metadata:

```bash
SONAR_TOKEN=... mvn verify org.sonarsource.scanner.maven:sonar-maven-plugin:sonar \
  -Dlicense.skip=true -Dsonar.scm.disabled=true
```

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

