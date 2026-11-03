# OpenMessaging Benchmark Docker image

> **Note:** See [Containers and Kubernetes](https://mihkels.github.io/open-messaging-benchmark/guides/containers-and-kubernetes/) in the documentation site for a usage guide.

One multi-stage BuildKit [`Dockerfile`](Dockerfile) builds the image, and
[`docker-bake.hcl`](../docker-bake.hcl) in the repository root holds the build targets. Run the
commands from the repository root. Only Docker with buildx is needed: Maven runs inside the build.

## Published images

`docker.io/mihkels/open-messaging-benchmark`, for `linux/amd64` and `linux/arm64`:

|                   Tag                    |                                     Contents                                      |
|------------------------------------------|-----------------------------------------------------------------------------------|
| `edge`, `sha-<commit>`                   | latest `main` build, every driver                                                 |
| `<version>`, `<major>.<minor>`, `latest` | release, every driver                                                             |
| `<tag>-<driver>`                         | the same build with one driver plugin, for example `edge-kafka` or `0.1.0-pulsar` |

Every image is signed with cosign (keyless, from the GitHub Actions workflow) and has SBOM and
provenance attestations. See
[Releasing](../docs/development/releasing.md#verifying-images-and-charts) for verification.

## Build locally

```bash
docker buildx bake                                    # openmessaging-benchmark:local, every driver
DRIVERS=kafka docker buildx bake                      # only the Kafka plugin
DRIVERS=kafka,pulsar IMAGE=omb:kp docker buildx bake  # several drivers, own tag
docker buildx bake multiarch-amd64 multiarch-arm64    # :local-amd64 and :local-arm64
```

`docker build -f docker/Dockerfile --build-arg DRIVERS=kafka .` works too. `DRIVERS` is `all` or
a comma-separated list from [`drivers.txt`](drivers.txt); unknown names fail the build.

Selecting drivers shortens the build: Maven builds only the selected driver modules (the
`-Domb.selectDrivers -Pdriver-<name>` profiles, see
[Building the project](../docs/development/building-the-project.md#building-selected-drivers)), and
the image contains only their plugins and configuration files. The Maven repository is kept in a
BuildKit cache mount, so later builds do not download dependencies again.

The build skips tests and the license, formatting and static analysis checks; CI runs them. It
does not need the `.git` directory, so it also works from a linked Git worktree.

## Stages

|   Stage   |                                                                                      Purpose                                                                                       |
|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `builder` | `maven:3.10.0-eclipse-temurin-27`; builds the distribution tarball                                                                                                                 |
| `dist`    | the tarball: `builder`, or a prebuilt one (`--build-arg DIST_SOURCE=prebuilt --build-context prebuilt=<dir with dist.tar.gz>`, used by CI to build every variant from one tarball) |
| `extract` | unpacks the distribution, removes unselected drivers, makes the files read-only                                                                                                    |
| `runtime` | `eclipse-temurin:27-jre`; the final image                                                                                                                                          |

Base images are pinned by digest and updated by Dependabot.

## Runtime

- The distribution is in `/benchmark` (working directory, read-only) and `/benchmark/bin` is on
  `PATH`. Relative paths such as `bin/benchmark`, `workloads/...` and `driver-kafka/...` work.
- The image runs as `omb` (uid and gid 10001). The root file system can be mounted read-only:
  logs go to `OMB_LOG_DIR=/tmp`, and results written without `--output` to
  `OMB_RESULTS_DIR=/results`. Mount writable volumes there.
- `JAVA_TOOL_OPTIONS` sets `-XX:+ExitOnOutOfMemoryError -XX:+DisableAttachMechanism` and moves the
  Java temporary directory and user home to `/tmp`.
- The default command is a shell; pass `bin/benchmark-worker -p 8080` or `bin/benchmark ...`.
- JMS provider jars go into `/benchmark/drivers/driver-jms/ext`; mount a volume there.

For the broker-backed driver regression suite, see
[e2e-tests/compose/README.md](../e2e-tests/compose/README.md). For Kubernetes, see the
[Helm chart](../deployment/kubernetes/helm/README.md) and
[Local Kubernetes](../docs/guides/kubernetes-local.md).
