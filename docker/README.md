# OpenMessaging Benchmark Docker images

Both images run on Amazon Corretto 25 (Alpine) and include Bash for the benchmark launchers.
Run the commands below from the repository root.

## Prebuilt distribution

This option requires Docker, Maven and JDK 25 on the host. Build the **full reactor** so the
distribution includes every driver plugin:

```bash
mvn -DskipTests install
export BENCHMARK_TARBALL=package/target/openmessaging-benchmark-0.0.6-SNAPSHOT-bin.tar.gz
docker build -t openmessaging-benchmark:latest \
  --build-arg "BENCHMARK_TARBALL=$BENCHMARK_TARBALL" -f docker/Dockerfile .
```

Adjust the tarball name if the Maven project version changes. The path must be inside the Docker
build context. If the license plugin fails in a Git worktree with "Bare Repository has neither a
working tree, nor an index", append `-Dlicense.skip` to the Maven command; this bypasses the
license check.

## Build inside Docker

This option does not require Maven or a JDK on the host. `Dockerfile.build` uses the
`maven:3.9.11-amazoncorretto-25-alpine` builder and assembles the distribution in the image build:

```bash
docker build -t openmessaging-benchmark:latest -f docker/Dockerfile.build .
```

The Maven license check needs Git metadata inside the build context. A linked Git worktree's
`.git` file may point outside that context and cause the build to fail. Use a regular checkout,
or build the distribution on the host with the worktree workaround above and use `docker/Dockerfile`.

For multi-platform builds, see `./local-build.sh <version> [platforms] [push|load]`. For the local
broker-backed driver regression suite, see [e2e-tests/compose/README.md](../e2e-tests/compose/README.md).
