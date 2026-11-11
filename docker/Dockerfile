# syntax=docker/dockerfile:1.27@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# OpenMessaging Benchmark image. Build from the repository root:
#
#   docker buildx bake local                                   # see docker-bake.hcl
#   docker build -f docker/Dockerfile .                        # every driver
#   docker build -f docker/Dockerfile --build-arg DRIVERS=kafka,pulsar .
#
# DRIVERS is "all" or a comma-separated list from docker/drivers.txt. By default the distribution
# is built with Maven inside the image build. CI builds it once on the runner instead and passes it
# with --build-arg DIST_SOURCE=prebuilt --build-context prebuilt=<dir containing dist.tar.gz>.
# Java: latest GA (27), the same release as maven.compiler.release in pom.xml.

ARG DIST_SOURCE=builder

# Build the distribution. Checks (tests, license, formatting, static analysis) run in CI, not here.
FROM maven:3.10.0-eclipse-temurin-27@sha256:a6cd0d45e565ec76bf1969702bcbdb800d3461c8338012728e891b8681374575 AS builder
WORKDIR /src
COPY . .
ARG DRIVERS=all
RUN --mount=type=cache,target=/root/.m2 \
    set -eu; \
    maven_args=$(docker/select-drivers.sh maven-args "$DRIVERS"); \
    mvn -B --no-transfer-progress -DskipTests -Dlicense.skip -Dspotless.check.skip \
        -Dcheckstyle.skip -Dspotbugs.skip -Djacoco.skip $maven_args package; \
    cp package/target/openmessaging-benchmark-*-bin.tar.gz /dist.tar.gz

FROM ${DIST_SOURCE} AS dist

# Unpack, keep only the selected drivers and make everything read-only.
FROM eclipse-temurin:27-jre@sha256:0dd0465f90d398fa3226b56a3973f5d3c47ac323934dceebdab45ac8910bee6e AS extract
COPY docker/select-drivers.sh docker/drivers.txt /tools/
COPY --from=dist /dist.tar.gz /tmp/dist.tar.gz
ARG DRIVERS=all
RUN set -eu; \
    mkdir /benchmark; \
    tar -xzf /tmp/dist.tar.gz -C /benchmark --strip-components=1; \
    /tools/select-drivers.sh prune "$DRIVERS" /benchmark; \
    if [ -d /benchmark/drivers/driver-jms ]; then mkdir -p /benchmark/drivers/driver-jms/ext; fi; \
    chmod -R a-w,a+rX /benchmark

FROM eclipse-temurin:27-jre@sha256:0dd0465f90d398fa3226b56a3973f5d3c47ac323934dceebdab45ac8910bee6e AS runtime
ARG DRIVERS=all
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="OpenMessaging Benchmark" \
      org.opencontainers.image.description="Coordinator and workers of the OpenMessaging Benchmark with driver plugins" \
      org.opencontainers.image.source="https://github.com/mihkels/open-messaging-benchmark" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      io.openmessaging.benchmark.drivers="${DRIVERS}"

# Upgrades OS packages so Ubuntu security fixes apply before the pinned Temurin image is rebuilt.
# --error-on=any fails the build when an index cannot be fetched (apt-get otherwise only warns),
# so a network failure cannot silently ship an image without the upgrades.
RUN apt-get update --error-on=any \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 omb \
    && useradd --system --uid 10001 --gid omb --home-dir /tmp --shell /usr/sbin/nologin omb \
    && mkdir -p /results \
    && chown 10001:10001 /results

COPY --from=extract --chown=root:root /benchmark /benchmark

USER 10001
# Logs go to OMB_LOG_DIR and results written without --output to OMB_RESULTS_DIR, so the root file
# system can be mounted read-only with /tmp and /results as writable volumes.
ENV HOME=/tmp \
    LANG=C.UTF-8 \
    OMB_HOME=/benchmark \
    OMB_LOG_DIR=/tmp \
    OMB_RESULTS_DIR=/results \
    PATH=/benchmark/bin:${PATH} \
    JAVA_TOOL_OPTIONS="-XX:+ExitOnOutOfMemoryError -XX:+DisableAttachMechanism -Djava.io.tmpdir=/tmp -Duser.home=/tmp"
WORKDIR /benchmark
EXPOSE 8080
STOPSIGNAL SIGTERM
CMD ["/bin/bash"]
