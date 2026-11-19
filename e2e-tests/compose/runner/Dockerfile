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

# Java 27 image used by the regression suite for the coordinator and both benchmark workers.
# The build context only contains the distribution tarball produced by the `package` module.
FROM amazoncorretto:27-al2023-headless@sha256:8d3d9a96a23daaa38148d56e513cc81fa4543c064e83e5e6393b29dd0774fd7c

# curl ships with the image; shadow-utils provides groupadd and useradd.
RUN dnf install -y --setopt=install_weak_deps=False shadow-utils \
    && dnf clean all \
    && rm -rf /var/cache/dnf

ARG BENCHMARK_TARBALL=openmessaging-benchmark-bin.tar.gz
ADD ${BENCHMARK_TARBALL} /opt/
RUN mv /opt/openmessaging-benchmark-* /benchmark \
    && mkdir -p /benchmark/drivers/driver-jms/ext \
    && groupadd --system --gid 10001 omb \
    && useradd --system --uid 10001 --gid omb --home-dir /tmp --shell /usr/sbin/nologin omb \
    && chown -R 10001:10001 /benchmark

# Non-root like docker/Dockerfile. /benchmark stays writable because the workers and the coordinator
# write their logs there; the compose files mount the result directory at /out.
USER 10001
ENV HOME=/tmp
WORKDIR /benchmark
ENV OMB_HOME=/benchmark
CMD ["bin/benchmark-worker", "-p", "8080", "-sp", "8081"]
