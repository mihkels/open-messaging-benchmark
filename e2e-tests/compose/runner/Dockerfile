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

# Java 25 image used by the regression suite for the coordinator and both benchmark workers.
# The build context only contains the distribution tarball produced by the `package` module.
FROM eclipse-temurin:25-jre

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

ARG BENCHMARK_TARBALL=openmessaging-benchmark-bin.tar.gz
ADD ${BENCHMARK_TARBALL} /opt/
RUN mv /opt/openmessaging-benchmark-* /benchmark \
    && mkdir -p /benchmark/drivers/driver-jms/ext

WORKDIR /benchmark
ENV OMB_HOME=/benchmark
CMD ["bin/benchmark-worker", "-p", "8080", "-sp", "8081"]
