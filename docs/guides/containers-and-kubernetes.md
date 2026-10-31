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

# Containers and Kubernetes

## Docker image

The repository has two Dockerfiles. Both produce an Amazon Corretto 27 (Alpine) image with the
distribution in `/benchmark`, which is also the working directory, and every driver plugin under
`/benchmark/drivers`.

|        Dockerfile         |            Build requirements            |                               Use when                               |
|---------------------------|------------------------------------------|----------------------------------------------------------------------|
| `docker/Dockerfile.build` | Docker only                              | You do not have JDK 27 and Maven, and you build from a regular clone |
| `docker/Dockerfile`       | A distribution tarball built on the host | You already build with Maven, or you work in a linked Git worktree   |

```bash
# Build everything inside Docker
docker build -t openmessaging-benchmark:local -f docker/Dockerfile.build .

# Or package a host build
mvn -DskipTests install
docker build -t openmessaging-benchmark:local \
  --build-arg BENCHMARK_TARBALL=package/target/openmessaging-benchmark-0.0.6-SNAPSHOT-bin.tar.gz \
  -f docker/Dockerfile .
```

Adjust the tarball name to the current project version. `docker/local-build.sh` builds
multi-platform images. See [docker/README.md](https://github.com/mihkels/open-messaging-benchmark/blob/main/docker/README.md)
for details.

> **Note:** The fork does not publish a Java 27 image. Push your build to a registry your machines
> or cluster can pull from.

### Running containers

The image's default command is a shell. Pass the command to run:

```bash
# Worker
docker run -d --name omb-worker -p 8080:8080 \
  -e HEAP_OPTS="-Xms4G -Xmx4G" openmessaging-benchmark:local bin/benchmark-worker

# Coordinator with configuration from the host
docker run --rm -it -v "$PWD/config:/config" openmessaging-benchmark:local \
  bin/benchmark --drivers /config/kafka.yaml --workers http://w1:8080,http://w2:8080 \
  -o /config/result.json /config/workload.yaml
```

Each worker container runs a supervisor JVM (`SUPERVISOR_HEAP_OPTS`, default `-Xmx1G`) and one
driver JVM (`HEAP_OPTS`, default 4 GiB). Give the container more memory than the sum; otherwise it is
killed for exceeding its limit.

### Adding a JMS provider

The JMS driver loads provider jars from `drivers/driver-jms/ext/`. Mount or copy them there:

```bash
docker run -d -p 8080:8080 \
  -v "$PWD/ext:/benchmark/drivers/driver-jms/ext:ro" \
  openmessaging-benchmark:local bin/benchmark-worker
```

See [JMS](../drivers/jms.md).

## Docker Compose

- [`local-test/`](https://github.com/mihkels/open-messaging-benchmark/tree/main/local-test) runs
  Kafka, two workers and a coordinator container; it is used by the [Quickstart](../getting-started/quickstart.md).
- [`e2e-tests/compose/`](https://github.com/mihkels/open-messaging-benchmark/tree/main/e2e-tests/compose)
  starts the broker for each driver and runs short functional cases. It is a regression suite, but
  its Compose files are also a reference for a working broker setup for every driver. See
  [Testing a driver](../development/testing-a-driver.md).

## Kubernetes with Helm

The chart in `deployment/kubernetes/helm/benchmark` creates a StatefulSet of workers, a headless
Service and a coordinator pod.

```bash
IMAGE=registry.example.com/team/openmessaging-benchmark:java27
helm upgrade --install benchmark deployment/kubernetes/helm/benchmark \
  --set image="$IMAGE" --set numWorkers=4 --set driverMemoryLimit=8Gi
```

|                     Value                     |                    Default                     |                                 Notes                                  |
|-----------------------------------------------|------------------------------------------------|------------------------------------------------------------------------|
| `image`                                       | `openmessaging/openmessaging-benchmark:latest` | Not maintained by this fork; always set your own image                 |
| `imagePullPolicy`                             | `Always`                                       |                                                                        |
| `numWorkers`                                  | 8                                              | Worker pods; producers use half of them                                |
| `driver`                                      | `driver-pulsar/pulsar.yaml`                    | Used only in the generated `example-run.sh`                            |
| `workload`                                    | `workloads/1-topic-16-partitions-1kb.yaml`     | Used only in the generated `example-run.sh`                            |
| `driverMemoryRequest` / `driverMemoryLimit`   | 512Mi / 2048Mi                                 | Coordinator pod; it sets `HEAP_OPTS=-Xms4G -Xmx4G`, so raise the limit |
| `workersMemoryRequest` / `workersMemoryLimit` | unset                                          | Worker pod resources, applied only when the request is set             |

The release must be named `benchmark`, because the worker address list is hard-coded to
`http://benchmark-worker-<n>.benchmark-worker:8080`. After the pods are ready:

```bash
kubectl exec -it benchmark-driver -- bash
bin/benchmark --drivers driver-kafka/kafka-latency.yaml --workers "$WORKERS" \
  workloads/1-topic-16-partitions-1kb.yaml
```

To use your own driver configuration or workload, copy it into the pod with `kubectl cp`, or build
an image that contains it. Broker addresses in the driver configuration must be reachable from the
worker pods. Copy the result out with `kubectl cp benchmark-driver:/benchmark/<file>.json .`, and
remove everything with `helm uninstall benchmark`.

For meaningful numbers, schedule workers on dedicated nodes, away from the brokers, with CPU
requests that match their limits.

## Cloud deployment scripts

Several driver modules contain a `deploy/` directory with Terraform and Ansible files for
provisioning brokers and workers on AWS or Alibaba Cloud. They come from upstream and are not tested
by this fork; their instance types, versions and paths may be out of date. Treat them as examples,
and check the driver page for their status.
