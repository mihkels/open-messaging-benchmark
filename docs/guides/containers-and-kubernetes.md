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

Published images are in `docker.io/mihkels/open-messaging-benchmark` for `linux/amd64` and
`linux/arm64`:

|                   Tag                    |                   Contents                    |
|------------------------------------------|-----------------------------------------------|
| `<version>`, `<major>.<minor>`, `latest` | release, every driver                         |
| `<tag>-<driver>`                         | one driver plugin, for example `latest-kafka` |

Images are published from release tags only; there are no images of unreleased `main` commits.
Build one locally to try changes that are not released yet.

Images are signed with cosign and carry SBOM and provenance attestations; see
[Releasing](../development/releasing.md#verifying-images-and-charts).

To build your own, [`docker/Dockerfile`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docker/Dockerfile)
compiles the project with Maven inside the build and needs only Docker with buildx:

```bash
docker buildx bake                      # openmessaging-benchmark:local, every driver
DRIVERS=kafka docker buildx bake        # only the Kafka plugin, faster and smaller
```

See [docker/README.md](https://github.com/mihkels/open-messaging-benchmark/blob/main/docker/README.md)
for the build targets, stages and multi-architecture builds.

The image is Eclipse Temurin 27 with the distribution in `/benchmark`, which is also the working
directory and is read-only. It runs as uid 10001. Logs go to `/tmp` (`OMB_LOG_DIR`) and results
written without `--output` to `/results` (`OMB_RESULTS_DIR`), so the root file system can be
mounted read-only with writable volumes on those two paths.

### Running containers

The image's default command is a shell. Pass the command to run:

```bash
# Shared worker token, kept out of your shell history
printf 'OMB_WORKER_TOKEN=%s\n' "$(openssl rand -hex 32)" > omb-token.env

# Worker
docker run -d --name omb-worker -p 8080:8080 --env-file omb-token.env \
  -e HEAP_OPTS="-Xms4G -Xmx4G" openmessaging-benchmark:local bin/benchmark-worker

# Coordinator with configuration from the host; --user lets it write the result there
docker run --rm -it --user "$(id -u):$(id -g)" -v "$PWD/config:/config" --env-file omb-token.env \
  openmessaging-benchmark:local \
  bin/benchmark --drivers /config/kafka.yaml --workers http://w1:8080,http://w2:8080 \
  -o /config/result.json /config/workload.yaml
```

A worker in a container listens on `0.0.0.0`, so it refuses to start without a
[bearer token](../reference/cli.md#authentication): pass the same `OMB_WORKER_TOKEN` to the workers
and the coordinator, as above, or start the worker with `--insecure-no-auth` on a network you trust.

Each worker container runs a supervisor JVM (`SUPERVISOR_HEAP_OPTS`, default `-Xmx1G`) and one
driver JVM (`HEAP_OPTS`, default 4 GiB). Give the container more memory than the sum; otherwise it is
killed for exceeding its limit.

### Adding a JMS provider

The JMS driver loads provider jars from `drivers/driver-jms/ext/`. Mount them there:

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

The chart `open-messaging-benchmark-chart` creates a StatefulSet of workers with a headless
Service, and a coordinator. It is published to Docker Hub as an OCI artifact whose version equals
the benchmark version, and is also in
[`deployment/kubernetes/helm`](https://github.com/mihkels/open-messaging-benchmark/tree/main/deployment/kubernetes/helm).

```bash
helm install omb oci://registry-1.docker.io/mihkels/open-messaging-benchmark-chart --version <version> \
  --set workers.replicas=4 \
  --set-file driverConfig=my-kafka.yaml \
  --set-file workloadConfig=my-workload.yaml
```

|                                 Value                                  |                                    Default                                    |                                                      Notes                                                       |
|------------------------------------------------------------------------|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `image.repository`                                                     | `docker.io/mihkels/open-messaging-benchmark`                                  |                                                                                                                  |
| `image.tag`                                                            | chart `appVersion`                                                            |                                                                                                                  |
| `image.variant`                                                        | empty                                                                         | A driver name selects the single-driver image `<tag>-<driver>`                                                   |
| `image.digest`                                                         | empty                                                                         | Pins the image; overrides tag and variant                                                                        |
| `workers.replicas`                                                     | 2                                                                             | Worker pods; producers use half of them                                                                          |
| `workers.heapOpts`                                                     | `-Xms1G -Xmx1G`                                                               | Driver JVM heap (`HEAP_OPTS`)                                                                                    |
| `workers.resources`                                                    | 500m CPU, 3Gi memory                                                          | Keep the memory limit well above both worker heaps                                                               |
| `coordinator.mode`                                                     | `interactive`                                                                 | `job` runs the workload once and prints the result JSON to the Job log                                           |
| `driverConfig` / `workloadConfig`                                      | empty                                                                         | YAML text (use `--set-file`), mounted at `/config` and used instead of the paths below                           |
| `driver` / `workload`                                                  | `driver-kafka/kafka-latency.yaml`, `workloads/1-topic-16-partitions-1kb.yaml` | Paths in the image                                                                                               |
| `workers.auth.enabled`                                                 | `true`                                                                        | Require a bearer token on the worker API; `false` adds `--insecure-no-auth`. See below                           |
| `workers.auth.existingSecret` / `workers.auth.secretKey`               | empty, `token`                                                                | Secret holding the token. When empty, the chart creates `<fullname>-worker-token`                                |
| `workers.auth.token`                                                   | empty                                                                         | Fixed token for the created Secret; set it or `existingSecret` for GitOps renders                                |
| `workers.auth.protectMetrics`                                          | `false`                                                                       | Require the token on `/metrics` too                                                                              |
| `workers.tls.enabled`                                                  | `false`                                                                       | Serve the worker API over HTTPS with a shared self-signed certificate. See below                                 |
| `workers.tls.existingSecret`                                           | empty                                                                         | `kubernetes.io/tls` Secret, for example from cert-manager. When empty, the chart creates `<fullname>-worker-tls` |
| `networkPolicy.enabled`                                                | `true`                                                                        | Only the coordinator may reach the worker port 8080                                                              |
| `extraVolumes`, `extraVolumeMounts`, `extraInitContainers`, `extraEnv` | empty                                                                         | For example JMS provider jars in `/benchmark/drivers/driver-jms/ext`                                             |

The coordinator finds the workers through the headless Service: `WORKERS` is
`dns://<fullname>-worker.<namespace>.svc:8080`, which `benchmark` resolves to one URL per ready
worker pod, waiting up to `--workers-timeout` (default `5m`) until `EXPECTED_WORKERS`
(`workers.replicas`) have resolved. Changing `workers.replicas` needs no other setting.

In interactive mode, open a shell in the coordinator and start a run; `DRIVER`, `WORKLOAD`,
`WORKERS` and `EXPECTED_WORKERS` are set:

```bash
kubectl exec -it deploy/omb-open-messaging-benchmark-chart-coordinator -- bash
benchmark --drivers "$DRIVER" --workers "$WORKERS" --expected-workers "$EXPECTED_WORKERS" -o /results/result.json "$WORKLOAD"
```

In `job` mode the Job keeps `coordinator.job.backoffLimit` at `0`. Leave it there: the coordinator
already reruns a failed workload up to `--retries` times (default `3`), and a Job retry on top would
rerun the whole benchmark for each failed pod, multiplying the attempts.

Copy the result with `kubectl cp <coordinator-pod>:/results/result.json .` while the pod runs, and
remove everything with `helm uninstall omb`.

The pods run as a non-root user with a read-only root file system, no Linux capabilities, the
`RuntimeDefault` seccomp profile and no service account token. The worker API loads any driver
configuration it receives, so the chart's NetworkPolicy only lets the coordinator of the release
reach it; `networkPolicy.metricsFrom` adds peers such as Prometheus.

By default (`workers.auth.enabled=true`) the worker API also requires a bearer token. The chart injects
`OMB_WORKER_TOKEN` from a Secret into the workers and the coordinator, so `benchmark` in the
coordinator sends it without extra arguments. Without `workers.auth.existingSecret`, the chart
creates a Secret with a random token and reuses it on `helm upgrade`; `helm template`,
`--dry-run`, Argo CD and Flux cannot look up the existing Secret and render a new token on every
render, which rotates it under running pods. For GitOps, set `workers.auth.existingSecret` or a
fixed `workers.auth.token`. To bring your own Secret:

```bash
kubectl create secret generic omb-worker-token --from-literal=token="$(openssl rand -hex 32)"
helm install omb oci://registry-1.docker.io/mihkels/open-messaging-benchmark-chart --version <version> \
  --set workers.auth.existingSecret=omb-worker-token
```

`/metrics` stays open unless `workers.auth.protectMetrics=true`; then Prometheus must send the token
too, for example with `authorization.credentials_file` in its scrape configuration. Probes are not
affected: `/health` needs no token. `workers.auth.enabled=false` starts the workers with
`--insecure-no-auth`, which leaves only the NetworkPolicy in front of the API.

With `workers.tls.enabled=true` the workers serve HTTPS, so the token does not cross the cluster
network in clear text. The chart creates a `kubernetes.io/tls` Secret with a self-signed
certificate (Helm's `genSelfSignedCert`) and reuses it on `helm upgrade`; like the generated token,
`helm template`, Argo CD and Flux render a new one each time, so use `workers.tls.existingSecret`
there. Workers mount the certificate and key, the coordinator only the certificate, which it trusts
through `OMB_WORKER_CA_FILE`; `dns://` discovery then uses `https://`. Readiness probes switch to
HTTPS. The coordinator trusts that one certificate and does not check host names, since it reaches
workers by pod IP, so cert-manager is optional: any `kubernetes.io/tls` Secret works, for example

```bash
bin/benchmark-tls-cert tls
kubectl create secret tls omb-worker-tls --cert=tls/worker.crt --key=tls/worker.key
helm install omb oci://registry-1.docker.io/mihkels/open-messaging-benchmark-chart --version <version> \
  --set workers.tls.enabled=true --set workers.tls.existingSecret=omb-worker-tls
```

A cert-manager `Certificate` works the same way; give it `privateKey.encoding: PKCS8` or an RSA key,
since the worker reads PKCS#8 and PKCS#1 RSA keys. Prometheus must then scrape with
`scheme: https` (see [Monitoring](monitoring.md)).

Worker readiness is an HTTP GET of [`/health`](monitoring.md#worker-health), which answers 503 once
the worker can no longer take a driver. Only ready workers are listed by the headless Service, so
`--expected-workers` discovery waits for them too. A worker whose driver crashed stays ready, since
the next run starts a fresh driver; the coordinator's watchdog fails the running workload instead.
Liveness stays a cheap TCP check, so a crashed driver never restarts the worker pod under a run.
Broker addresses in the driver configuration must be reachable from the worker pods.

For meaningful numbers, schedule workers on dedicated nodes, away from the brokers, with CPU
requests that match their limits. To try the chart with every driver on a laptop, see
[Local Kubernetes](kubernetes-local.md).
