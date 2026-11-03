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

# Local Kubernetes

This guide runs any driver against its broker on a local [kind](https://kind.sigs.k8s.io/) cluster,
with the [Helm chart](containers-and-kubernetes.md#kubernetes-with-helm) and the image built from
your checkout. Use it to try chart or image changes and to check a driver end to end. The cluster
shares one machine between brokers, workers and the coordinator, so its numbers are not benchmark
results.

It reuses the [Compose regression suite](../development/testing-a-driver.md) definitions: the stacks
in `e2e-tests/compose/stacks.txt`, the cases in `cases.txt`, and the unchanged driver configurations
and workloads. Each stack runs in its own namespace `omb-<stack>`, and its broker Services have the
Compose service names (`kafka`, `pulsar`, `rabbitmq`, ...), so `kafka:9092` in a driver
configuration resolves in Kubernetes as it does in Compose. The broker manifests are in
[`deployment/kubernetes/local/brokers`](https://github.com/mihkels/open-messaging-benchmark/tree/main/deployment/kubernetes/local/brokers).

## Tools

[mise](https://mise.jdx.dev) installs the pinned tools from `mise.toml`:

|    Tool     | Version |
|-------------|---------|
| Java        | 27      |
| Maven       | 3.10.0  |
| Helm        | 4.3.1   |
| kind        | 0.33.0  |
| kubectl     | 1.37.2  |
| k9s         | 0.51.0  |
| kubeconform | 0.8.0   |

Docker (Docker Desktop on macOS) with buildx must be running. kind talks to the Docker engine only;
leave Docker Desktop's own Kubernetes off. kubectl and k9s come from mise, not from Docker Desktop.

## Setup

From the repository root:

```bash
./setup.sh
```

`setup.sh` installs mise when it is missing (Homebrew, then the official installer), then runs:

```bash
mise trust
mise install
mise run check
```

`mise run check` prints the tool versions and fails when `kubectl` or `k9s` resolve to a Docker
Desktop binary, or when the current context is a Docker Desktop context. Activate mise in your shell
(`eval "$(mise activate zsh)"`) so that `which kubectl` is a mise shim.

mise sets `KUBECONFIG` to `.kube/config` in the repository (ignored by Git), so the tasks, kubectl
and k9s never touch your other clusters.

## Run it locally

```bash
mise run k8s:cluster
mise run k8s:run kafka
```

`k8s:cluster` creates the kind cluster `omb` (one control-plane and one worker node) and selects
context `kind-omb`. `k8s:run <stack>`:

1. builds the single-driver image for the stack (`openmessaging-benchmark:local-kafka`) unless it
   exists, and loads it into kind;
2. deploys the broker into `omb-kafka` and waits until it is ready;
3. installs the chart with two workers and runs each case of the stack as a coordinator Job;
4. writes the coordinator log, the worker logs and `result.json` of each case to
   `e2e-tests/target/k8s/<case>/`;
5. deletes the `omb-kafka` namespace, so the next stack has the cluster's memory
   (`OMB_K8S_KEEP=1` keeps it for inspection).

The command fails when a case fails. Other commands:

```bash
mise run k8s:list                          # stacks and cases
mise run k8s:run pulsar pulsar-contract    # selected cases only
mise run k8s:deploy redis                  # broker + chart with an interactive coordinator
mise run k8s:shell redis                   # shell in that coordinator
mise run k8s:logs redis                    # pods and recent logs
mise run k9s                               # k9s on the kind-omb context
mise run k8s:down redis                    # delete the omb-redis namespace
mise run k8s:down                          # delete the cluster
```

In the coordinator shell, `DRIVER`, `WORKLOAD` and `WORKERS` are set for the contract case:

```bash
benchmark --drivers "$DRIVER" --workers "$WORKERS" -o /results/result.json "$WORKLOAD"
```

### Environment

|         Variable          |                                         Effect                                          |
|---------------------------|-----------------------------------------------------------------------------------------|
| `OMB_K8S_REBUILD=1`       | Rebuild the stack image even if it exists                                               |
| `OMB_K8S_KEEP=1`             | Keep the stack namespace after `k8s:run`                              |
| `OMB_K8S_PUBLISHED=1`     | Use `docker.io/mihkels/open-messaging-benchmark:edge-<driver>` instead of a local build |
| `OMB_K8S_KAFKA_JMS_JAR`   | Confluent Kafka JMS client jar for the `jms-kafka` stack                                |
| `OMB_K8S_STARTUP_TIMEOUT` | Seconds to wait for the broker and chart (default 900)                                  |
| `OMB_K8S_CASE_TIMEOUT`    | Seconds for one case (default 600)                                                      |
| `OMB_K8S_UNSCHEDULABLE_GRACE` | Seconds a pod may stay unschedulable before a wait fails (default 90) |

## Stacks with prerequisites

- **jms-artemis, jms-pulsar**: an init container downloads the pinned provider jar from Maven
  Central and checks its sha256 (from `e2e-tests/compose/artifacts.txt`); see
  `deployment/kubernetes/local/values/`.
- **jms-kafka**: the Confluent Kafka JMS client is not redistributable. Put the fat jar in
  `e2e-tests/compose/jms-providers/kafka/` or set `OMB_K8S_KAFKA_JMS_JAR`; the task adds it to a
  local image.
- **kop**: the broker downloads the KoP protocol handler and checks its sha256.
- **pravega, kop**: their broker images exist for `linux/amd64` only, and kind nodes have the host
  architecture. They run on amd64 hosts; on Apple Silicon `k8s:run` stops with an explanation.

## Develop the image and chart

Check a chart or manifest change before installing it:

```bash
mise run chart:lint
```

It runs `helm lint --strict`, renders the chart with the default values, in Job mode and with each
values file in `deployment/kubernetes/local/values/`, renders every broker with kustomize and
validates the result with kubeconform. CI runs the same task.

Rebuild the image for a stack after a code or Dockerfile change, then run again:

```bash
OMB_K8S_REBUILD=1 mise run k8s:run kafka kafka-contract
```

`mise run image` builds and loads an image with every driver (`DRIVERS=kafka,pulsar mise run image`
for a selection). A chart-only change needs no new image: the next `k8s:run` or `k8s:deploy`
upgrades the release.

## Image architectures

The tasks build the architecture of your machine, which is also the architecture of the kind
nodes. `mise run image:multiarch` builds `linux/amd64` and `linux/arm64` with separate local tags
(`:local-amd64`, `:local-arm64`), because Docker Desktop's default builder cannot load a
multi-platform image. Published images are multi-platform.

## Troubleshooting

- **Context errors**: run `mise run k8s:cluster`; the tasks refuse any context other than `kind-omb`.
- **A broker does not become ready**: `mise run k8s:logs <stack>` shows the pods, events and logs.
  Brokers such as Pulsar can take a few minutes on the first start while images download.
- **Pods are `Pending`**: every stack namespace holds its broker and workers in memory. The tasks
  fail after 90 seconds of an unschedulable pod and name the other `omb-*` namespaces; remove them
  with `mise run k8s:down <stack>`. Docker needs about 8 GB of memory for the larger stacks.
- **Pods are `OOMKilled` or restart during a case**: the kind nodes report the whole Docker VM
  memory as their own, so Kubernetes does not see other containers. Stop other containers, such as
  the `local-test` Compose stack (`docker compose -f local-test/docker-compose.yaml down`), or give
  Docker more memory. `k8s:run` lists other running containers before it starts.
- **A case fails**: read `e2e-tests/target/k8s/<case>/coordinator.log` and the worker logs next to
  it. Run with `OMB_K8S_KEEP=1` to keep the namespace, then use `mise run k8s:logs <stack>`,
  `kubectl get pods -n omb-<stack>` and `kubectl logs`.

