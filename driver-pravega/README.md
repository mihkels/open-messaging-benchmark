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

# Pravega driver

This plugin benchmarks Pravega with the Pravega Java client 0.13.0. Benchmark topics are streams in
one scope, partitions are stream segments, subscriptions are reader groups, and message keys are
routing keys. Producers write events directly or, optionally, in transactions.

- Plugin ID: `pravega`
- `driverClass`: `io.openmessaging.benchmark.driver.pravega.PravegaBenchmarkDriver`
- Configs: [`pravega.yaml`](pravega.yaml), [`pravega-exactly-once.yaml`](pravega-exactly-once.yaml)

`pravega-exactly-once.yaml` enables transactional writes. Its `eventPerTransaction: 1` key is
misspelled and ignored, so it runs with the default of 100 events per transaction. It does not make
the benchmark exactly-once end to end.

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/pravega/>

## Prerequisites

- A Pravega cluster whose controller (`client.controllerURI`, port 9090) and segment stores (port
  12345) are reachable from every worker.
- Permission to create the scope, streams and reader groups, and to seal and delete streams.

A test broker (the image is amd64-only; it runs emulated on Apple Silicon):

```bash
docker run -d --name pravega --platform linux/amd64 \
  -e HOST_IP=127.0.0.1 -p 9090:9090 -p 12345:12345 \
  pravega/pravega:0.13.0 standalone
```

## Usage

From the repository root (after `mvn -DskipTests install`) or from `/benchmark` in the benchmark
image:

```bash
bin/benchmark --drivers driver-pravega/pravega.yaml workloads/1-topic-1-partition-1kb.yaml
```

## Smoke check

```bash
e2e-tests/compose/run-e2e.sh --stacks pravega --case-timeout 600
```

This runs `pravega-contract` and `pravega-transactions`. See the
[Compose regression suite](../e2e-tests/compose/README.md). A passing case is a functional check, not
a performance result.

## Limitations

- The `pravega/pravega:0.13.0` image is amd64-only.
- With transactions, publish latency does not include the commit, events become visible only at
  commit, and the open transaction is aborted at close (up to `eventsPerTransaction - 1` events are
  dropped).
- `eventsPerSecond` and `kbytesPerSecond` only apply with `enableStreamAutoScaling: true`.
- With `includeTimestampInEvent: false`, end-to-end latency is meaningless.
- Streams are sealed and deleted at the end (`deleteStreams`); reader groups and the scope remain.
- No authentication or TLS settings.

More: [workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/),
[results](https://mihkels.github.io/open-messaging-benchmark/guides/results/),
[troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/).

## Legacy deployment files

These files come from the original project. They are unmaintained and need adapting for Java 27 and
the current distribution and plugin layout (`drivers/` bundles on every worker) before use:

- [`deploy/`](deploy): Terraform (`provision-pravega-aws.tf`, `terraform.tfvars`) and Ansible
  (`deploy.yaml`, `vars.yaml`, `collect_logs_and_metrics.yaml`) for an AWS cluster with metrics
  collection.
- [`doc/`](doc): notes on building Pravega from source, metrics and logs, running in Kubernetes, and
  troubleshooting.
- [`build-docker.sh`](build-docker.sh), [`deploy-k8s-components.sh`](deploy-k8s-components.sh),
  [`driver-bash.sh`](driver-bash.sh), [`uninstall.sh`](uninstall.sh): Kubernetes helper scripts.
  `build-docker.sh` expects the old `openmessaging-benchmark-0.0.1-SNAPSHOT` tarball.

