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

# Apache Pulsar driver

This plugin benchmarks Apache Pulsar with the Pulsar Java client. Before the run it uses the admin
API to create a tenant (if missing), a new namespace with the configured persistence and
deduplication policies, and partitioned topics.

- Plugin ID: `pulsar`
- `driverClass`: `io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkDriver`
- Configs: [`pulsar.yaml`](pulsar.yaml), [`pulsar-effectively-once.yaml`](pulsar-effectively-once.yaml)
  (broker-side deduplication only; no transactions)

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/pulsar/>

## Prerequisites

- A Pulsar cluster whose binary endpoint (`serviceUrl`, port 6650) and admin endpoint (`httpUrl`,
  port 8080) are reachable from every worker. The identity used must be allowed to create tenants,
  namespaces and partitioned topics.
- `clusterName` must name an existing cluster (`standalone` for `bin/pulsar standalone`; the shipped
  files use `local`).
- `persistence.ensembleSize`/`writeQuorum`/`ackQuorum` must fit your bookies. The shipped files use
  3/3/2; a standalone broker needs 1/1/1.

A test broker, as used by the regression suite:

```bash
docker run -d --name pulsar -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:4.0.6 bin/pulsar standalone -nfw -nss
```

## Usage

Copy `pulsar.yaml`, set `clusterName: standalone` and the persistence quorums to `1`, and run in
local mode from the repository root (after `mvn -DskipTests install`) or from `/benchmark` in the
benchmark image:

```bash
bin/benchmark --drivers pulsar-local.yaml workloads/1-topic-1-partition-1kb.yaml
```

For several workers, see the
[distributed runs guide](https://mihkels.github.io/open-messaging-benchmark/getting-started/distributed-runs/).
Install the plugin bundle on every worker.

## Smoke check

```bash
e2e-tests/compose/run-e2e.sh --cases pulsar-contract
e2e-tests/compose/run-e2e.sh --stacks pulsar
```

See the [Compose regression suite](../e2e-tests/compose/README.md). A passing case is a functional
check, not a performance result.

## Limitations

- Topics, subscriptions and namespaces are not deleted after the run. Every driver instance creates
  a new namespace.
- The subscription type applies per partition: the driver creates one consumer per partition.
- There is no pass-through for other Pulsar client settings and no transaction support.
- `pulsar-effectively-once.yaml` only enables broker deduplication on the namespace.

More: [workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/),
[results](https://mihkels.github.io/open-messaging-benchmark/guides/results/),
[troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/).

## Legacy deployment files

[`deploy/ssd`](deploy/ssd) contains Terraform and Ansible files for AWS (and
[`deploy/ssd/alicloud`](deploy/ssd/alicloud) for Alibaba Cloud) from the original project. They are
unmaintained: they refer to older distribution names, Java versions and launcher settings. Adapt
them for Java 27 and the current distribution and plugin layout (`drivers/` bundles on every worker)
before use. The playbooks read Ansible variables from
[`deploy/ssd/extra_vars.yaml`](deploy/ssd/extra_vars.yaml), for example `disk_dev` for the bookie
disk devices and `protocol_handlers` for downloading protocol handlers such as KoP;
`restart-brokers.yaml` restarts the brokers after a configuration change.
