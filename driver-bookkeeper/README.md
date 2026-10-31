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

# Apache BookKeeper (DistributedLog) driver

This plugin benchmarks Apache BookKeeper through the DistributedLog library (shaded
`distributedlog-core-shaded`, BookKeeper 4.18.2). Each benchmark topic is a DistributedLog log
stream: records are buffered by an asynchronous log writer, written as BookKeeper entries, and read
back by log readers. Namespace and log metadata live in ZooKeeper. The plugin does not use the raw
ledger API.

- Plugin ID: `bookkeeper`
- `driverClass`: `io.openmessaging.benchmark.driver.bookkeeper.DlogBenchmarkDriver`
- Config: [`bookkeeper.yaml`](bookkeeper.yaml) (`dlogUri` plus a `dlogConf` properties block)

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/bookkeeper/>

## Prerequisites

- ZooKeeper and a BookKeeper cluster with initialized metadata and at least `bkcEnsembleSize`
  writable bookies (the shipped file uses 3/3/2 with rack-aware placement).
- A DistributedLog namespace bound to the ledger metadata. The driver does not create it:

```bash
java -cp '/benchmark/drivers/driver-bookkeeper/lib/*' \
  dlshade.org.apache.distributedlog.tools.Tool \
  dlshade.org.apache.distributedlog.admin.DistributedLogAdmin \
  bind -l /ledgers -s zookeeper:2181 -c distributedlog://zookeeper:2181/streams
```

The Compose stack [`e2e-tests/compose/stacks/bookkeeper.yaml`](../e2e-tests/compose/stacks/bookkeeper.yaml)
starts ZooKeeper, initializes the cluster, runs one bookie and binds this namespace.

## Usage

Point `dlogUri` at your ZooKeeper and namespace, set the quorums for your bookies (see
[`e2e-tests/compose/drivers/bookkeeper.yaml`](../e2e-tests/compose/drivers/bookkeeper.yaml) for a
single-bookie example), then run from the repository root (after `mvn -DskipTests install`) or from
`/benchmark` in the benchmark image:

```bash
bin/benchmark --drivers bookkeeper-local.yaml workloads/1-topic-1-partition-1kb.yaml
```

## Smoke check

```bash
e2e-tests/compose/run-e2e.sh --cases bookkeeper-contract
```

See the [Compose regression suite](../e2e-tests/compose/README.md). A passing case is a functional
check, not a performance result.

## Limitations

- Subscription names are ignored: every consumer reads the whole stream from the beginning, so
  several consumers in one subscription each receive every message.
- With more than one partition, extra `<topic>/NNNNNN` logs are created but never used.
- Message keys are ignored.
- The end-to-end timestamp is the producer's wall-clock time (record transaction ID).
- No DistributedLog or BookKeeper client metrics are exported.
- Logs are not deleted after the run.

More: [workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/),
[results](https://mihkels.github.io/open-messaging-benchmark/guides/results/),
[troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/).

## Legacy deployment files

[`deploy/`](deploy) contains Terraform (`provision-pulsar-aws.tf`) and Ansible (`deploy.yaml`) files
with service templates from the original project. They are unmaintained: they download old release
tarballs and the `openmessaging-benchmark-0.0.1-SNAPSHOT` distribution. Adapt them for Java 27 and
the current distribution and plugin layout (`drivers/` bundles on every worker) before use.
