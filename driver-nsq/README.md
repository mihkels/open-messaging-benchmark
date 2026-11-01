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

# NSQ driver

This plugin benchmarks [NSQ](https://nsq.io/). Producers publish to one `nsqd`, consumers find `nsqd` through
`nsqlookupd`; each benchmark topic is an NSQ topic and each subscription is an NSQ channel.

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/nsq/>

- Plugin ID: `nsq`
- `driverClass`: `io.openmessaging.benchmark.driver.nsq.NsqBenchmarkDriver`
- Shipped config: [`nsq.yaml`](nsq.yaml) (`localhost`)
- Tested with: `nsqio/nsq:v1.3.0` ([stack file](../e2e-tests/compose/stacks/nsq.yaml))

## Prerequisites

- `nsqd` reachable on TCP 4150 (fixed) and on its HTTP port (`nsqdHttpPort`, default 4151, used to create topics).
- `nsqlookupd` reachable on HTTP 4161 (fixed), with `nsqd` registered to it.
- `nsqd` must announce (`--broadcast-address`) an address the workers can reach; consumers connect to that address.
- `nsqdHost:nsqdHttpPort` and `nsqdHost:4150` must reach the same single daemon. The driver reads its `/info`
  identity and waits for that node to appear in discovery.
- The `nsq` plugin bundle installed on the coordinator (local mode) and on every worker.

A local test setup for a benchmark running on the Docker host:

```bash
docker network create nsq
docker run -d --name nsqlookupd --network nsq -p 4160:4160 -p 4161:4161 nsqio/nsq:v1.3.0 /nsqlookupd
docker run -d --name nsqd --network nsq -p 4150:4150 -p 4151:4151 nsqio/nsq:v1.3.0 \
  /nsqd --broadcast-address=localhost --lookupd-tcp-address=nsqlookupd:4160 --data-path=/tmp
```

## Usage

Edit `nsqdHost`, `lookupHost` and, if needed, `nsqdHttpPort` in `nsq.yaml`, then run from the repository root (after
`mvn -DskipTests install`) or from `/benchmark` in the container image:

```bash
bin/benchmark --drivers driver-nsq/nsq.yaml workloads/1-topic-1-partition-1kb.yaml
```

Smoke check against NSQ started by Docker Compose (see [`e2e-tests/compose/README.md`](../e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks nsq
```

Send futures report publish failures and timeouts surfaced by the client, interrupted capacity waits,
and producer shutdown. Consumer creation waits for discovery and actual `SUB` acknowledgements, with a 30-second
readiness timeout. Batch cancellation requests partial cleanup without blocking; the creation-quiescence barrier
tracks late connections and cleanup failures. Use a bounded wait for non-cooperative client code. Rediscovery
continues once per minute.

## Limitations

- Partitions are not supported: `partitionsPerTopic` is ignored. Keys are ignored.
- All producers publish to the single `nsqdHost`.
- Closing a consumer from its own callback is rejected to avoid deadlock; retry on another thread. Consumer close
  waits up to 15 seconds for callbacks to finish and send `FIN`, and at most 30 seconds overall. Close failures and
  timeouts are reported; cleanup can continue after a timeout.
- End-to-end latency uses the timestamp `nsqd` assigns on receipt (nsqd clock).
- Topics and channels are not deleted after a run.

## Further reading

- [Workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/)
- [Distributed runs](https://mihkels.github.io/open-messaging-benchmark/getting-started/distributed-runs/)
- [Runtime and plugins](https://mihkels.github.io/open-messaging-benchmark/reference/runtime-and-plugins/)
- [Troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/)

## Legacy deployment files

[`deploy/`](deploy) contains Terraform and Ansible files for AWS from the original project. They are unmaintained:
they reference older distribution names, Java versions and launcher settings. Adapt them to Java 27, the current
distribution and the plugin layout (complete `drivers/` bundles on every worker) before using them.
