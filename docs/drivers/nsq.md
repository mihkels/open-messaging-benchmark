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

# NSQ

The NSQ driver benchmarks [NSQ](https://nsq.io/). Producers publish to one `nsqd` over TCP, consumers discover
`nsqd` through `nsqlookupd` and read from a channel. Each benchmark topic is an NSQ topic and each subscription is an
NSQ channel. It uses the `com.github.brainlag:nsq-client` library (1.0.0.RC4).

|          Item          |                                                                                                         Value                                                                                                         |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID              | `nsq`                                                                                                                                                                                                                 |
| Module                 | `driver-nsq`                                                                                                                                                                                                          |
| `driverClass`          | `io.openmessaging.benchmark.driver.nsq.NsqBenchmarkDriver`                                                                                                                                                            |
| Example config shipped | `driver-nsq/nsq.yaml` (`nsqd` and `nsqlookupd` on `localhost`)                                                                                                                                                        |
| Tested broker          | `nsqio/nsq:v1.3.0` (Compose stack `nsq`, NSQ 1.3.0 with `nsqlookupd` and one `nsqd`)                                                                                                                                  |
| Regression cases       | `nsq-contract` (nsqd publish, consumer discovery via nsqlookupd), `nsq-fanout` (2 topics x 2 channels)                                                                                                                |
| Known limitations      | Partitions are not supported (ignored); keys are ignored; one `nsqd` for publishing; fixed TCP 4150 and lookup HTTP 4161 ports; publish errors are not reported as errors; end-to-end latency uses the nsqd timestamp |

## Prerequisites and broker setup

The driver uses three ports, two of them fixed in the code:

|         Port         |                            Used for                             |            Configurable            |
|----------------------|-----------------------------------------------------------------|------------------------------------|
| nsqd TCP 4150        | Publishing (`PUB`) from every producer                          | No, always 4150                    |
| nsqd HTTP 4151       | Creating topics before the run (`POST /topic/create?topic=...`) | Yes, `nsqdHttpPort` (default 4151) |
| nsqlookupd HTTP 4161 | Consumer discovery of `nsqd` instances for a topic              | No, always 4161                    |

In addition:

- `nsqd` must register with `nsqlookupd` (`--lookupd-tcp-address`) so that consumers can find it.
- The address `nsqd` announces to `nsqlookupd` (`--broadcast-address`, default the host name) must be reachable from
  the workers on port 4150. Consumers connect to that announced address, not to `nsqdHost`.

The regression suite starts NSQ from
[`e2e-tests/compose/stacks/nsq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/nsq.yaml),
where `nsqd` announces `--broadcast-address=nsqd`. For a benchmark running on the Docker host, announce `localhost`
instead and publish the ports:

```bash
docker network create nsq
docker run -d --name nsqlookupd --network nsq -p 4160:4160 -p 4161:4161 nsqio/nsq:v1.3.0 /nsqlookupd
docker run -d --name nsqd --network nsq -p 4150:4150 -p 4151:4151 nsqio/nsq:v1.3.0 \
  /nsqd --broadcast-address=localhost --lookupd-tcp-address=nsqlookupd:4160 --data-path=/tmp
```

The image is multi-arch and runs natively on Apple Silicon.

## Minimal configuration and benchmark command

The shipped `driver-nsq/nsq.yaml`:

```yaml
name: Nsq
driverClass: io.openmessaging.benchmark.driver.nsq.NsqBenchmarkDriver

nsqdHost: localhost
nsqdHttpPort: 4151
lookupHost: localhost
```

Set `nsqdHost` to the `nsqd` host and `lookupHost` to the `nsqlookupd` host. Change `nsqdHttpPort` only if `nsqd`
listens for HTTP on a non-default port (`--http-address`).

Run a local-mode benchmark from the repository root (after a full `mvn -DskipTests install`) or from `/benchmark`
in the container image:

```bash
bin/benchmark --drivers driver-nsq/nsq.yaml workloads/1-topic-1-partition-1kb.yaml
```

Host names depend on where the benchmark runs:

- Source checkout or distribution on the Docker host with the commands above: `localhost` works.
- Benchmark inside a container: `localhost` is the container itself; use host names the container can resolve, and
  make `nsqd` broadcast an address the container can reach.
- Compose regression suite: [`e2e-tests/compose/drivers/nsq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/nsq.yaml)
  uses `nsqdHost: nsqd` and `lookupHost: nsqlookupd`.

For remote workers add `--workers` or `--workers-file`; see [Distributed runs](../getting-started/distributed-runs.md).
Install the plugin bundle on every worker, see [Runtime and plugins](../reference/runtime-and-plugins.md), and
[Using your broker](../getting-started/using-your-broker.md) for adapting a shipped configuration.

## Configuration reference

All keys are read by `NsqConfig`. Unknown keys are ignored.

|      Key       |  Type  | Default  |                                             Description                                              |
|----------------|--------|----------|------------------------------------------------------------------------------------------------------|
| `nsqdHost`     | string | required | Host of the `nsqd` that producers publish to (TCP port 4150) and that topics are created on (HTTP).  |
| `nsqdHttpPort` | int    | `4151`   | `nsqd` HTTP port used for topic creation. Must be from 1 to 65535; other values fail initialization. |
| `lookupHost`   | string | required | Host of `nsqlookupd`; consumers query its HTTP API on port 4161.                                     |

There are no pass-through client properties. Client settings such as max-in-flight, timeouts, authentication and TLS
are the defaults of the [nsq-client library](https://github.com/brainlag/JavaNSQClient).

## Semantics

**Topics and partitions.** Topic names start with `Nsq-Benchmark` followed by an index and a random suffix.
`createTopic` sends `POST /topic/create?topic=<topic>` to `http://<nsqdHost>:<nsqdHttpPort>` and fails on any
response other than HTTP 200. Creating the topic up front makes `nsqd` register it with `nsqlookupd` before consumers
look it up; otherwise consumers would only find it at their next periodic lookup. NSQ has no partitions;
`partitionsPerTopic` is accepted, logged as ignored, and has no effect.

**Producers.** Each producer has its own NSQ producer connected to `nsqdHost:4150`. A send is handed to a thread pool
that performs a blocking publish; up to 1000 sends per producer can be outstanding, after which `sendAsync` waits.
The send completes after the publish call returns. Keys are ignored.

**Publish errors.** When a publish fails (NSQ error or timeout), the driver logs `send exception` but still completes
the send successfully. Failed publishes are therefore counted as published, not as publish errors. Check worker logs
and compare published and consumed counts.

**Subscriptions and consumers.** Each consumer queries `nsqlookupd` at `lookupHost:4161` for the topic and connects
to the announced `nsqd` instances. The benchmark subscription name is used as the NSQ channel. Consumers of the same
channel share messages; each channel receives a copy of every message in the topic, so multiple subscriptions per
topic work.

**Acknowledgement.** Each message is finished (`FIN`) after the benchmark callback returns. Requeueing is not used.

**End-to-end latency.** The consumer uses the message timestamp, which `nsqd` assigns when it receives the message,
as the publish time. Latency is measured from the `nsqd` clock and excludes the time from producer to `nsqd`. Clock
offset between the `nsqd` host and the workers adds directly to the result. See
[Latency and backlog](../methodology/latency-and-backlog.md).

**Durability.** Messages are held in `nsqd` memory and written to disk (`--data-path`) when the in-memory queue is
full, according to `nsqd` settings. The driver does not change this.

**Cleanup.** Topics and channels are not deleted after a run. Remove them through the `nsqd` HTTP API
(`/topic/delete`) or by restarting a disposable `nsqd`.

**Workload features.** Multiple topics, multiple subscriptions per topic and multiple consumers per subscription
work. Partition counts and keys are ignored. For backlog workloads (`consumerBacklogSizeGB`) the framework blocks the
consumer callback; messages accumulate in the channel on `nsqd`. See [Workloads](../guides/workloads.md).

## Tuning examples

The driver exposes no throughput settings: per-producer concurrency (1000 outstanding sends) and the consumer's
max-in-flight are fixed by the driver and the client library. Parallelism comes from the workload, through more
topics, more producers per topic and more consumers per subscription; see [Workloads](../guides/workloads.md).

A non-default `nsqd` HTTP port:

```yaml
nsqdHost: nsqd-1.example.com
nsqdHttpPort: 14151
lookupHost: nsqlookupd-1.example.com
```

All producers publish to the single `nsqdHost`, so an NSQ deployment with several `nsqd` instances is only partly
exercised: consumers will connect to every `nsqd` that `nsqlookupd` returns, but publishing goes to one. Take this
into account when comparing with other systems; see [Fair comparisons](../methodology/fair-comparisons.md).

## Troubleshooting and verification

Run the NSQ regression cases (Docker required, see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks nsq
e2e-tests/compose/run-e2e.sh --cases nsq-contract,nsq-fanout
```

A passing case is a functional check at a low rate, not a performance result.

Common problems:

- `Failed to create NSQ topic <topic>: HTTP <code>` or a connection error during topic creation: `nsqdHost` or
  `nsqdHttpPort` does not point to the `nsqd` HTTP API.
- `nsqdHttpPort must be between 1 and 65535`: invalid port in the driver file.
- Messages are published but nothing is consumed: consumers cannot reach the address `nsqd` announced to
  `nsqlookupd` (check `--broadcast-address`), `nsqd` is not registered with `nsqlookupd`, or `lookupHost` is wrong.
  `http://<lookupHost>:4161/lookup?topic=<topic>` shows what consumers receive.
- `send exception` in worker logs: publishes failed but were counted as successful; see Publish errors above.

General advice is in [Troubleshooting](../guides/troubleshooting.md); result interpretation in
[Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-nsq/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-nsq/src/main/java/io/openmessaging/benchmark/driver/nsq)
- Shipped config: [`driver-nsq/nsq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-nsq/nsq.yaml)
- Regression config: [`e2e-tests/compose/drivers/nsq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/nsq.yaml)
- Stack file: [`e2e-tests/compose/stacks/nsq.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/nsq.yaml)
- Legacy Terraform/Ansible files: [`driver-nsq/deploy`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-nsq/deploy). They are unmaintained, reference older distribution names, Java versions and launcher settings, and must be adapted before use.

