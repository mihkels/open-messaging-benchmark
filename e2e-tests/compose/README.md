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

# Driver plugin regression suite (Docker Compose)

This is a local end-to-end regression suite for every driver plugin in the distribution. Each plugin
runs against the messaging service it targets. Everything runs in Docker Compose:

- the brokers;
- two benchmark workers, each starting the selected plugin in its own isolated child JVM;
- the benchmark coordinator.

Each run produces a report that a person can review.

## Quick start

```bash
e2e-tests/compose/run-e2e.sh                  # full suite, about 50 minutes
e2e-tests/compose/run-e2e.sh --list           # show the stacks and cases
e2e-tests/compose/run-e2e.sh --stacks redis   # one messaging service
e2e-tests/compose/run-e2e.sh --cases kafka-contract,pulsar-contract
```

**Requirements:**
- Docker with Compose v2.
- About 8 GB of memory available to Docker.
- Network access to Docker Hub and to the pinned artifact URLs.
- No host JDK or Python is needed. The distribution is built with Maven in Docker when it is missing.
Use `--build host` to build it with the host Maven and JDK 25 instead, or `--build docker` to always rebuild.

Run `--help` to see all options. Filters must name existing cases and stacks; explicitly selected
cases must belong to a selected, configured stack.

## What a case checks

Every case runs `bin/benchmark` with a driver configuration from `drivers/` and a workload from `workloads/`
against `http://worker-1:8080,http://worker-2:8080`.

Each workload lasts one minute, with no warm-up, at a low fixed rate. A case passes when all of these hold:

- the coordinator exits normally within the case timeout and does not log `Failed to run the workload`;
- `result.json` is written with at least three 10-second rate samples and complete, finite publish,
  consume, publish-error and backlog series;
- at least 50% of the target publish rate was published, with no publish errors;
- every subscription received the messages: consumed / (published × subscriptions per topic) is between 0.9 and 1.1;
- the final backlog has drained, within a small tolerance;
- end-to-end latency was recorded;
- any collected worker counter values and their combined totals are finite numbers.

Every plugin has one **contract** case: one topic, one partition, one subscription.

**Regression** cases exercise driver-specific features:
- compression
- idempotence
- subscription types
- deduplication
- quorum, classic and stream queues
- JetStream storage types
- transactions
- KoP protocol conversions
- JMS selectors and transactions
- fan-out, partitioning, keys, random payloads and weighted message-size distributions
- MQTT 5 and RocketMQ 5 gRPC contract cases

In a full run, any plugin shipped in `drivers/` that has no registered contract case is reported as
a failure. Matrix cases that were not registered, for example because their stack is missing, are
also reported.

## Reports

Each run writes to `e2e-tests/target/e2e-reports/<run-id>/`. The latest run is also linked as `latest`.

|            File             |                                                        Contents                                                         |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------|
| `index.html`                | Self-contained report: verdict, plugin and case matrix, checks, rate charts, log excerpts, images and artifacts         |
| `summary.md`                | Short Markdown summary for pull requests or chat                                                                        |
| `junit.xml`                 | One test suite per stack plus run-level infrastructure errors, for CI tools                                             |
| `results.json`              | Machine-readable results (`schemaVersion` 1)                                                                            |
| `cases/<case>/`             | `result.json`, the coordinator log, worker and driver logs, worker metrics, and the effective driver and workload files |
| `stacks/<stack>/`           | Effective Compose file, startup, broker and teardown logs, images with digests and platforms                            |
| `run.log`, `run.properties` | Orchestrator log and run metadata: git revision, host, Docker and Java versions                                         |

**Case statuses:**

|        Status         |                          Meaning                           |
|-----------------------|------------------------------------------------------------|
| `passed`              | All checks passed.                                         |
| `failed`              | A check failed.                                            |
| `error`               | The stack or the workers did not start.                    |
| `blocked`             | A required artifact is missing, such as the Kafka JMS jar. |
| `aborted` / `not-run` | The run was interrupted.                                   |

The exit code is 0 only when every selected case passed and every stack was cleaned up.
If the report container fails or does not produce all four files, the runner tries host `python3`.
If Docker is unavailable and host Python is not installed, it cannot create a report and reports
the preflight error on the console.

Reports may contain hostnames, image digests and log excerpts. Values that look like passwords or tokens are
redacted from the excerpts.

Use `--cases` or `--stacks` to rerun failures; each failed case in the report shows its own rerun command.

## Platforms

All brokers run natively on linux/amd64 and on Apple Silicon (linux/arm64), except these, which only
publish amd64 images:

- **Pravega:** `pravega/pravega:0.13.0`
- **KoP:** `apachepulsar/pulsar:2.10.4` with the KoP 2.10.4.6 protocol handler

On Apple Silicon, Docker Desktop runs them under emulation, which is slower. If cases on these stacks time out,
pass `--case-timeout 600`. They are marked
`emulated-amd64` in the report. BookKeeper and DistributedLog use the multi-arch Pulsar 4.0.6 image, so they
run natively.

## JMS providers

The JMS plugin is tested with three providers. Each provider jar is mounted into `drivers/driver-jms/ext`
and loaded through the plugin's optional classpath:

|     Lane      |               Provider               |                     Source                      |
|---------------|--------------------------------------|-------------------------------------------------|
| `jms-artemis` | `artemis-jms-client-all` 2.42.0      | Maven Central, sha256-pinned in `artifacts.txt` |
| `jms-pulsar`  | `pulsar-jms-all` 6.0.11              | Maven Central, sha256-pinned in `artifacts.txt` |
| `jms-kafka`   | Confluent `kafka-jms-client` fat jar | You supply it, because of its license           |

To run the Kafka lane, build the fat jar as described in [driver-jms/README.md](../../driver-jms/README.md).
Then either put it in `e2e-tests/compose/jms-providers/kafka/`, or pass `--kafka-jms-jar /path/to/jar`.
`*.jar` files in that directory are git-ignored. Without the jar, the lane is reported as `blocked`, and the run fails.

`pulsar-jms-all` 6.0.11 is the newest `javax.jms` release; 7.x and later use `jakarta.jms`. Older releases,
such as 1.2.2, bundle an unshaded Pulsar 2.8 client. That client cannot reach the transaction coordinator of a
Pulsar 4 broker, and its classes collide with those of the Pulsar plugin.

Downloaded artifacts are cached in `e2e-tests/target/e2e-cache/`. Each artifact has its own cache directory,
which is mounted as a whole, so older versions are removed when a new one is downloaded.

## Layout and extending the matrix

|           Path           |                                          Purpose                                          |
|--------------------------|-------------------------------------------------------------------------------------------|
| `run-e2e.sh`             | Orchestrator, compatible with bash 3.2                                                    |
| `common.yaml`            | Workers, coordinator (profile `runner`) and the `stack-ready` gate                        |
| `stacks/*.yaml`          | One Compose file per messaging service                                                    |
| `stacks.txt`             | `stack\|compose files\|architecture\|prerequisite\|description`                           |
| `cases.txt`              | `case\|plugin id\|stack\|contract or regression\|driver file\|workload file\|description` |
| `drivers/`, `workloads/` | Case inputs, using Compose service names as hosts                                         |
| `artifacts.txt`          | Pinned external artifacts with sha256                                                     |
| `runner/Dockerfile`      | Java 25 image containing the binary distribution                                          |
| `report/report.py`       | Report generator, Python standard library only                                            |

**To add a plugin:**

1. Add a stack file, or reuse one. It must make `stack-ready` depend on the broker being healthy.
2. Add the stack to `stacks.txt`.
3. Add a driver configuration that uses the Compose host names.
4. Add a `contract` row to `cases.txt`.

The Compose matrix includes an MQTT 5 stack backed by Mosquitto and a RocketMQ 5 stack that adds
the gRPC proxy to the existing RocketMQ broker services. `kafka-size-distribution` exercises
weighted message sizes through the worker's isolated plugin process and serialized command path.

Stacks run one after another, and every case gets freshly recreated workers. A failing case or stack never
stops the run.

## Driver defects found and fixed by this suite

- **Pulsar:** concurrent tenant creation by two workers failed with HTTP 500 on Pulsar 4 standalone. This error is now treated like a conflict if the tenant exists.
- **RabbitMQ:** stream queues (`queueType: STREAM`) never got consumers, because stream queues need manual
  acknowledgements and a prefetch limit. Before the fix, the benchmark hung.
- **NATS:** `close()` threw a `NullPointerException` after a failed initialization.
- **NATS configuration:** `storageType` must be lowercase (`file` or `memory`), because Jackson 3 reads enums by `toString()`.
- **NATS Streaming:** end-to-end latency was always 0, because message timestamps are in nanoseconds and were
  treated as milliseconds.
- **NSQ:** `createTopic` did nothing, so consumers sometimes subscribed after the probe messages had been
  published, and the run failed with "Timed out waiting for consumers to be ready". Topics are now created
  through the nsqd HTTP API. `nsqdHttpPort` defaults to 4151 and can be overridden in the driver YAML.
- **KoP:** the Pulsar producer branch checked `consumerType` instead of `producerType`.
- **KoP stack:** Kafka 3.x clients use idempotent producers, which KoP rejects unless the transaction coordinator
  and broker deduplication are enabled.
- **Pravega configuration:** `writer.enableConnectionPooling` was cast to `Boolean`, but YAML values such as
  `True` arrive as strings under Jackson 3. Every Pravega run failed with a `ClassCastException`. It is now parsed.
- **Pravega transactions:** the probe transaction was filled with events that lacked the timestamp header. The
  consumer read a timestamp of 0, the latency recorder threw, and the exception silently stopped the reader
  thread. Its segments then remained unread. Probe events are now encoded like regular events, and the reader
  logs processing errors and keeps reading.
- **Plugin classpath:** plugins that share a process, such as JMS and Pulsar, failed to start when they bundled
  different versions of annotation-only jars, such as `error_prone_annotations`. For these jars only, the
  highest version is now used, with a warning.

The driver-specific regression cases are listed in [cases.txt](cases.txt).
