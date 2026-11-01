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

# Testing a driver

A driver can be tested at three levels:

|          Level           |               Command                |         Needs          | Runs in CI |
|--------------------------|--------------------------------------|------------------------|------------|
| Unit tests               | `mvn -pl driver-<name> -am verify`   | JDK and Maven          | yes        |
| Testcontainers e2e tests | `mvn -pl e2e-tests -am -De2e verify` | JDK, Maven and Docker  | no         |
| Compose regression suite | `e2e-tests/compose/run-e2e.sh`       | Docker with Compose v2 | no         |

All commands run from the repository root.

> **Note:** These suites check that a driver works, not its performance. The Testcontainers suite uses
> short message exchanges; the separate Compose suite uses one-minute regression workloads. To measure
> performance, see [Benchmark design](../methodology/benchmark-design.md).

## Unit tests

Unit tests live in `driver-<name>/src/test/java` and run with Maven Surefire:

```bash
mvn -pl driver-<name> -am verify
```

`-am` also builds the modules the driver depends on, such as `driver-api`. The Kafka, KoP, NSQ, RabbitMQ
and RocketMQ drivers currently have unit tests. The framework's own tests, including the plugin loader and the worker, are in
`benchmark-framework`:

```bash
mvn -pl benchmark-framework -am verify
```

Good candidates for unit tests in a driver:

- parsing of the driver YAML, including defaults and values that YAML parses differently than expected;
- the mapping from configuration to client properties;
- how the publish timestamp is written and read back, including its unit (milliseconds, see
  [Implementing a driver](implementing-a-driver.md#end-to-end-publish-timestamp));
- `close()` after a failed or partial `initialize`.

CI runs `mvn verify` for the whole reactor on every pull request (see
[Building the project](building-the-project.md#continuous-integration)).

## Testcontainers end-to-end tests

The [`e2e-tests`](https://github.com/mihkels/open-messaging-benchmark/tree/main/e2e-tests) module checks
message delivery against brokers started with Testcontainers. The tests are skipped by default. Enable
them with the `e2e` property, which activates the `e2e-tests` profile. They require JDK 27, Maven,
Docker, and network access to the broker images and pinned provider artifacts:

```bash
mvn -pl e2e-tests -am -De2e verify
```

- The tests are named `*IT.java` and run with Maven Failsafe, so the `verify` phase is required. Surefire
  is skipped in this module.
- The profile builds the plugin bundles and binary distribution. Tests use the same isolated
  `ProcessWorker` driver JVM as the benchmark: either from the host against mapped ports, or from a
  packaged benchmark container sharing a network with the broker. The latter handles advertised or fixed
  broker addresses without binding their fixed ports on the host. Maven also runs upstream unit tests
  when building the required modules, including for a selected integration test.
- Each functional check sends readiness probes and a small burst of messages at a low rate, then stops as
  soon as it observes delivery and no publish errors. The delivery polling deadline is 30 seconds; broker
  startup, worker operations and cleanup may take longer. There are no throughput or load-test thresholds.
  Broker-specific checks also inspect resources and, where appropriate, actual message bytes, timestamps,
  selectors, or delayed delivery. Running the full suite takes longer than one check because it starts
  multiple brokers, particularly when images need emulation. Networked checks log when each exchange or
  message probe starts and completes.
- Pravega and KoP use `linux/amd64` images and need amd64 emulation on arm64 hosts. The shared-network
  runner builds an image containing the distribution; allow sufficient Docker disk space and memory.
  The first run downloads broker images and the Artemis/Pulsar JMS jars and KoP NAR pinned and
  SHA-256-checked against `e2e-tests/compose/artifacts.txt`. Testcontainers caches these artifacts under
  `e2e-tests/target/testcontainers-artifacts/`; JMS providers are copied into separate runner containers.
- The license Maven plugin can fail in a linked Git worktree because it treats the worktree as a bare
  repository. Run the full command in a regular clone if that happens.

Run a subset using Failsafe's test selector. The `failIfNoSpecifiedTests` setting prevents the upstream
reactor modules, which do not have these integration tests, from rejecting the selection:

```bash
mvn -pl e2e-tests -am -De2e -Dit.test=KafkaE2eIT,RedisE2eIT \
  -Dfailsafe.failIfNoSpecifiedTests=false verify
```

The suite writes `e2e-tests/target/testcontainers-coverage.txt` from the plugin IDs in the shipped
descriptors and the contract methods executed in the current Failsafe JVM. `PASSED`, `FAILED`, `SKIPPED`
and `NOT_EXECUTED` are method outcomes, not claims about other environments. A selective run naturally
leaves other drivers `NOT_EXECUTED`; old Failsafe XML files are not used to fill the report.
At `pre-integration-test`, the old coverage and Failsafe reports are removed so a selection that runs no
tests leaves no coverage report and cannot reuse an earlier Failsafe result. If Maven stops before that
phase, existing reports may predate the run. The per-class Failsafe XML reports in
`e2e-tests/target/failsafe-reports/` contain failure and skip details.
Shared-network runs also retain their smoke-check output and result JSON, when produced, under
`e2e-tests/target/testcontainers-runs/<run-id>/` for diagnosing failed assertions.

The suite defines the following contracts and focused regressions:

|      Plugin      |                          Test class                          |                                         Broker and cases                                         |
|------------------|--------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `kafka`          | `KafkaE2eIT`                                                 | Kafka 4.4; topics, partitions, keys, compression and consumer groups                             |
| `rabbitmq`       | `RabbitMqE2eIT`, `RabbitMq41E2eIT`                           | RabbitMQ 3.13 and 4.1; classic, quorum and stream queues, fan-out and broker-application restart |
| `redis`          | `RedisE2eIT`                                                 | Redis 7.4; streams, consumer groups, binary payloads and pool settings                           |
| `pravega`        | `PravegaE2eIT`                                               | Pravega 0.13; streams, segments, timestamps and transactions                                     |
| `pulsar`         | `PulsarE2eIT`                                                | Pulsar 4.0; Failover, Shared and Key_Shared subscriptions, batching and deduplication            |
| `artemis`        | `ArtemisE2eIT`                                               | Artemis 2.57; multicast and independent subscriptions                                            |
| `bookkeeper`     | `BookKeeperE2eIT`                                            | ZooKeeper and a BookKeeper bookie; DistributedLog stream and ordered payloads                    |
| `jms`            | `JmsArtemisE2eIT`, `JmsPulsarE2eIT`, `JmsKafkaOptionalE2eIT` | Artemis and Pulsar JMS 1.1/2.0, transactions and selectors; optional Kafka JMS 1.1               |
| `nats`           | `NatsE2eIT`                                                  | JetStream file/memory storage, fan-out, durable groups and timestamp-bearing payloads            |
| `nats-streaming` | `NatsStreamingE2eIT`                                         | STAN publish/ack, queue groups, server timestamps and non-durable subscriptions                  |
| `nsq`            | `NsqE2eIT`                                                   | Lookup discovery, topics and channel delivery                                                    |
| `rocketmq`       | `RocketMqE2eIT`                                              | RocketMQ 5.3 remoting, queues and consumer groups                                                |
| `rocketmq5`      | `RocketMq5E2eIT`                                             | RocketMQ 5.3 gRPC proxy, normal and delayed delivery                                             |
| `mqtt5`          | `Mqtt5E2eIT`                                                 | Mosquitto, QoS 0/1/2 and shared subscriptions; QoS 0 is not a durability guarantee               |
| `kop`            | `KopE2eIT`                                                   | KoP 2.10.4.6; Kafka and Pulsar producer/consumer combinations                                    |

These are defined cases, not a guarantee of success for every Docker environment or broker version. The
coverage and Failsafe reports show which ones actually completed.

The Kafka JMS provider is licensed and is never downloaded by the suite. Its test is explicitly skipped
unless enabled. To run it, supply your own provider fat jar:

```bash
mvn -pl e2e-tests -am -De2e -Dit.test=JmsKafkaOptionalE2eIT \
  -Dfailsafe.failIfNoSpecifiedTests=false \
  -Domb.e2e.jms.kafka.enabled=true -Domb.e2e.jms.kafka.jar=/path/to/provider-fat.jar verify
```

Enabling the case without a usable jar fails rather than silently skipping it.

To add a test for a new driver:

1. Extend `BaseE2eIT` and start the broker with Testcontainers, using mapped ports or `NetworkBenchmark`
   for discovery-sensitive brokers. The `package` dependency in the opt-in profile builds shipped bundles.
2. Run an isolated-process smoke check, call `validateResults`, and check actual broker resources or messages.
3. Annotate the contract method with `@DriverContract("<plugin-id>")` so its executed outcome appears in
   the coverage report. An added plugin without an executed contract is reported as `NOT_EXECUTED`.

## Compose regression suite

[`e2e-tests/compose/run-e2e.sh`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/run-e2e.sh)
runs every shipped plugin end to end in Docker Compose: the brokers, two benchmark workers (each with its
own isolated driver JVM) and the coordinator. Because producers and consumers run on different workers, it
exercises the distributed path (see [Architecture](architecture.md#distributed-mode-distributedworkersensemble)).
The full description is in
[`e2e-tests/compose/README.md`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md).

Requirements: Docker with Compose v2, about 8 GB of memory available to Docker, and network access to
Docker Hub and to the pinned artifact URLs. No host JDK or Python is needed.

```bash
e2e-tests/compose/run-e2e.sh                  # full suite
e2e-tests/compose/run-e2e.sh --list           # show the stacks and cases
e2e-tests/compose/run-e2e.sh --stacks redis   # one messaging service
e2e-tests/compose/run-e2e.sh --cases kafka-contract,pulsar-contract
```

### Options

|       Option        |                                  Meaning                                   |
|---------------------|----------------------------------------------------------------------------|
| `--stacks a,b`      | Only run these stacks (see `stacks.txt`)                                   |
| `--cases a,b`       | Only run these cases (see `cases.txt`); combines with `--stacks`           |
| `--build MODE`      | `auto` (default): build the distribution in Docker only when it is missing |
|                     | `docker`: always rebuild with Maven in Docker                              |
|                     | `host`: always rebuild with the host Maven and JDK 27                      |
|                     | `skip`: never build; fail when the distribution is missing                 |
| `--kafka-jms-jar F` | Confluent Kafka JMS client fat jar (default: `jms-providers/kafka/*.jar`)  |
| `--case-timeout S`  | Timeout for one case in seconds (default 420)                              |
| `--list`            | Print the stacks and cases and exit                                        |
| `-h`, `--help`      | Show the help                                                              |

Environment variables: `OMB_E2E_CASE_TIMEOUT`, `OMB_E2E_STARTUP_TIMEOUT`, `OMB_E2E_KAFKA_JMS_JAR`,
`OMB_E2E_IMAGE`, `OMB_E2E_PYTHON_IMAGE`, `OMB_E2E_MAVEN_IMAGE` and `OMB_E2E_M2` (the Maven repository
mounted for `--build docker`).

Filters must name existing stacks and cases, and explicitly selected cases must belong to a selected,
configured stack. Stacks run one after another, every case gets freshly created workers, and a failing
case or stack does not stop the run.

### What a case checks

Every case runs `bin/benchmark` with a driver configuration from `drivers/` and a workload from
`workloads/` against `http://worker-1:8080,http://worker-2:8080`. Each workload lasts one minute at a low
fixed rate, with no warm-up. A case passes when:

- the coordinator exits normally within the case timeout and does not log `Failed to run the workload`;
- `result.json` is written with at least three 10-second rate samples and complete, finite publish,
  consume, publish-error and backlog series;
- at least 50% of the target publish rate was published, with no publish errors;
- every subscription received the messages: consumed / (published × subscriptions per topic) is between
  0.9 and 1.1;
- the final backlog has drained, within a small tolerance;
- end-to-end latency was recorded;
- any collected worker counter values and their totals are finite numbers.

Every plugin has one **contract** case that uses
[`workloads/contract.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/workloads/contract.yaml):
one topic, one partition, one subscription, one producer, 1 KB messages at 200 msg/s for one minute.
**Regression** cases cover driver-specific features such as compression, partitioning, fan-out, keys,
queue types and transactions. In a full run, a shipped plugin without a contract case is reported as a
failure.

The driver defects that this suite has found so far, such as consumers that subscribed after the readiness
probe and timestamps in the wrong unit, are listed in the Compose README.

### Reports

Each run writes to `e2e-tests/target/e2e-reports/<run-id>/`, and `e2e-tests/target/e2e-reports/latest`
links to the most recent run.

|            File             |                                                Contents                                                 |
|-----------------------------|---------------------------------------------------------------------------------------------------------|
| `index.html`                | Self-contained report: verdict, plugin and case matrix, checks, rate charts, log excerpts               |
| `summary.md`                | Short Markdown summary                                                                                  |
| `junit.xml`                 | One test suite per stack plus run-level infrastructure errors                                           |
| `results.json`              | Machine-readable results                                                                                |
| `cases/<case>/`             | `result.json`, coordinator, worker and driver logs, worker metrics, effective driver and workload files |
| `stacks/<stack>/`           | Effective Compose file, startup, broker and teardown logs, images with digests                          |
| `run.log`, `run.properties` | Orchestrator log and run metadata                                                                       |

|        Status         |                          Meaning                          |
|-----------------------|-----------------------------------------------------------|
| `passed`              | All checks passed                                         |
| `failed`              | A check failed                                            |
| `error`               | The stack or the workers did not start                    |
| `blocked`             | A required artifact is missing, such as the Kafka JMS jar |
| `aborted` / `not-run` | The run was interrupted                                   |

The exit code is 0 only when every selected case passed and every stack was cleaned up. Each failed case in
the report shows the command that reruns it.

### Adding a stack and a case

The suite is driven by files in
[`e2e-tests/compose`](https://github.com/mihkels/open-messaging-benchmark/tree/main/e2e-tests/compose)
(paths below are relative to that directory):

|           Path           |                                    Purpose                                     |
|--------------------------|--------------------------------------------------------------------------------|
| `common.yaml`            | Workers, the coordinator (Compose profile `runner`) and the `stack-ready` gate |
| `stacks/*.yaml`          | One Compose file per messaging service                                         |
| `stacks.txt`             | The stacks, in execution order                                                 |
| `cases.txt`              | The cases                                                                      |
| `drivers/`, `workloads/` | Case inputs, using Compose service names as host names                         |
| `artifacts.txt`          | Pinned external artifacts with SHA-256 checksums                               |
| `runner/Dockerfile`      | Java 27 image containing the binary distribution                               |

**1. Stack file.** `common.yaml` defines a `stack-ready` service, and both workers start only after it
has completed successfully. A stack file adds the broker services and makes `stack-ready` wait until the
broker is healthy. `stacks/redis.yaml`:

```yaml
services:
  redis:
    image: redis:7.4-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 5s
      retries: 30

  stack-ready:
    depends_on:
      redis:
        condition: service_healthy
```

The health check should reflect the broker being able to accept clients, not only the container running.

**2. `stacks.txt` row.** Columns are separated by `|`:

```text
stack | compose files in stacks/ (space separated) | architecture | prerequisite | description
```

- `architecture` is `native` (multi-architecture images) or `emulated-amd64` (`linux/amd64`-only images,
  emulated on arm64 hosts).
- `prerequisite` is `-`, `kop-nar` or `jms:<provider>`. A missing prerequisite marks the stack's cases as
  blocked.

**3. Driver configuration.** Add `drivers/<name>.yaml`, using the Compose service names as hosts:

```yaml
name: Redis
driverClass: io.openmessaging.benchmark.driver.redis.RedisBenchmarkDriver
redisPort: 6379
redisHost: redis
jedisPoolMaxTotal: 8
jedisPoolMaxIdle: 8
```

**4. `cases.txt` row.** Add at least a `contract` case:

```text
case id | plugin id | stack | contract or regression | driver file (drivers/) | workload file (workloads/) | description
```

For example:

```text
kafka-contract | kafka | kafka | contract | kafka.yaml | contract.yaml | acks=all producer, single partition
```

The plugin id is the `id` from the bundle's `plugin.yaml` (see
[Packaging a plugin](packaging-a-plugin.md#pluginyaml)).

**External artifacts.** If the stack needs a file that is downloaded, add it to `artifacts.txt`:

```text
name | url | sha256
```

Then run only the new stack until it passes, replacing `my-stack` with its ID:

```bash
e2e-tests/compose/run-e2e.sh --stacks my-stack
```
