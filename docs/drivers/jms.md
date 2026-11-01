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

# JMS

The JMS driver benchmarks any broker that has a JMS 1.1 or 2.0 client using the `javax.jms` package. The
plugin contains only the JMS API; you add the provider's client jar (the "JMS provider") to the plugin's
`ext/` directory. Topic creation can be delegated to another benchmark driver, such as the Kafka or
Pulsar driver, for providers that cannot create topics through JMS.

|       Item        |                                                                              Value                                                                               |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID         | `jms`                                                                                                                                                            |
| Module            | `driver-jms`                                                                                                                                                     |
| `driverClass`     | `io.openmessaging.benchmark.driver.jms.JMSBenchmarkDriver`                                                                                                       |
| Example configs   | `driver-jms/artemis-jms.yaml`, `artemis-jms-transactions.yaml`, `pulsar-jms.yaml`, `pulsar-jms-selectors.yaml`, `pulsar-jms-transactions.yaml`, `kafka-jms.yaml` |
| Client library    | `jakarta.jms-api` 2.0.3 (package `javax.jms`) plus a provider jar you supply                                                                                     |
| Tested providers  | `artemis-jms-client-all` 2.57.0 with Artemis 2.57.0; `pulsar-jms-all` 6.0.11 with Pulsar 4.0.6; Confluent `kafka-jms-client` (user-built) with Kafka 4.4.0       |
| Regression cases  | `jms-artemis-contract`, `jms-artemis-transactions`, `jms-pulsar-contract`, `jms-pulsar-selectors`, `jms-pulsar-transactions`, `jms-kafka-contract`               |
| Known limitations | Needs a `javax.jms` provider (not `jakarta.jms`); one message per transaction; JMS 1.1 mode ignores subscription names; topics are not deleted                   |

For Apache ActiveMQ Artemis there is also a native driver that uses the Artemis core client instead of
JMS; see [Apache ActiveMQ Artemis](artemis.md#artemis-native-or-artemis-via-jms).

## Prerequisites and broker setup

You need a running broker, its JMS client jar, and, for Pulsar and Kafka, the Pulsar or Kafka plugin
for topic creation. The provider sections below give the jar and broker requirements for each tested
provider.

### Provider jars and `ext/`

The plugin's `plugin.yaml` declares `optionalClasspath: ext/*.jar`: every jar in the plugin's `ext/`
directory is added to the plugin's class loader when the plugin is loaded. The plugin does not ship any
provider. Put the provider jar, with all its dependencies (an "all" or fat jar), in:

|    Installation     |                                          Directory                                          |
|---------------------|---------------------------------------------------------------------------------------------|
| Distribution        | `drivers/driver-jms/ext/` on the coordinator (local mode) and on every worker               |
| Benchmark container | `/benchmark/drivers/driver-jms/ext/` (mount or copy the jar there)                          |
| Source checkout     | `driver-jms/target/omb-plugin/ext/` after `mvn -DskipTests install`; `mvn clean` removes it |

Jars added after the process started are not picked up; restart the workers.

Two scripts add a provider to the distribution tarball in `package/target/` (run
`mvn clean install -DskipTests` first; the scripts need exactly one
`openmessaging-benchmark-*-bin.tar.gz` there):

```bash
bash driver-jms/package-pulsar.sh                      # downloads and sha256-checks pulsar-jms-all 6.0.11
bash driver-jms/package-kafka.sh /path/to/kafka-jms-client-fat.jar
```

Both scripts rewrite the tarball in place with the jar in `drivers/driver-jms/ext/`.

### Admin delegate

`delegateForAdminOperationsClassName` names a `BenchmarkDriver` class that the JMS driver uses for topic
creation and the topic name prefix. The JMS driver initializes the delegate with the **same**
configuration file, so the delegate's own keys go into the JMS file next to the JMS keys (see the Kafka
and Pulsar examples below).

`plugin.yaml` declares this key in `classReferenceProperties`, so the runtime loads the plugin that
contains the class (for example `kafka` or `pulsar`) into the same process as the JMS plugin. That
plugin must be installed. Libraries that both plugins and the provider jar contain must not conflict;
only for annotation-only jars is the highest version chosen, with a warning. See
[Runtime and plugins](../reference/runtime-and-plugins.md).

Without a delegate, `createTopic` does nothing: the broker must create topics on first use, and the
topic prefix is `topicNamePrefix`.

## Minimal configuration and run

`driver-jms/artemis-jms.yaml`, which needs only the Artemis provider jar:

```yaml
name: JMS-Artemis
driverClass: io.openmessaging.benchmark.driver.jms.JMSBenchmarkDriver
connectionFactoryClassName: org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory
connectionFactoryConfigurationParam: 'tcp://localhost:61616'
use20api: true
```

The shipped files all use `name: JMS`; set a distinct `name` per provider so that result files and the
result's `driver` field identify the run.

Host names to change: the shipped files use `localhost`. In the benchmark container use addresses
reachable from the container; the regression configs in `e2e-tests/compose/drivers/jms-*.yaml` use the
Compose service names `artemis`, `pulsar` and `kafka`. For Pulsar and Kafka, change the host in both the
connection factory parameter and the delegate's keys.

Run in local mode from the repository root or from `/benchmark` in the container:

```bash
bin/benchmark --drivers my-artemis-jms.yaml -o jms-artemis.json workloads/1-topic-1-partition-1kb.yaml
```

This workload runs for 15 minutes at 50,000 messages/s; copy it and lower `producerRate` and
`testDurationMinutes` for a quick check (see [Workloads](../guides/workloads.md)). For remote workers,
see [Distributed runs](../getting-started/distributed-runs.md); for an existing cluster, see
[Using your broker](../getting-started/using-your-broker.md).

## Configuration reference

Keys read by the JMS driver (`io.openmessaging.benchmark.driver.jms.config.JMSConfig`). Unknown keys are
ignored by the JMS driver and passed on to the delegate, which reads the same file.

|                  Key                  |          Type           | Default  |                                                                                                                  Description                                                                                                                   |
|---------------------------------------|-------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`                                | string                  | required | Used in the result file name and the result's `driver` field.                                                                                                                                                                                  |
| `driverClass`                         | string                  | required | `io.openmessaging.benchmark.driver.jms.JMSBenchmarkDriver`                                                                                                                                                                                     |
| `connectionFactoryClassName`          | string                  | required | Provider's `javax.jms.ConnectionFactory` class. The default is a placeholder that fails.                                                                                                                                                       |
| `connectionFactoryConfigurationParam` | string                  | `""`     | Passed to the factory. If the class has a `String` constructor, the value is passed as is (Artemis URL, Pulsar JMS JSON). Otherwise, if it has a `Properties` constructor, the value is parsed as a JSON object into `Properties` (Confluent). |
| `use20api`                            | boolean                 | `false`  | `true`: JMS 2.0 asynchronous send and shared durable subscriptions. `false`: JMS 1.1 synchronous send and non-durable consumers. See [Semantics](#semantics).                                                                                  |
| `sendWithTransactions`                | boolean                 | `false`  | Send each message in its own transacted session and commit it. See [Transactions](#transactions).                                                                                                                                              |
| `messageSelector`                     | string                  | none     | JMS message selector applied to every consumer. See [Selectors](#selectors-and-message-properties).                                                                                                                                            |
| `properties`                          | list of `{name, value}` | `[]`     | String properties set on every message.                                                                                                                                                                                                        |
| `delegateForAdminOperationsClassName` | string                  | none     | `BenchmarkDriver` class used for topic creation and the topic prefix, for example `io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkDriver`.                                                                                            |
| `topicNamePrefix`                     | string                  | `""`     | Topic prefix when there is no delegate.                                                                                                                                                                                                        |

> **Note:** With no delegate and the default empty `topicNamePrefix`, topic names start with a hyphen
> (`-0000000-<random>`). Set `topicNamePrefix`, for example to `test-topic`.

Provider-specific settings go into `connectionFactoryConfigurationParam` and are documented by the
provider: [Artemis JMS client](https://activemq.apache.org/components/artemis/documentation/latest/using-jms.html),
[Pulsar JMS (DataStax Starlight for JMS)](https://github.com/datastax/pulsar-jms) and
[Confluent JMS client](https://docs.confluent.io/platform/current/clients/kafka-jms-client/overview.html).

### Shipped configurations

|              File               |      Provider       | `use20api` | Transactions | Delegate |                                   Notes                                    |      Regression case       |
|---------------------------------|---------------------|------------|--------------|----------|----------------------------------------------------------------------------|----------------------------|
| `artemis-jms.yaml`              | Artemis             | `true`     | no           | none     |                                                                            | `jms-artemis-contract`     |
| `artemis-jms-transactions.yaml` | Artemis             | `true`     | yes          | none     | The regression config uses `use20api: false` (synchronous transacted send) | `jms-artemis-transactions` |
| `pulsar-jms.yaml`               | Pulsar JMS          | `true`     | no           | Pulsar   |                                                                            | `jms-pulsar-contract`      |
| `pulsar-jms-selectors.yaml`     | Pulsar JMS          | `true`     | no           | Pulsar   | Property `foo=bar`, selector `foo = 'bar'`, client-side emulation          | `jms-pulsar-selectors`     |
| `pulsar-jms-transactions.yaml`  | Pulsar JMS          | `true`     | yes          | Pulsar   | `enableTransaction: true`; needs Pulsar transactions on the broker         | `jms-pulsar-transactions`  |
| `kafka-jms.yaml`                | Confluent Kafka JMS | `false`    | no           | Kafka    | `replicationFactor: 3`, `min.insync.replicas=2`; needs three brokers       | `jms-kafka-contract`       |

## Providers

### Apache ActiveMQ Artemis

- Provider jar: `org.apache.artemis:artemis-jms-client-all` (2.57.0 is tested), from
  [Maven Central](https://repo1.maven.org/maven2/org/apache/artemis/artemis-jms-client-all/2.57.0/). The
  sha256 is pinned as `jms-artemis` in
  [`e2e-tests/compose/artifacts.txt`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/artifacts.txt).
- Connection factory: `org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory`, parameter the
  broker URL, for example `tcp://localhost:61616`. Credentials and other options can be added as URL
  parameters.
- No delegate: Artemis creates addresses on first use with its default address settings.
- Test broker: `apache/artemis:2.57.0` with `ANONYMOUS_LOGIN=true`, as in
  [`e2e-tests/compose/stacks/artemis.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/artemis.yaml);
  see the [Artemis driver page](artemis.md#prerequisites-and-broker-setup) for a `docker run` command.

```bash
mkdir -p drivers/driver-jms/ext
curl -fsSL -o drivers/driver-jms/ext/artemis-jms-client-all-2.57.0.jar \
  https://repo1.maven.org/maven2/org/apache/artemis/artemis-jms-client-all/2.57.0/artemis-jms-client-all-2.57.0.jar
```

In a source checkout, use `driver-jms/target/omb-plugin/ext` as the target directory.

### Apache Pulsar

- Provider jar: `com.datastax.oss:pulsar-jms-all` **6.0.11** (DataStax Starlight for JMS), from
  [Maven Central](https://repo1.maven.org/maven2/com/datastax/oss/pulsar-jms-all/6.0.11/), pinned as
  `jms-pulsar` in `artifacts.txt`. `driver-jms/package-pulsar.sh` adds it to the distribution tarball.
- Why 6.0.11: it is the newest release that implements `javax.jms`. Releases 7.x and later implement
  `jakarta.jms` and cannot be loaded by this driver. Older releases such as 1.2.2 bundle an unshaded
  Pulsar 2.8 client, which cannot reach the transaction coordinator of a Pulsar 4 broker and whose
  classes collide with the Pulsar plugin.
- Connection factory: `com.datastax.oss.pulsar.jms.PulsarConnectionFactory`, parameter a JSON object with
  at least `brokerServiceUrl` and `webServiceUrl`.
- Delegate: `io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkDriver`, which needs the `pulsar`
  plugin and reads the `client` and `producer` sections of the same file (see the [Pulsar driver](pulsar.md)).
  It creates a namespace `public/default-<random>` per run through the admin URL `client.httpUrl`, and
  the JMS producers and consumers use topics in it.
- `client.clusterName` is `local` in the shipped files; the Pulsar standalone broker and the regression
  configs use `standalone`. The value matters only if the delegate has to create the tenant.
- Tested broker: Pulsar 4.0.6 standalone from
  [`e2e-tests/compose/stacks/pulsar.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/pulsar.yaml),
  which also enables `transactionCoordinatorEnabled` and `systemTopicEnabled` for the transactions case.

### Apache Kafka (Confluent JMS client)

- Provider jar: Confluent `kafka-jms-client`. Its license does not allow redistribution, so the project
  does not download it; you build a fat jar yourself.
- Connection factory: `io.confluent.kafka.jms.KafkaConnectionFactory`. It has a `Properties` constructor,
  so `connectionFactoryConfigurationParam` is a JSON object of Kafka and JMS client properties, for
  example `bootstrap.servers`, `client.id`, `group.id` and `allow.out.of.order.acknowledge`.
- `use20api: false`: the Confluent client does not support the JMS 2.0 shared durable consumers or
  durable subscribers used by the driver, so consumers are plain `createConsumer` consumers, and the
  consumer group is `group.id` from the connection factory parameter, not the benchmark subscription
  name.
- Delegate: `io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver`, which needs the `kafka` plugin
  and reads `replicationFactor`, `topicConfig`, `commonConfig`, `producerConfig` and `consumerConfig` from
  the same file (see the [Kafka driver](kafka.md#configuration-reference)). All four property blocks must
  be present.
- Tested broker: `apache/kafka:4.4.0` from the `kafka` Compose stack, with `replicationFactor: 1`.

Build the fat jar:

1. Follow Confluent's
   [installation instructions, Appendix 1](https://docs.confluent.io/platform/current/clients/kafka-jms-client/installation.html#appendix-1).
2. Create a directory, for example `mkdir kafka-jms-client`, and in it create the `pom.xml` from the
   appendix.
3. In that `pom.xml`, change the repository URL `http://packages.confluent.io/maven/` to
   `https://packages.confluent.io/maven/`. Maven blocks plain HTTP repositories.
4. Run `mvn clean package`. The fat jar is in `target/`, for example
   `target/kafka-jms-client-fat-6.2.1.jar`.

Add it to the distribution tarball, giving an absolute or relative path to the jar:

```bash
mvn clean install -DskipTests
bash driver-jms/package-kafka.sh /path/to/kafka-jms-client-fat-6.2.1.jar
```

Or copy the jar into the `ext/` directory of an installed distribution or container, as for Artemis.

## Semantics

| Benchmark concept |                                                                           Implementation                                                                           |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Topic             | JMS `Topic` named `<prefix>-<7-digit index>-<random suffix>`; created by the delegate, or by the broker on first use                                               |
| Partitions        | Passed to the delegate's `createTopic`; without a delegate they have no effect                                                                                     |
| Producer          | `MessageProducer` on its own session of one shared connection; messages are `BytesMessage`s with the provider's default delivery mode (persistent)                 |
| Key               | Set as the `JMSXGroupID` string property (message groups, where the provider supports them)                                                                        |
| Subscription      | `use20api: true`: shared durable subscription named after the benchmark subscription. `use20api: false`: a non-durable consumer; the subscription name is not used |
| Consumer          | One connection and one `AUTO_ACKNOWLEDGE` session per consumer, with a `MessageListener`                                                                           |

**Producing.** With `use20api: true`, the driver sends with a JMS 2.0 `CompletionListener`, and a message
counts as published when the provider confirms it. With `use20api: false`, `send` blocks until the
provider returns, so each producer has one message in flight and the publish rate is limited by the send
round trip. Closing a non-transactional producer fails any sends still unconfirmed and rejects new sends.
Closing from its provider completion callback is rejected; retry after the callback returns.

**Consuming.** The listener copies the body (`getBody(byte[].class)` in 2.0 mode, `BytesMessage.readBytes`
in 1.1 mode), calls the benchmark callback and then `acknowledge()`. In an `AUTO_ACKNOWLEDGE` session the
provider acknowledges when the listener returns. Delivery is at-least-once.

With `use20api: false`, all consumers on a topic are independent non-durable consumers. Whether two
consumers of one benchmark subscription share or both receive the messages depends on the provider: for
Artemis, every consumer receives every message; for the Confluent client, consumers sharing `group.id`
share partitions, and all benchmark subscriptions use the same `group.id`.

**End-to-end latency.** The producer sets the long property `E2EStartMillis` to its wall clock just
before sending; the consumer reports latency as its wall clock minus that value. Cross-host latency
depends on clock synchronization.

**Backlog.** The worker pauses consumers inside the listener callback (see
[Latency and backlog](../methodology/latency-and-backlog.md)). Non-durable consumers in 1.1 mode can only
build a backlog in the provider's consumer buffer.

**Cleanup.** At the end of a run the worker closes producer and consumer handles, then the driver
closes its shared producer connection, the connection factory (if it is `AutoCloseable`) and the
delegate. Initialization and creation failures also close partially opened resources. Topics, durable
subscriptions and delegate-created namespaces are not deleted.

### Selectors and message properties

`properties` adds fixed string properties to every message, and `messageSelector` sets a JMS selector on
every consumer. `pulsar-jms-selectors.yaml` sets `foo=bar` and the selector `foo = 'bar'`, so every message
matches; the run measures the cost of evaluating the selector, not of filtering. It also sets
`jms.enableClientSideEmulation: true`, which makes the Pulsar JMS client evaluate selectors on the client.
To measure filtering, send properties that only some consumers match; the driver has no option to vary
properties per message.

### Transactions

With `sendWithTransactions: true`, every `sendAsync` opens a new transacted session and producer, sends
one message, commits and closes them. Each transaction contains exactly one message, so the results
measure per-message commit cost, not batched transactional throughput. With `use20api: true`, the commit
happens after the asynchronous send completes; with `use20api: false`, send and commit are synchronous.
Consumers are not transacted. Cancelling a transactional send or closing its producer settles the send
future and closes its per-send producer and session; new sends after close fail. A late provider
acknowledgement cannot restart or commit that send. Cancellation does not guarantee that the broker did
not receive the message.

For Pulsar, also set `"enableTransaction": "true"` in the connection factory parameter and enable the
transaction coordinator on the broker (`transactionCoordinatorEnabled=true`, `systemTopicEnabled=true`).
Artemis supports transacted sessions without broker changes.

## Tuning examples

- **Asynchronous send.** `use20api: true` allows several sends in flight per producer; compare it with
  `false` only with the same provider.
- **Provider batching and flow control.** Settings such as Pulsar JMS `producerConfig` (for example
  `blockIfQueueFull`, `batchingMaxPublishDelayMicros`) or Artemis URL parameters (for example
  `confirmationWindowSize`, `consumerWindowSize`) go into `connectionFactoryConfigurationParam`. See the
  provider documentation.
- **Delegate settings.** The Pulsar `client.persistence` quorum settings and the Kafka `topicConfig`
  apply to the topics the delegate creates.

Comparisons between a JMS provider and the broker's native driver also measure the JMS client layer; see
[Fair comparisons](../methodology/fair-comparisons.md).

## Troubleshooting and verification

The regression suite has a lane per provider. The Artemis and Pulsar provider jars are downloaded and
sha256-checked automatically and mounted into `/benchmark/drivers/driver-jms/ext`. For the Kafka lane, put
your fat jar in `e2e-tests/compose/jms-providers/kafka/` or pass it with `--kafka-jms-jar`; without it the
lane is reported as `blocked` and the run fails.

```bash
e2e-tests/compose/run-e2e.sh --stacks jms-artemis
e2e-tests/compose/run-e2e.sh --stacks jms-pulsar
e2e-tests/compose/run-e2e.sh --stacks jms-kafka --kafka-jms-jar /path/to/kafka-jms-client-fat-6.2.1.jar
e2e-tests/compose/run-e2e.sh --cases jms-pulsar-selectors
```

A passing case is a functional check at a low rate, not a performance result. See the
[Compose regression suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md).

|                                Symptom                                 |                                                        Cause and fix                                                        |
|------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `ClassNotFoundException` for the connection factory class              | The provider jar is not in the plugin's `ext/` directory on this host, or the process was started before it was added.      |
| `ClassNotFoundException` or `NoClassDefFoundError` for `jakarta.jms.*` | The provider implements `jakarta.jms` (for example `pulsar-jms-all` 7.x). Use a `javax.jms` release.                        |
| `Cannot find a suitable constructor`                                   | The factory has neither a `String` nor a `Properties` constructor.                                                          |
| `Cannot initialize JMS benchmark driver`                               | Inspect the cause: the provider or delegate plugin may be missing, its settings invalid, or the connection unable to start. |
| Plugin fails to load with conflicting classes                          | The provider jar bundles libraries that conflict with the delegate plugin, for example an unshaded Pulsar client.           |
| Pulsar transacted sends fail                                           | Transactions are not enabled on the broker or `enableTransaction` is missing in the factory parameter.                      |
| Kafka JMS lane reported as `blocked`                                   | The fat jar was not supplied.                                                                                               |
| Topic names start with `-`                                             | No delegate and no `topicNamePrefix`.                                                                                       |

See also [Troubleshooting](../guides/troubleshooting.md).

Source and configuration files:

- Driver source: [`driver-jms/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-jms/src/main/java/io/openmessaging/benchmark/driver/jms)
- Shipped configurations and packaging scripts: [`driver-jms/`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-jms)
- Regression driver configs: [`e2e-tests/compose/drivers/`](https://github.com/mihkels/open-messaging-benchmark/tree/main/e2e-tests/compose/drivers)
- Related drivers: [Apache ActiveMQ Artemis](artemis.md), [Kafka](kafka.md), [Pulsar](pulsar.md)

