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

# JMS driver

This plugin benchmarks any broker with a JMS 1.1 or 2.0 client in the `javax.jms` package. It contains
only the JMS API (`jakarta.jms-api` 2.0.3); you supply the provider's client jar. Topic creation can be
delegated to the Kafka or Pulsar driver.

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/jms/>

## Plugin and configuration files

- Plugin ID: `jms`
- `driverClass`: `io.openmessaging.benchmark.driver.jms.JMSBenchmarkDriver`
- Configurations in this directory:

|              File               |      Provider       |                     Notes                     |
|---------------------------------|---------------------|-----------------------------------------------|
| `artemis-jms.yaml`              | Artemis             | JMS 2.0 API                                   |
| `artemis-jms-transactions.yaml` | Artemis             | One transaction per message                   |
| `pulsar-jms.yaml`               | Pulsar JMS          | Pulsar admin delegate                         |
| `pulsar-jms-selectors.yaml`     | Pulsar JMS          | Message property and selector                 |
| `pulsar-jms-transactions.yaml`  | Pulsar JMS          | Needs Pulsar transactions on the broker       |
| `kafka-jms.yaml`                | Confluent Kafka JMS | Kafka admin delegate, JMS 1.1 API, 3 replicas |

All use `localhost` and the same `name: JMS`; set a distinct `name` per provider.

## Provider jars

`plugin.yaml` declares `optionalClasspath: ext/*.jar`. Put the provider jar, with its dependencies, in:

- `drivers/driver-jms/ext/` in the distribution, on the coordinator (local mode) and every worker;
- `/benchmark/drivers/driver-jms/ext/` in the container;
- `driver-jms/target/omb-plugin/ext/` in a source checkout (removed by `mvn clean`).

| Provider |                           Jar                            |                      Connection factory class                      |
|----------|----------------------------------------------------------|--------------------------------------------------------------------|
| Artemis  | `org.apache.artemis:artemis-jms-client-all` 2.57.0       | `org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory` |
| Pulsar   | `com.datastax.oss:pulsar-jms-all` 6.0.11                 | `com.datastax.oss.pulsar.jms.PulsarConnectionFactory`              |
| Kafka    | Confluent `kafka-jms-client` fat jar (build it yourself) | `io.confluent.kafka.jms.KafkaConnectionFactory`                    |

Use `pulsar-jms-all` 6.0.11: 7.x and later implement `jakarta.jms`, which this driver cannot load, and
older releases such as 1.2.2 bundle an unshaded Pulsar 2.8 client that cannot reach a Pulsar 4
transaction coordinator and collides with the Pulsar plugin.

The scripts below add a provider to the distribution tarball. Run `mvn clean install -DskipTests` first;
they need exactly one `package/target/openmessaging-benchmark-*-bin.tar.gz`:

```bash
bash driver-jms/package-pulsar.sh                      # downloads and sha256-checks pulsar-jms-all 6.0.11
bash driver-jms/package-kafka.sh /path/to/kafka-jms-client-fat.jar
```

### Building the Confluent Kafka JMS fat jar

The Confluent client cannot be redistributed, so build it yourself:

1. Follow Confluent's
   [installation instructions, Appendix 1](https://docs.confluent.io/platform/current/clients/kafka-jms-client/installation.html#appendix-1).
2. `mkdir kafka-jms-client` and create the `pom.xml` from the appendix in it.
3. Change the repository URL `http://packages.confluent.io/maven/` to `https://packages.confluent.io/maven/`.
4. Run `mvn clean package`. The jar is in `target/`, for example `target/kafka-jms-client-fat-6.2.1.jar`.
5. In this repository, run `mvn clean install -DskipTests`, then
   `bash driver-jms/package-kafka.sh /path/to/kafka-jms-client-fat-6.2.1.jar` (absolute or relative path).

## Admin delegate

`delegateForAdminOperationsClassName` (for example
`io.openmessaging.benchmark.driver.pulsar.PulsarBenchmarkDriver` or
`io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver`) creates topics and provides the topic
prefix. The delegate is initialized with the same YAML file, so its keys (`client`/`producer` for
Pulsar, `replicationFactor` and the four property blocks for Kafka) go in the JMS file. The runtime
loads the delegate's plugin into the same process, so that plugin must be installed. Without a delegate,
the broker must create topics on first use; set `topicNamePrefix`.

## Usage

Local mode, from the repository root or from `/benchmark` in the container, after adding the Artemis
provider jar:

```bash
bin/benchmark --drivers driver-jms/artemis-jms.yaml -o jms-artemis.json workloads/1-topic-1-partition-1kb.yaml
```

For remote workers, add `--workers` (see
[Distributed runs](https://mihkels.github.io/open-messaging-benchmark/getting-started/distributed-runs/)).

## Smoke check

```bash
e2e-tests/compose/run-e2e.sh --stacks jms-artemis
e2e-tests/compose/run-e2e.sh --stacks jms-pulsar
e2e-tests/compose/run-e2e.sh --stacks jms-kafka --kafka-jms-jar /path/to/kafka-jms-client-fat-6.2.1.jar
```

The Artemis and Pulsar provider jars are downloaded and sha256-checked automatically. For Kafka, put the
fat jar in `e2e-tests/compose/jms-providers/kafka/` or pass `--kafka-jms-jar`; otherwise the lane is
`blocked`. See the [Compose regression suite](../e2e-tests/compose/README.md#jms-providers). A passing
case is a functional check, not a performance result.

## Limitations

- Providers must implement `javax.jms`; `jakarta.jms` providers are not supported.
- `sendWithTransactions` commits one message per transaction; consumers are not transacted.
- With `use20api: false`, consumers are non-durable and ignore the benchmark subscription name (the
  Confluent client uses `group.id` from the connection factory parameter).
- Message selectors and properties are fixed for the whole run.
- Topics, subscriptions and delegate-created namespaces are not deleted.

## Further reading

- [JMS driver page](https://mihkels.github.io/open-messaging-benchmark/drivers/jms/)
- [Apache ActiveMQ Artemis native driver](https://mihkels.github.io/open-messaging-benchmark/drivers/artemis/)
- [Runtime and plugins](https://mihkels.github.io/open-messaging-benchmark/reference/runtime-and-plugins/)
- [Workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/),
  [Troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/)

