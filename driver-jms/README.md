# General Purpose JMS benchmark

With this driver you can run the benchmark against any messaging system that supports JMS 1.1 or JMS 2.0 API.

For instructions on running the OpenMessaging benchmarks for JMS, see the [official documentation](https://github.com/openmessaging/openmessaging.github.io/blob/source/docs/benchmarks/jms.md).

## Modifying the Openmessaging Benchmark Tar for JMS

The JMS client library is not part of the build. Put the JMS provider jars into `drivers/driver-jms/ext/`
of the distribution, on the coordinator and on every worker. They are only visible to the JMS driver process.
When `delegateForAdminOperationsClassName` is set, the plugin of that driver (for example `driver-kafka`)
is loaded into the same process. The scripts below add the provider to the distribution tar file.
Run a full-reactor build from the repository root first; the scripts require exactly one
`package/target/openmessaging-benchmark-*-bin.tar.gz`. They use a temporary extraction and replace the
tarball only after repackaging succeeds.

### DataStax Starlight for JMS / Pulsar Fast JMS

Build the distribution with JDK 25 and add the Pulsar JMS provider:

```bash
mvn clean install -DskipTests
bash driver-jms/package-pulsar.sh
```

The script downloads and SHA-256 verifies `pulsar-jms-all` 6.0.11, replacing older
`pulsar-jms-all` jars in the tarball. This is the latest `javax.jms` release; version 1.2.2 has an
unshaded older Pulsar client that conflicts with the Pulsar plugin and its transaction support.

### Confluent JMS Client / Kafka JMS

Follow the [Confluent instructions][1] to create a fat jar.

- Create a directory

  ```
  cd ~
  mkdir kafka-jms-client
  cd kafka-jms-client
  ```
- Create the pom.xml
- Change `<url>http://packages.confluent.io/maven/</url>` to `<url>https://packages.confluent.io/maven/</url>`
- Build the fat jar

  ```
  mvn clean package
  ```

Follow these instructions to compile the openmessaging benchmark for Confluent JMS Client

- Build the openmessaging benchmark package

  ```
  mvn clean install -DskipTests
  ```
- Run the repacking script passing in the location of the fat jar. EG. `~/kafka-jms-client/target/kafka-jms-client-fat-6.2.1.jar`

  ```
  bash driver-jms/package-kafka.sh /path/to/the/kafka-jms-client.jar
  ```

The jar path can be absolute or relative to the directory from which the script is invoked.
Legacy AWS deployment scripts need adaptation for Java 25 and plugin bundles before use; see
the [root README](../README.md#kuberneteshelm-and-deployments).

## JMS Driver Benchmarks

For Pulsar JMS (and likely Kafka) you will likely want to allocate additional consumer clients.

- Edit your `terraform.tfvars` file to adjust `num_instances["client"]`.
- Run `bin/benchmark` with the `--extra` option to allocate more workers as consumers.

[1]: https://docs.confluent.io/platform/current/clients/kafka-jms-client/installation.html#appendix-1

