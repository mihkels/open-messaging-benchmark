# Tool

> **Note:** See [Workload generation](https://mihkels.github.io/open-messaging-benchmark/guides/workload-generation/) in the documentation site for a usage guide.

## `WorkloadGenerationTool`

Generates a set of `Workload` definition files from a `WorkloadSetTemplate` file.

### Example

Template:

```yaml
nameFormat: "${topics}-topics-${partitionsPerTopic}-partitions-${messageSize}b-${producersPerTopic}p-${consumerPerSubscription}c-${producerRate}"
topics: [1]
partitionsPerTopic: [1]
messageSize: [100]
payloadFile: "payload/payload-100b.data"
keyDistributor: "NO_KEY"
useRandomizedPayloads: false
randomBytesRatio: 0.0
randomizedPayloadPoolSize: 0
subscriptionsPerTopic: [1]
consumerPerSubscription: [1, 2, 4, 8, 16, 32, 64]
producersPerTopic: [1, 2, 4, 8, 16, 32, 64]
producerRate: [50000]
consumerBacklogSizeGB: 0
testDurationMinutes: 15
warmupDurationMinutes: 0
```

Save the template as `template.yaml`. From the repository root, build with JDK 27 and create the
tool's runtime classpath:

```bash
mvn -DskipTests install
mvn -q -pl tool dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
mkdir my-workloads
java -cp "tool/target/classes:$(cat tool/target/classpath.txt)" \
  io.openmessaging.benchmark.tool.workload.WorkloadGenerationTool \
  --template-file template.yaml --output-folder my-workloads
```

The tool creates 49 YAML files, one for each producer/consumer combination. When randomized
payloads are disabled, the payload file must contain exactly `messageSize` bytes. Its log includes:

```
Generated 49 workloads
```

Example generated workload:

```yaml
name: "1-topics-1-partitions-100b-64p-2c-50k"
topics: 1
partitionsPerTopic: 1
keyDistributor: "NO_KEY"
messageSize: 100
useRandomizedPayloads: false
randomBytesRatio: 0.0
randomizedPayloadPoolSize: 0
payloadFile: "payload/payload-100b.data"
subscriptionsPerTopic: 1
producersPerTopic: 64
consumerPerSubscription: 2
producerRate: 50000
consumerBacklogSizeGB: 0
testDurationMinutes: 15
warmupDurationMinutes: 0
```

