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

# Workload generation

For parameter sweeps, such as latency at increasing producer counts, write a template and let the
`WorkloadGenerationTool` produce one workload file per combination. The tool lives in the `tool`
module and runs from a source checkout.

## Template

```yaml
nameFormat: "${topics}-topics-${partitionsPerTopic}-partitions-${messageSize}b-${producersPerTopic}p-${consumerPerSubscription}c-${producerRate}"
topics: [1]
partitionsPerTopic: [16]
messageSize: [1024]
payloadFile: "payload/payload-1Kb.data"
keyDistributor: "NO_KEY"
useRandomizedPayloads: false
randomBytesRatio: 0.0
randomizedPayloadPoolSize: 0
subscriptionsPerTopic: [1]
consumerPerSubscription: [1, 2, 4]
producersPerTopic: [1, 2, 4]
producerRate: [10000, 50000]
consumerBacklogSizeGB: 0
testDurationMinutes: 15
warmupDurationMinutes: 1
```

These fields are lists, and the tool writes the Cartesian product of their values:

`topics`, `partitionsPerTopic`, `messageSize`, `subscriptionsPerTopic`, `producersPerTopic`,
`consumerPerSubscription`, `producerRate`.

The other fields are single values copied into every workload. The example produces
`1 × 1 × 1 × 1 × 3 × 3 × 2 = 18` files. The template does not support `messageSizeDistribution` or
`backlogDrainRatio`; add them to the generated files by hand if needed. With a fixed
`payloadFile`, list only one `messageSize`, because the file length must match it.

## Names

`nameFormat` builds each workload's `name` and its file name, `<name>.yaml`. Any template field can
be used as `${field}`. Counts are abbreviated (`50000` becomes `50k`, `2000000` becomes `2m`), and a
`producerRate` of 10,000,000 or more is written as `max-rate`. Without `nameFormat`, the default is
the format shown above.

Because results are named after the workload file, a descriptive format makes result files easy to
group later.

## Running the tool

Build the project with JDK 27, create the tool's classpath and run it from the repository root:

```bash
mvn -DskipTests install
mvn -q -pl tool dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
mkdir -p my-workloads
java -cp "tool/target/classes:$(cat tool/target/classpath.txt)" \
  io.openmessaging.benchmark.tool.workload.WorkloadGenerationTool \
  --template-file template.yaml --output-folder my-workloads
```

The tool logs `Generated 18 workloads`. See
[Building the project](../development/building-the-project.md) for build prerequisites.

## Running the generated set

```bash
bin/benchmark --drivers driver-kafka/kafka-latency.yaml --workers-file workers.yaml my-workloads/*.yaml
```

Every workload runs in turn and writes its own result file. Leave out `-o`, which would make all of
them write to the same file. A sweep of 18 workloads at 16 minutes each takes almost five hours, so
estimate the total before starting.
