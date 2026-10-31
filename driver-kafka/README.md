# Apache Kafka benchmarks

For instructions on running the OpenMessaging bencmarks for Kafka, see the [official documentation](http://openmessaging.cloud/docs/benchmarks/kafka).

For a local Kafka-backed plugin check, run
`e2e-tests/compose/run-e2e.sh --stacks kafka`; see the
[Compose regression suite](../e2e-tests/compose/README.md) for requirements and reports.

Workloads can use randomized payloads by setting `useRandomizedPayloads`, `randomBytesRatio`, and
`randomizedPayloadPoolSize` in the workload YAML. To convert benchmark result JSON files in a
directory to a CSV in the current directory, run:

```bash
bin/benchmark --csv path/to/results
```

The [legacy AWS deployment](deploy/README.md) requires adaptation for Java 27 and plugin bundles.
