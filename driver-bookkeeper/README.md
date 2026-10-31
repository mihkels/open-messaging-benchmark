# Apache BookKeeper benchmarks

Apache BookKeeper is the storage used by Apache Pulsar. This driver allows benchmarking Apache BookKeeper directly using the OpenMessaging benchmark
framework. For more details see the official pulsar [benchmark documentation](http://openmessaging.cloud/docs/benchmarks/pulsar/).

For local BookKeeper and DistributedLog plugin checks, run
`e2e-tests/compose/run-e2e.sh --stacks bookkeeper`; see the
[Compose regression suite](../e2e-tests/compose/README.md) for requirements and reports.
