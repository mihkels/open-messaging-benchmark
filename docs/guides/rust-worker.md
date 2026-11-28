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

# Rust worker

`worker-rs` is an optional benchmark worker written in Rust
([ADR 0021](../development/adr/0021-rust-worker-behind-worker-api.md)). It speaks the same worker
HTTP API as the Java `benchmark-worker`, so the Java coordinator drives it with `--workers` and no
other change. Java workers, Java drivers and external plugins are unaffected.

This first version is a preview with one driver only, the loopback driver, for measuring the load
generator itself. Broker drivers ([#605](https://github.com/mihkels/open-messaging-benchmark/issues/605))
and a Docker image with a Helm option ([#606](https://github.com/mihkels/open-messaging-benchmark/issues/606))
come later.

## Results name their client stack

A Rust worker runs a different load generator, and its broker drivers will use different client
libraries than the Java drivers. Every result records the worker implementations in
`metadata.workerRuntime` (`java`, `rust`, or `java,rust` for a mixed run); see the
[results schema](../reference/results-schema.md). Compare results only within one runtime.

## Build and run

`worker-rs` needs the Rust toolchain that `worker-rs/rust-toolchain.toml` pins; `rustup` installs it on
first use. The Java build does not need Rust.

```bash
cd worker-rs && cargo build --release
```

The worker takes the options of the Java worker, with the same secure defaults
([ADR 0012](../development/adr/0012-worker-api-secure-by-default.md)): every request except
`/health` needs the bearer token from `--token-file` or `OMB_WORKER_TOKEN`, and without a token it
refuses to listen on a non-loopback address unless `--insecure-no-auth` is set.

```bash
OMB_WORKER_TOKEN=change-me worker-rs/target/release/worker-rs --bind 0.0.0.0 --port 8080
```

`--port 0` picks a free port; the worker prints `worker-rs listening on <address>` once it accepts
connections. It serves HTTP/1.1 and h2c on one port, as the coordinator expects.

## Loopback driver

The built-in driver is the equivalent of the `SyntheticDriver` of `performance-harness`. Either
`driverClass: io.github.mihkels.omb.performance.SyntheticDriver` or `driverClass: synthetic`
selects it, so one driver file runs against both workers:

```yaml
name: synthetic
driverClass: io.github.mihkels.omb.performance.SyntheticDriver
mode: sync
# For consumer-only workers of a distributed run: an independent receive stream at this rate
receiveRate: 0
```

`mode` is `sync` (deliver on the sending thread) or `send-only`; `async` is not supported. A send
reaches every subscription of its topic once, round-robin across the subscription's consumers.
Message keys are not generated, since the loopback driver ignores them.

Each producer sends on its own thread at the worker's rate divided by the number of producers, with
the same schedule-corrected publish delay as the Java worker
([ADR 0009](../development/adr/0009-schedule-corrected-latency.md)).

## Not supported yet

The coordinator gets an HTTP 400 that names the option for workloads that use `verifyDelivery`,
`headers` or `partitionTelemetry`. The worker has no `/metrics` endpoint and no TLS
(`--tls-cert`), and it does not report CPU load, so the `generatorSaturation` check does not fire
for Rust workers. [#607](https://github.com/mihkels/open-messaging-benchmark/issues/607) tracks these gaps.

## Cross-worker contract test

`CrossWorkerContractTest` in `performance-harness` runs the same loopback workload (50,000 msg/s,
2 producers, 2 subscriptions of 2 consumers, 1 KB payloads) against an in-process Java worker and
`worker-rs`, through the coordinator's own worker client. It passes when, for each worker:

- the achieved rate is within 2% of the target, and within 2% of the target of the other worker's;
- every sent message is received once per subscription, within 1%;
- the latency histograms count every measured send and receive, within 1%;
- the publish delay and publish latency medians are below 1 ms.

It runs only when `omb.workerRs` names the binary, in the `Rust worker` CI workflow:

```bash
mvn -Pperformance -pl performance-harness -am install -Dtest=CrossWorkerContractTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Domb.workerRs="$PWD/worker-rs/target/release/worker-rs"
```

## Checks

The `Rust worker` workflow runs `cargo fmt --check`, `cargo clippy -D warnings`, the unit tests,
`cargo-deny` (advisories, licenses, bans and sources, configured in `worker-rs/deny.toml`) and
`cargo-audit`, then the contract test. It runs only when `worker-rs`, the Java worker package or
`performance-harness` change.
