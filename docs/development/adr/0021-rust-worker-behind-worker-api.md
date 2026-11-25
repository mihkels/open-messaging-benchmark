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

# ADR 0021: Add an optional Rust worker behind the existing worker API

- **Status:** Proposed
- **Date:** 2026-11-25
- **Issue:** [#588](https://github.com/mihkels/open-messaging-benchmark/issues/588)
- **Scope:** new `worker-rs` directory, `benchmark-framework` (`worker` package, `ResultMetadata`),
  `performance-harness`, `docker`, `deployment`, `e2e-tests/compose`

## Context

The worker, its hot path and every driver are Java. A Rust rewrite was proposed for lower harness
overhead: no garbage collector or JIT warm-up in the load generator, fewer workers per GB/s and a
smaller image. A full rewrite is about 36k lines of main code and 50k lines of tests. It would also
change what the benchmark measures, because the client library is part of every result. Five
drivers have no usable Rust client (JMS, Artemis, BookKeeper, NATS Streaming and, at the time of
writing, RocketMQ 4). External plugins depend on the Java `driver-api` ([ADR 0004](0004-driver-plugin-bundles.md)).

No one has measured whether the Java harness limits a run today.
[Schedule-corrected latency](0009-schedule-corrected-latency.md) already charges harness stalls to
the result rather than hiding them.

## Decision drivers

- Results from different client stacks must never be compared without the reader knowing.
- Keep every existing driver, external plugins and the `driver-api` contract working.
- Keep the [result format](0011-versioned-result-format.md) and the
  [validity verdict](0010-run-validity-verdict.md) the same whichever worker produced the run.
- Spend rewrite effort only where a measurement shows the Java harness is the limit.

## Options considered

- **Keep Java only.** No cost. The harness overhead stays unknown, and a Rust stack cannot be offered.
- **Rewrite everything in Rust.** Drops five drivers and external plugins, needs a new plugin
  mechanism (Rust has no stable ABI) and breaks comparison with earlier results. Months of solo work
  with two codebases in the meantime.
- **Rust coordinator first.** The coordinator is not on the hot path, so this pays the rewrite cost
  for no gain in measurement.
- **Optional Rust worker that speaks the existing worker HTTP API.** Chosen. The coordinator already
  drives workers only over HTTP (`HttpWorkerClient`), so a worker written in another language plugs
  in at that seam. Java workers and drivers stay as they are.

## Decision

1. Before any Rust code, measure Java harness overhead with `performance-harness` and the loopback
   driver: maximum message rate per worker, GC pause times and how much schedule lag they add. The
   Rust worker goes ahead only if this shows the harness limits realistic workloads. Otherwise this
   ADR is rejected.
2. If it goes ahead, `worker-rs` implements the worker HTTP API, including its
   [security defaults](0012-worker-api-secure-by-default.md), and returns statistics the Java
   coordinator reads without changes. Histograms use the HdrHistogram V2 compressed encoding, which
   the Rust `hdrhistogram` crate shares with the Java library.
3. Rust drivers are compiled into `worker-rs` behind cargo features. There is no dynamic plugin
   loading in Rust. External plugins stay Java only.
4. A Rust driver is added only when its client is official or actively maintained. Kafka, NATS,
   Redis, RabbitMQ and MQTT 5 are the candidates.
5. Every result records the worker runtime and the client library with its version in
   `ResultMetadata`, as an additive field. Reports and run checks treat a comparison across client
   stacks as a separate stack, never as a regression or an improvement.

## Consequences

- Users get both a Java-client and a Rust-client number for the same broker and can see the
  difference between clients.
- Two worker implementations must keep the same API and statistics semantics. A shared contract test
  runs both against the loopback driver and fails when their results differ beyond a set tolerance.
- The Rust toolchain becomes a build dependency, with rustfmt, clippy (`-D warnings`), cargo-deny
  and cargo-audit as its gates, in the spirit of [ADR 0007](0007-static-analysis-gates.md). The Java
  build does not need Rust.
- A second image, `docker.io/mihkels/open-messaging-benchmark-worker-rs`, is published on release tags under ADRs
  [0002](0002-docker-hub-only.md), [0015](0015-images-on-release-tags.md) and
  [0018](0018-supply-chain-attestations.md). The Helm chart gets an option to pick the worker image.
- Kafka through librdkafka is a different client from the Java client, so its numbers describe a
  different stack. Docs must say this wherever Rust results appear.
- A full rewrite and a Rust plugin SPI are ruled out unless a later ADR supersedes this one.
- **Enforced by:** the overhead measurement in step 1 (gate), the cross-worker contract test, the
  `ResultMetadata` tests and the Compose cases for each Rust driver. None exist yet.

