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

# ADR 0012: Secure the worker HTTP API by default

- **Status:** Accepted, implemented
- **Date:** 2026-11-17
- **Issue:** epics [#196](https://github.com/mihkels/open-messaging-benchmark/issues/196) and
  [#430](https://github.com/mihkels/open-messaging-benchmark/issues/430);
  [#189](https://github.com/mihkels/open-messaging-benchmark/issues/189) in
  [#199](https://github.com/mihkels/open-messaging-benchmark/pull/199),
  [#188](https://github.com/mihkels/open-messaging-benchmark/issues/188) in
  [#203](https://github.com/mihkels/open-messaging-benchmark/pull/203),
  [#421](https://github.com/mihkels/open-messaging-benchmark/issues/421) in
  [#440](https://github.com/mihkels/open-messaging-benchmark/pull/440),
  [#217](https://github.com/mihkels/open-messaging-benchmark/issues/217) in
  [#447](https://github.com/mihkels/open-messaging-benchmark/pull/447)
- **Scope:** `benchmark-framework` (worker), the Helm chart, coordinator to worker traffic

## Context

The worker API had no authentication, listened on all interfaces and accepted cross-site requests.
The driver configuration it accepts is powerful: it names classes to load and can point the worker
at any host, so being able to reach port 8080 meant being able to read and delete files on the
worker host. A hostile web page could drive a local worker blind with CORS "simple" POSTs or reach
it through DNS rebinding. The Helm NetworkPolicy protected Kubernetes only, not bare-metal, VM or
Docker setups, and a token sent in clear text can be read on shared networks.

## Decision drivers

- Safe without configuration on any network, not only inside Kubernetes.
- Local development on loopback keeps working without setup.
- Secrets never appear in process listings.

## Options considered

- **Accept requests without a JSON or YAML content type for one release, with a warning.**
  Rejected for a hard break with no grace period.
- **Pass the token as a flag value.** Rejected: it would show in `ps`.
- **Mutual TLS.** Deferred: the bearer token already authenticates the coordinator.
- **Host-name checks against DNS rebinding.** Not used, because workers are reached by pod IP.

## Decision

- Every POST must have an `application/json` or `application/yaml` content type, or it gets 415.
  Authentication runs first.
- The bearer token comes only from `OMB_WORKER_TOKEN` or `--token-file` and is compared in
  constant time. `/metrics` stays open unless `--protect-metrics` is set.
- A worker on a non-loopback bind without a token refuses to start, unless `--insecure-no-auth` is
  given. Loopback binds start with a warning.
- The Helm chart enables authentication by default and generates the token Secret.
- TLS is opt-in and adds to the token; it never replaces it.

## Consequences

- Breaking changes: coordinators and workers must be upgraded together, and a worker on the default
  `0.0.0.0` bind without a token exits.
- The Compose e2e stacks stay on plain HTTP.
- **Enforced by:** `BearerTokenAuthTest`, `AuthenticatedWorkerHttpTest`, `WorkerHttpRoundTripTest`,
  `BenchmarkWorkerArgumentsTest`, `WorkerTlsTest` and the chart lint in `mise run chart:lint`. Users
  see the rules under Authentication and TLS in the [CLI reference](../../reference/cli.md).

