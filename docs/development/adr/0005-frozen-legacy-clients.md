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

# ADR 0005: Freeze legacy driver clients and pin their transitive libraries

- **Status:** Accepted, implemented
- **Date:** 2026-11-19
- **Issue:** [#475](https://github.com/mihkels/open-messaging-benchmark/issues/475), implemented in [#479](https://github.com/mihkels/open-messaging-benchmark/issues/479); related [#332](https://github.com/mihkels/open-messaging-benchmark/issues/332), [#538](https://github.com/mihkels/open-messaging-benchmark/issues/538)
- **Scope:** `driver-nsq`, `driver-nats-streaming`, `driver-pravega`, `driver-rocketmq`,
  `.github/dependabot.yml`

## Context

Each legacy client is at its final release: `nsq-client` 1.0.0.RC4, `java-nats-streaming` 2.2.3,
`pravega-client` 0.13.0 and `rocketmq-acl` 5.3.2. NATS Streaming is deprecated upstream and its
support ended in June 2023 ([#332](https://github.com/mihkels/open-messaging-benchmark/issues/332)). The clients cannot be upgraded, but the libraries they
pull in can be stale or vulnerable, for example `commons-beanutils` with CVE-2025-48734.

## Decision drivers

- Keep the legacy drivers working against their brokers.
- Remove known-vulnerable transitive libraries where a safe bump exists.
- Never change the API a frozen client was compiled against.

## Options considered

- **Upgrade or replace the clients.** Not possible for clients at their final release, and a
  rewrite is out of scope for drivers kept for comparison.
- **Leave the dependency tree as it is.** Keeps known CVEs in the bundles.
- **Pin transitive libraries to newer versions on the same major line.** Chosen. Moving across a
  major line was rejected case by case in [#479](https://github.com/mihkels/open-messaging-benchmark/issues/479): Pravega's `bcprov` needs an artifact swap,
  RocketMQ's Caffeine 3.x changes the API, jnats 2.6.5 to 2.27 is too large a jump for a frozen
  client, and protobuf 4.x breaks code generated against protobuf 3.9.1 ([#538](https://github.com/mihkels/open-messaging-benchmark/issues/538)).

## Decision

The client jars of the NSQ, NATS Streaming, Pravega and RocketMQ drivers stay at their final
releases. Stale transitive libraries are pinned in each driver's `dependencyManagement` to newer
versions on the same major line, and every pin is verified with the Compose e2e suite for that
driver. CVEs in the code of `nsq-client` itself cannot be fixed by a version bump and are not
tracked as issues.

## Consequences

- A pin cannot reach another driver, because each driver runs in its own plugin bundle
  ([ADR 0004](0004-driver-plugin-bundles.md)).
- Unit tests do not prove a pin is safe. CI passed the protobuf 4.x bump only because no driver test
  exercised protobuf serialization ([#538](https://github.com/mihkels/open-messaging-benchmark/issues/538)), so pins need an e2e run.
- Dependabot ignores `java-nats-streaming` and protobuf, so it does not propose updates the frozen
  NATS Streaming client cannot take.
- **Enforced by:** the `dependencyManagement` blocks in the four driver poms, the NATS Streaming and
  protobuf ignore rules in `.github/dependabot.yml`, and the "Security audits" section of `AGENTS.md`. The e2e
  verification is manual until [#106](https://github.com/mihkels/open-messaging-benchmark/issues/106) runs the suites in CI.

