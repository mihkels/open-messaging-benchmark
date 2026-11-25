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

# Coming from the original OMB

This project started as a fork of the original OpenMessaging Benchmark,
[openmessaging/benchmark](https://github.com/openmessaging/benchmark), in December 2024 and is now
developed independently. It does not merge changes from the original OMB, and the original OMB does
not ship this project's plugin runtime, drivers or tooling. This page lists what carries over from
an original OMB setup and what has to change.

## What carries over

|        Item        |       Status        |                                                                                                                                                           What to do                                                                                                                                                           |
|--------------------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Workload YAML      | Compatible          | Example workloads from the original OMB load unchanged. Unknown keys and missing required fields are rejected; see [Workload schema](../reference/workload-schema.md).                                                                                                                                                         |
| Driver YAML        | Edit class names    | The keys are the same, but benchmark classes moved from `io.openmessaging.benchmark.driver.*` to `io.github.mihkels.omb.driver.*`. Update `driverClass`, and `delegateForAdminOperationsClassName` in JMS files; leave provider classes such as `connectionFactoryClassName` as they are. Start from the files in `driver-*/`. |
| Result JSON        | Superset            | Fields from the original OMB are kept and new ones are added; see [Results schema](../reference/results-schema.md).                                                                                                                                                                                                            |
| Custom driver code | Port required       | Drivers compile against `driver-api` in `io.github.mihkels.omb.driver` and ship as plugin bundles; see [Packaging a plugin](../development/packaging-a-plugin.md).                                                                                                                                                             |
| Maven coordinates  | Changed             | The groupId is `io.github.mihkels.omb` instead of `io.openmessaging.benchmark`.                                                                                                                                                                                                                                                |
| Container images   | Changed             | Images are published to `docker.io/mihkels/open-messaging-benchmark`.                                                                                                                                                                                                                                                          |
| Workers            | Not interchangeable | Do not mix original OMB workers and this project's workers in one run. Workers here need a shared `OMB_WORKER_TOKEN` and expose extra endpoints; see [Distributed runs](distributed-runs.md).                                                                                                                                  |
| Java               | Java 27             | Builds and runs need JDK 27.                                                                                                                                                                                                                                                                                                   |

## Main additions

- Every driver runs as a plugin in its own JVM, so independent drivers never share client
  libraries. A driver that loads a delegate, such as JMS with its Pulsar admin delegate, shares a JVM
  with it and the framework, so those dependencies must stay compatible; see
  [Runtime and plugins](../reference/runtime-and-plugins.md).
- A Docker Compose regression suite runs every plugin end to end against its broker; see
  [Testing a driver](../development/testing-a-driver.md).
- Workloads and driver files are validated against published JSON Schemas before a run starts.

## License

The project is licensed under Apache 2.0 and includes code from the original OMB, which is also licensed
under Apache 2.0.
