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

# Troubleshooting

Start with the first error in the coordinator output. Errors from a driver process are printed by
the coordinator in local mode and by the worker in distributed mode, so check the worker logs too.
Driver-specific problems are listed on each [driver page](../drivers/index.md).

## The driver cannot be loaded

**`No driver plugin bundle provides driver class ...`**

The `driverClass` in the driver configuration does not match any installed plugin. The message
lists the known driver classes and the searched directories.

- Check the spelling against the driver page.
- Run from a distribution, the Docker image or a source checkout built with
  `mvn -DskipTests install`. Plugins are looked up in `<home>/drivers/*`, in
  `<home>/*/target/omb-plugin` and in `OMB_PLUGIN_PATH`. See
  [Runtime and plugins](../reference/runtime-and-plugins.md).
- In distributed mode, each worker must have the plugin installed. The configuration is sent to the
  workers, the plugin is not.

**`Driver configuration ... must specify a driverClass`**

Add the `driverClass` listed on the driver page.

**`Driver process for ... exited during startup with code ...`**

The driver JVM failed before it started serving. The cause is printed just above. Common causes
are invalid JVM options in `OMB_DRIVER_JVM_OPTS` or `JVM_OPTS`, or a heap larger than the host or
container allows.

**`Driver process for ... did not start within 120s`**

The host is very slow or overloaded. Raise the limit by adding
`-Domb.driver.startupTimeoutSeconds=300` to `SUPERVISOR_HEAP_OPTS`, which the launch scripts pass to
the coordinator and worker JVM.

**`Failed to initialize driver ...`**

The driver started, but its initialization failed, usually because it could not reach the broker or
the configuration was invalid. The message contains the driver's exception.

## The run does not start

**`Timed out waiting for consumers to be ready`**

Consumers did not receive the probe message on every topic within 60 seconds. Check that:

- consumers can reach the broker, including advertised listener addresses for Kafka and similar
  systems;
- the subscription starts from the earliest position or is created before the probe is sent, as the
  driver page describes;
- the broker can create the requested number of topics and partitions quickly enough.

**`Cannot probe producer sustainable rate when building backlog`**

`producerRate: 0` cannot be combined with `consumerBacklogSizeGB`. Set a fixed rate.

**`Payload length mismatch`**

The payload file size differs from `messageSize`. See [Payloads and keys](payloads-and-keys.md).

**`At least two workers are required`**

Distributed mode needs two or more workers. For a single machine, omit `--workers` and run in local
mode.

**`Stats port must be different from HTTP port`**

`bin/benchmark-worker --port 8081` collides with the default value of the deprecated
`--stats-port`. Pass `--stats-port` with any other port.

## The run completes but the results look wrong

|                        Symptom                        |                                        Likely cause                                         |
|-------------------------------------------------------|---------------------------------------------------------------------------------------------|
| Publish rate below `producerRate`, high publish delay | The broker or the workers are saturated; add producer workers or lower the rate             |
| Backlog grows throughout the run                      | Consumers cannot keep up; add consumers or partitions, or the system is at capacity         |
| Consume rate is a multiple of the publish rate        | Expected with `subscriptionsPerTopic` above 1                                               |
| End-to-end latency implausibly low or high            | Clocks of producer and consumer hosts are not synchronized; non-positive values are dropped |
| Large periodic latency spikes                         | GC in the driver process or broker background work; see [Monitoring](monitoring.md)         |
| Results overwritten                                   | `-o` used with several workloads or drivers                                                 |
| Coordinator exits non-zero or a result is missing     | Check the first coordinator error and the worker logs; other successful results may exist   |

## Resource problems

- **Out of memory or a killed container.** The driver JVM uses a 4 GiB heap by default, plus the
  supervisor JVM. Lower `HEAP_OPTS` for small tests or raise the container memory limit.
- **Too many open files.** Workloads with many topics, partitions or producers need a high file
  descriptor limit on workers and brokers (`ulimit -n`).
- **Leftover topics.** Each run creates new topics. Delete them between runs if the broker slows
  down as metadata grows.

## Getting help

If the problem persists, open an issue in
[mihkels/open-messaging-benchmark](https://github.com/mihkels/open-messaging-benchmark/issues) with
the driver configuration, the workload, the coordinator log and the worker and driver logs.
