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

The host is very slow or overloaded. Raise the limit with
`SUPERVISOR_HEAP_OPTS="-Xmx1G -Domb.driver.startupTimeoutSeconds=300"`, which the launch scripts pass
to the coordinator and worker JVM. Setting the variable replaces its `-Xmx1G` default, so keep the
heap flag.

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

**`HTTP post http://worker:8080/create-producers timed out after 600000 ms`**

The named worker did not answer that request in time, usually because its driver call is blocked on
the broker, for example while fetching metadata for many topics. Check that worker's log for the
call it was running. If the call is slow but healthy, raise `OMB_WORKER_REQUEST_TIMEOUT_SECONDS` on
the coordinator and all workers; see [Runtime and plugins](../reference/runtime-and-plugins.md#environment-variables).

**`Backlog drain stalled: messages received has not moved for 300 s`**

During a `consumerBacklogSizeGB` or `consumerPause` run, consumers (drain) or producers (build,
reported as `Backlog build stalled: messages sent ...`) made no progress for the stall window. The
message carries the backlog, its target and the summed counters; in distributed mode the log line
before it lists the counters of each worker, so the worker whose count stands still is the one to
check. If the drain is slow but still moving between checks, raise `OMB_STALL_TIMEOUT_SECONDS` on
the coordinator; see [Runtime and plugins](../reference/runtime-and-plugins.md#environment-variables).

**`Worker http://worker:8080 is unhealthy: driver process is EXITED with exit code 137`**

The coordinator's watchdog found that worker's driver process dead during the run and failed the
workload; it also logs every worker's counters next to it. Exit code 137 usually means the driver
was killed for running out of memory: raise the worker memory limit or lower `HEAP_OPTS`. For other
codes, check that worker's log, or `curl` its `/health` with the token for the last error. A
reason of `unreachable after 3 health checks` means the worker itself went away, for example a
restarted pod. See [Monitoring](monitoring.md#coordinator-watchdog).

**`No workload progress for 300 s; coordinator thread dump`**

No report interval was logged for that long. The `main` thread's stack shows what the coordinator
is waiting on, often a worker request; check that worker's log for the same call.

**`Cannot probe producer sustainable rate when building backlog`**

`producerRate: 0` cannot be combined with `consumerBacklogSizeGB`. Set a fixed rate.

**`Payload length mismatch`**

The payload file size differs from `messageSize`. See [Payloads and keys](payloads-and-keys.md).

**`At least two workers are required`**

Distributed mode needs two or more workers. For a single machine, omit `--workers` and run in local
mode.

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

To find which phase a problem started in, run with `OMB_LOG_FORMAT=json` and filter by the
`run_id` and `phase` fields, for example
`jq -c 'select(.event == "phase" or .["log.level"] == "WARN")' benchmark-worker.log`; the `phase`
list in the result tags every period the same way. See [JSON logs](monitoring.md#json-logs).

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
