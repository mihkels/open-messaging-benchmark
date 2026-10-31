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

# Architecture

This page explains how the benchmark runtime fits together, from the coordinator down to the
isolated driver JVM. It is for people changing the framework or writing a driver. To run
benchmarks, start with the [Quickstart](../getting-started/quickstart.md) and
[Distributed runs](../getting-started/distributed-runs.md).

The classes mentioned here are in
[`benchmark-framework/src/main/java/io/openmessaging/benchmark`](https://github.com/mihkels/open-messaging-benchmark/tree/main/benchmark-framework/src/main/java/io/openmessaging/benchmark),
mostly in the `worker` and `plugin` packages.

## Components at a glance

```text
coordinator JVM: bin/benchmark  (io.openmessaging.benchmark.Benchmark)
  WorkloadGenerator --> Worker
                          |
      local mode          |          distributed mode (--workers / --workers-file)
  +-----------------------+-------------------------------+
  |                                                       |
  v                                                       v
ProcessWorker                                  DistributedWorkersEnsemble
  |                                              | HttpWorkerClient per worker
  |                                              v
  |                          worker JVM: bin/benchmark-worker  (BenchmarkWorker, "supervisor")
  |                            Javalin on -p <port> (default 8080)
  |                            WorkerHandler routes + GET /metrics
  |                              --> ProcessWorker
  |                                    |
  +------------------+-----------------+
                     | HTTP on 127.0.0.1:<ephemeral port> + stdin pipe
                     v
driver JVM  (DriverProcessMain)
  classpath = benchmark core + selected plugin bundle(s)
  Javalin on 127.0.0.1, WorkerHandler routes + GET /metrics + POST /initialize-driver-local
    --> LocalWorker  (rate limiting, statistics, ConsumerCallback)
          --> BenchmarkDriver implementation from the plugin bundle
                --> broker
```

The runtime uses three kinds of JVM:

|     JVM     |                      Main class                       |                             Started by                              |                           Classpath                           |
|-------------|-------------------------------------------------------|---------------------------------------------------------------------|---------------------------------------------------------------|
| Coordinator | `io.openmessaging.benchmark.Benchmark`                | `bin/benchmark`                                                     | Benchmark core                                                |
| Supervisor  | `io.openmessaging.benchmark.worker.BenchmarkWorker`   | `bin/benchmark-worker`                                              | Benchmark core                                                |
| Driver      | `io.openmessaging.benchmark.worker.DriverProcessMain` | `ProcessWorker`, in the coordinator (local mode) or in a supervisor | Benchmark core plus the jars of the selected plugin bundle(s) |

Drivers never appear on the coordinator or supervisor classpath.

## Coordinator (`Benchmark`)

`Benchmark` parses the command line (see [CLI](../reference/cli.md)), reads the workload files and
creates a single `Worker`:

- If `--workers` or `--workers-file` is given, or a `workers.yaml` file exists in the working
  directory, it creates one `HttpWorkerClient` per worker URL and wraps them in a
  `DistributedWorkersEnsemble`. In this mode `--legacy-driver-name` is ignored and a warning is logged.
- Otherwise it creates a `ProcessWorker` in its own JVM. This is **local mode**: the coordinator
  supervises one driver process itself.

For each workload and each driver file, the coordinator:

1. calls `stopAll()` to stop any workload still running;
2. calls `initializeDriver(driverConfig, driverLocation)`, where `driverLocation` is the
   `--legacy-driver-name` path or `null`;
3. runs a `WorkloadGenerator`, writes the `TestResult` as JSON (to `--output`, otherwise
   `<workload>-<driver name>-<yyyy-MM-dd-HH-mm-ss>.json`), and calls `stopAll()` again.

`WorkloadGenerator.run()` calls the `Worker` interface in this order:

1. `createTopics`. Topic names are `<getTopicNamePrefix()>-<7-digit index>-<random suffix>`.
2. `createConsumers`, one entry per topic, subscription (`sub-<3-digit index>-<random>`) and consumer,
   in shuffled order.
3. `createProducers`, each topic repeated `producersPerTopic` times, in shuffled order.
4. The readiness check: `probeProducers()`, then a poll of the counters every 2 s until at least
   `topics × subscriptionsPerTopic` messages have been received. After 60 s it fails with
   `Timed out waiting for consumers to be ready`.
5. `startLoad` with the prepared payloads, publish rate and key distributor.
6. Optional warm-up and optional backlog build and drain (`pauseConsumers` / `resumeConsumers`).
   When `producerRate` is 0, a rate controller also calls `adjustPublishRate`.
7. `resetStats`, the measured phase (periodic `getPeriodStats`, then a final
   `getCumulativeLatencies`), and `stopAll`.

## The `Worker` interface

Every layer implements
[`Worker`](https://github.com/mihkels/open-messaging-benchmark/blob/main/benchmark-framework/src/main/java/io/openmessaging/benchmark/worker/Worker.java).
The same calls therefore pass through the ensemble, over HTTP, through the supervisor and into the
driver process.

|        Implementation        |                                                               Role                                                               |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `DistributedWorkersEnsemble` | Splits work across remote workers and aggregates their statistics                                                                |
| `HttpWorkerClient`           | Sends each call to a `WorkerHandler` over HTTP. A non-200 response becomes an `IllegalStateException` carrying the response body |
| `ProcessWorker`              | Starts the driver JVM and forwards every call to it through an `HttpWorkerClient`                                                |
| `LocalWorker`                | Runs in the driver JVM. Loads the `BenchmarkDriver`, generates load and records statistics                                       |

## Worker supervisor (`BenchmarkWorker`)

`bin/benchmark-worker` starts `BenchmarkWorker`, which:

- creates a `ProcessWorker` and a `WorkerHandler` for it;
- uses Javalin to serve the handler routes and `GET /metrics` on `-p` / `--port` (default 8080);
- on JVM shutdown, stops the HTTP server and closes the worker, which stops the driver process.

`-sp` / `--stats-port` is deprecated and no longer starts a listener. The worker still checks that
its value differs from `--port`.

### `WorkerHandler` routes

[`WorkerHandler`](https://github.com/mihkels/open-messaging-benchmark/blob/main/benchmark-framework/src/main/java/io/openmessaging/benchmark/worker/WorkerHandler.java)
maps HTTP routes to `Worker` methods. The supervisor and the driver process serve the same routes.

| Method |          Path           |                                        `Worker` call                                        |
|--------|-------------------------|---------------------------------------------------------------------------------------------|
| POST   | `/initialize-driver`    | `initializeDriver`. The body is the driver YAML, which is first written to a temporary file |
| POST   | `/create-topics`        | `createTopics(TopicsInfo)`, which returns the created topic names                           |
| POST   | `/create-producers`     | `createProducers(List<String> topics)`                                                      |
| POST   | `/probe-producers`      | `probeProducers()`                                                                          |
| POST   | `/create-consumers`     | `createConsumers(ConsumerAssignment)`                                                       |
| POST   | `/pause-consumers`      | `pauseConsumers()`                                                                          |
| POST   | `/resume-consumers`     | `resumeConsumers()`                                                                         |
| POST   | `/start-load`           | `startLoad(ProducerWorkAssignment)`                                                         |
| POST   | `/adjust-publish-rate`  | `adjustPublishRate(double)`                                                                 |
| POST   | `/stop-all`             | `stopAll()`                                                                                 |
| GET    | `/period-stats`         | `getPeriodStats()`                                                                          |
| GET    | `/cumulative-latencies` | `getCumulativeLatencies()`                                                                  |
| GET    | `/counters-stats`       | `getCountersStats()`                                                                        |
| POST   | `/reset-stats`          | `resetStats()`                                                                              |

Any exception becomes an HTTP 500 response whose body lists the exception chain
(`Class: message | caused by: ...`).

The driver process also serves `POST /initialize-driver-local`. Its body is the absolute path of a
driver YAML file that already exists on the same host.

## Distributed mode (`DistributedWorkersEnsemble`)

[`DistributedWorkersEnsemble`](https://github.com/mihkels/open-messaging-benchmark/blob/main/benchmark-framework/src/main/java/io/openmessaging/benchmark/worker/DistributedWorkersEnsemble.java)
needs at least two workers. It takes the workers in the order given on the command line and splits
them into producer workers (the start of the list) and consumer workers (the rest):

| `--extra` / `-x` |          Producer workers          |   Consumer workers    |
|------------------|------------------------------------|-----------------------|
| not set          | `workers / 2` (rounded down)       | the remaining workers |
| set              | `(workers + 2) / 3` (rounded down) | the remaining workers |

For example, with 4 workers and no `--extra`, workers 1 and 2 produce and workers 3 and 4 consume.
With 6 workers and `--extra`, workers 1 and 2 produce and workers 3 to 6 consume. A comment in the
source says the extra consumer workers are needed for the JMS driver.

Calls are routed as follows:

|                              Call                              |                                           Sent to                                            |
|----------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| `initializeDriver`, `stopAll`, `resetStats`                    | every worker                                                                                 |
| `createTopics`                                                 | only the first worker in the list (the leader)                                               |
| `createProducers`                                              | producer workers, with the topic list partitioned across them                                |
| `createConsumers`                                              | consumer workers, with the topic/subscription list partitioned across them                   |
| `probeProducers`                                               | producer workers                                                                             |
| `startLoad`, `adjustPublishRate`                               | producer workers; the rate is divided by the number of producer workers that received topics |
| `pauseConsumers`, `resumeConsumers`                            | consumer workers                                                                             |
| `getPeriodStats`, `getCumulativeLatencies`, `getCountersStats` | every worker, with the results summed                                                        |

Two consequences matter for driver authors:

- Producers and consumers of the same topic run in **different driver processes**, usually on
  different hosts. Messages must go through the broker, and the consumer host's clock is compared with
  the publish timestamp set on the producer host.
- Only the leader's driver instance creates topics. The other workers' driver instances must be able
  to produce to and consume from topics they did not create.

## Isolated driver JVM (`ProcessWorker` and `DriverProcessMain`)

[`ProcessWorker`](https://github.com/mihkels/open-messaging-benchmark/blob/main/benchmark-framework/src/main/java/io/openmessaging/benchmark/worker/ProcessWorker.java)
runs the selected driver in a child JVM. On every `initializeDriver` call it:

1. stops the previous driver process, if one is running;
2. reads `driverClass` from the driver YAML and resolves the plugin bundle(s) through
   `PluginRegistry`, or from the given location when `--legacy-driver-name` is used in local mode;
3. builds the classpath with `RuntimeClasspath` and logs any warnings;
4. starts `java` from its own `java.home`. It passes an argument file in a temporary `omb-driver-*`
   directory, removes the `CLASSPATH` environment variable, and inherits stdout and stderr;
5. waits up to `omb.driver.startupTimeoutSeconds` (default 120 s) for the child to write its port
   file;
6. sends `POST /initialize-driver-local` with the path of the driver YAML.

The child JVM options come from the `omb.driver.jvmOpts` system property or the
`OMB_DRIVER_JVM_OPTS` environment variable. If neither is set, `ProcessWorker` uses its built-in
defaults. The supervisor forwards its `log4j.configurationFile`, `log4j2.configurationFile` and
`zone.id` system properties. It also sets `omb.log.name=benchmark-driver`, so the driver process logs
to `benchmark-driver.log` instead of `benchmark-worker.log`. All settings are listed in
[Runtime and plugins](../reference/runtime-and-plugins.md).

[`DriverProcessMain`](https://github.com/mihkels/open-messaging-benchmark/blob/main/benchmark-framework/src/main/java/io/openmessaging/benchmark/worker/DriverProcessMain.java)
creates a `LocalWorker` with its own `PrometheusMeterRegistry` and serves the `WorkerHandler` routes
on an ephemeral `127.0.0.1` port. It writes that port atomically to the `--port-file` and never
listens on a non-loopback address.

Process lifetime:

- The supervisor keeps the child's stdin open but never writes to it. When the child reads EOF
  (because the supervisor closed the pipe or exited), it calls `System.exit(0)`, and its shutdown hook
  runs `stopAll()` on the `LocalWorker`. A watchdog halts the process if shutdown takes longer than
  10 s.
- `ProcessWorker.stopAll()` stops only the producers, consumers and driver inside the child. The
  process keeps running until the next driver is initialized or the worker is closed.
- To terminate the child, the supervisor closes stdin and waits 10 s. If the process is still running,
  it destroys it and waits another 10 s, then kills it forcibly.
- If a forwarded call fails and the child has exited, the error message includes the exit code.

## Plugin discovery and the shared runtime

[`PluginRegistry`](https://github.com/mihkels/open-messaging-benchmark/blob/main/benchmark-framework/src/main/java/io/openmessaging/benchmark/plugin/PluginRegistry.java)
discovers bundles each time a driver is initialized. It searches these locations in order:

1. `<home>/drivers/*`, the binary distribution layout;
2. `<home>/*/target/omb-plugin`, bundles built in a source checkout;
3. each entry of the `omb.plugin.path` system property or the `OMB_PLUGIN_PATH` environment variable,
   separated by the platform path separator. An entry can be a bundle directory or a directory of
   bundles.

`<home>` is taken from the `omb.home` system property, then the `OMB_HOME` environment variable, then
the working directory. The launch scripts pass `-Domb.home="${OMB_HOME:-<installation directory>}"`.

An invalid bundle produces a warning and does not affect other drivers. If two bundles have the same
id, or two bundles provide the same driver class, selecting an affected driver class fails with an
error that names both bundles. A bundle that declares `classReferenceProperties` also pulls in the
bundles of the classes named by those driver YAML properties, and they are all loaded into the same
driver process.

[`RuntimeClasspath`](https://github.com/mihkels/open-messaging-benchmark/blob/main/benchmark-framework/src/main/java/io/openmessaging/benchmark/plugin/RuntimeClasspath.java)
puts the benchmark core first, followed by the bundle jars. The core is the artifact list recorded at
build time in `META-INF/omb/worker-runtime-artifacts.txt` (read by `HostRuntime`).
[`benchmark-framework/pom.xml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/benchmark-framework/pom.xml)
excludes coordinator-only HTTP client libraries and some other groups from that list. Bundle jars are
identified by their `META-INF/maven/**/pom.properties`:

- If a bundle ships a core artifact at the same version, the bundle copy is dropped.
- A different version of `driver-api`, Micrometer (`micrometer-core`, `-commons`, `-observation`,
  `-registry-prometheus`) or `prometheus-metrics-core` / `-model` is an error.
- A different version of any other core artifact is ignored with a warning, and the core version is
  used.
- Bundles loaded into one process must use the same version of each artifact and classifier. The
  exception is annotation-only artifacts: for those, the highest version is used and a warning is
  logged.

[Packaging a plugin](packaging-a-plugin.md#compatibility-rules) covers these rules in detail and lists
the annotation-only artifacts.

## Metrics ownership

The statistics used for results (`PeriodStats`, `CumulativeLatencies`, `CountersStats`) are kept by
`WorkerStats` in the driver process's `LocalWorker` and returned through the HTTP routes.

Prometheus metrics are split between processes:

- The driver process owns a `PrometheusMeterRegistry`. `WorkerStats` registers its meters there, and
  the same registry is passed to `BenchmarkDriver.initialize(...)` as `statsLogger` so the driver can
  add its own metrics. The driver process serves this registry at `/metrics` on its loopback port.
- The supervisor's `GET /metrics` returns the supervisor's own registry followed by a scrape of the
  driver process's `/metrics`. Before any driver has started, the driver part is empty.
- In local mode there is no supervisor HTTP server, so `/metrics` cannot be reached from outside the
  host.

To scrape these endpoints, see [Monitoring](../guides/monitoring.md).
