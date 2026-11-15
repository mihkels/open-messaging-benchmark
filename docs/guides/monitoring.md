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

# Monitoring

The JSON result describes what the clients observed. To explain a result, and to detect a run that
was limited by the load generator instead of the system under test, also watch the workers, the
brokers and the hosts.

## Worker health

Each worker answers `GET /health` on its HTTP port with the state of its driver process:

```bash
curl http://worker-1:8080/health
```

```json
{ "ready" : true, "driver" : "EXITED", "exitCode" : 137, "lastError" : null, "clockMicros" : 1763200000123456 }
```

|     Field     |                                                                             Meaning                                                                             |
|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ready`       | The worker can take a driver; `false`, with status 503, once it is shutting down or failed to clean up a driver                                                 |
| `driver`      | `NOT_STARTED`, `INITIALIZING`, `RUNNING` or `EXITED`                                                                                                            |
| `exitCode`    | Exit code of a driver process that has exited                                                                                                                   |
| `lastError`   | The last driver failure; only sent with a valid bearer token, since it can name brokers and configuration                                                       |
| `clockMicros` | The worker's wall clock, in microseconds since the epoch; the coordinator uses it to [measure clock offset](../methodology/latency-and-backlog.md#clock-offset) |

`/health` needs no token even with `OMB_WORKER_TOKEN` or `--token-file` set, so load balancers and
Kubernetes probes can use it; without the token it shows only the state and exit code.

### Coordinator watchdog

While a workload runs, the coordinator polls `/health` of every worker every 10 seconds on a
virtual thread. When a worker's driver is not `RUNNING`, or the worker cannot be reached three polls
in a row, it logs the counters of every worker and fails the workload with
`Worker <url> is unhealthy: <reason>`, for example
`driver process is EXITED with exit code 137`. Set the interval with
`OMB_WATCHDOG_HEALTH_INTERVAL_SECONDS` or `-Domb.watchdog.healthIntervalSeconds`.

When no report interval is logged for 5 minutes, the coordinator logs its own thread dump with the
message `No workload progress for <n> s`, and again every 5 minutes while the run stays stuck. The
`main` thread's stack shows the call the run is waiting on. Set the timeout with
`OMB_WATCHDOG_PROGRESS_TIMEOUT_SECONDS` or `-Domb.watchdog.progressTimeoutSeconds`. Topic,
producer and consumer creation and the wait for consumers to be ready log no report intervals, so a
long one can log a dump without being stuck. Backlog build and drain keep logging report intervals;
their own stall check (`OMB_STALL_TIMEOUT_SECONDS`, see
[Troubleshooting](troubleshooting.md)) fails a backlog whose counters stop moving.

## Worker metrics

Each worker serves Prometheus metrics on its HTTP port:

```bash
curl http://worker-1:8080/metrics
```

The response combines the worker supervisor's registry with the metrics of its current driver
process. Once a driver is initialized, it includes:

|               Metric               |  Type   |                                                           Meaning                                                            |
|------------------------------------|---------|------------------------------------------------------------------------------------------------------------------------------|
| `messages_sent_total`              | Counter | Sends completed successfully by the driver                                                                                   |
| `message_send_errors_total`        | Counter | Failed sends                                                                                                                 |
| `bytes_sent_total`                 | Counter | Payload bytes sent                                                                                                           |
| `messages_recv_total`              | Counter | Messages received by consumers                                                                                               |
| `bytes_recv_total`                 | Counter | Payload bytes received                                                                                                       |
| `produce_latency_seconds`          | Timer   | Publish latency                                                                                                              |
| `producer_delay_latency_seconds`   | Timer   | Delay between the scheduled and actual send                                                                                  |
| `produce_schedule_latency_seconds` | Timer   | Intended send to successful completion (schedule-corrected publish latency)                                                  |
| `e2e_latency_seconds`              | Timer   | End-to-end latency                                                                                                           |
| `delivery_sent_total`              | Counter | With `verifyDelivery`: acknowledged stamped messages                                                                         |
| `delivery_received_total`          | Counter | With `verifyDelivery`: stamped messages received for the first time per subscription                                         |
| `delivery_duplicated_total`        | Counter | With `verifyDelivery`: stamped messages received again on the same subscription                                              |
| `delivery_out_of_order_total`      | Counter | With `verifyDelivery`: stamped messages received after a later one from the same producer                                    |
| `benchmark_phase`                  | Gauge   | 1 for the current run phase (`phase` label: `warmup`, `backlog_build`, `backlog_drain`, `measure`, `delivery_drain`), else 0 |

Lost messages are not a worker metric: a message is lost when no worker received it, so the
coordinator derives `lost` from all workers' counts after the run and writes it to the result.

Each driver process serves one workload run, so its metrics carry `run_id` and `driver` (the
driver configuration's `name`) labels. Match `run_id` to the `runId` of the JSON result to find
the workload.

The driver's send-completion point depends on its broker and configuration; it is not always a
broker acknowledgement. See [Publish latency](../methodology/latency-and-backlog.md#publish-latency).
Timers expose `_count`, `_sum` and `_max` series. A minimal Prometheus scrape configuration:

```yaml
scrape_configs:
  - job_name: omb-workers
    scrape_interval: 10s
    static_configs:
      - targets: ["worker-1:8080", "worker-2:8080", "worker-3:8080", "worker-4:8080"]
```

`/metrics` needs no token even when the worker requires one, unless the worker runs with
`--protect-metrics`; then add `authorization: {credentials_file: /path/to/token}` to the scrape job.
See [Authentication](../reference/cli.md#authentication).

JVM resource gauges are exported separately as `omb_resource_*`, with `role="supervisor"` and
`role="driver"` labels so the combined endpoint retains process identity. They include process
CPU seconds, heap used/committed bytes, non-heap/direct-buffer bytes, platform-thread count,
GC count/collection seconds, and PID. Unsupported management readings are `NaN`.
These gauges are read on scrape, not updated on the message hot path. Collection time is not a
GC-pause distribution, and heap is not resident memory.
For broker-independent resource measurements, profiling, and repeatable baseline comparisons,
see [Framework resource efficiency](../development/framework-performance.md).

In local mode there is no worker HTTP server; use the JSON result and the logs, or push the
metrics with OpenTelemetry.

## OpenTelemetry export

Workers can also push the same metrics to an OpenTelemetry collector over OTLP/HTTP, which suits
short-lived Kubernetes jobs that a scraper can miss. It is off by default and adds nothing when
off; `/metrics` keeps working when it is on. Enable it on every worker (or on the coordinator in
local mode) with:

```bash
export OMB_METRICS_OTLP=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
export OTEL_RESOURCE_ATTRIBUTES=service.instance.id=worker-1
```

The supervisor and every driver process it starts push their own metrics, configured with the
standard OpenTelemetry variables:

|                      Variable                       |                        Default                        |
|-----------------------------------------------------|-------------------------------------------------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT`                       | `http://localhost:4318` (`/v1/metrics` is appended)   |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`               | Full metrics URL; overrides the one above             |
| `OTEL_EXPORTER_OTLP_HEADERS`                        | None; for example `authorization=Bearer ...`          |
| `OTEL_METRIC_EXPORT_INTERVAL`                       | `10000` milliseconds                                  |
| `OTEL_SERVICE_NAME`                                 | `omb-worker`                                          |
| `OTEL_RESOURCE_ATTRIBUTES`                          | None; set `service.instance.id` to tell workers apart |
| `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` | `cumulative`                                          |

On Kubernetes, set `service.instance.id` from the pod name with the downward API. Metric names and
the `run_id`, `driver`, `phase` and `role` attributes are the same as on `/metrics`. A driver
process pushes its last interval when it stops, so a run's final counts arrive even when the job
ends right after it.

Timers export count, sum and max only. Use the JSON result for latency percentiles.

A minimal collector configuration that receives OTLP and serves it to Prometheus, for example as the
data source of a Grafana dashboard:

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  prometheus:
    endpoint: 0.0.0.0:8889
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
```

## Logs

Processes log to the console and to rolling files of up to 100 MB in their working directory:

|                                   Process                                    |          File          |
|------------------------------------------------------------------------------|------------------------|
| Coordinator (`bin/benchmark`) and worker supervisor (`bin/benchmark-worker`) | `benchmark-worker.log` |
| Driver process started by either of them                                     | `benchmark-driver.log` |

Driver process output is also forwarded to the parent's console. Useful messages include
`Starting driver process for ...`, `All consumers are ready`, `Workers list - producers: ...`,
`Failed to run the workload ...` and client library warnings about timeouts or disconnects.

### JSON logs

Set `OMB_LOG_FORMAT=json` (or `-Domb.log.format=json`) to write every appender as one
[ECS](https://www.elastic.co/guide/en/ecs/current/index.html) JSON object per line instead of text.
Driver processes inherit the setting. Coordinator lines of a workload run carry `run_id`,
`workload` and `driver` fields; period lines also carry `phase`.

Each phase change logs one event with `"event": "phase"`, the new `phase`, the run context and
`@timestamp`:

```json
{"@timestamp":"2026-01-01T10:00:00.000Z","log.level":"INFO","message":"Run phase: MEASURE","log.logger":"io.openmessaging.benchmark.WorkloadGenerator","event":"phase","phase":"MEASURE","run_id":"018f...","workload":"quick","driver":"Kafka"}
```

Workers log `Run phase: ...` too and expose the current phase as the `benchmark_phase` gauge, so a
dashboard can shade warm-up and backlog phases.

## JVM and garbage collection

The `bin/benchmark` and `bin/benchmark-worker` scripts append `JVM_OPTS` to the driver process
options, for example to log garbage collection:

```bash
export JVM_OPTS="-Xlog:gc*:file=driver-gc.log:time,uptime"
```

Do not use `OMB_DRIVER_JVM_OPTS` for this: it replaces all driver JVM options, including the heap
size and required `--add-opens` flags.

Long GC pauses in the driver process show up as publish delay and latency spikes that have nothing
to do with the broker. See [Runtime and plugins](../reference/runtime-and-plugins.md) for heap
settings.

## Load-generator saturation

When a worker runs out of CPU or spends much of its time in GC pauses, throughput flattens and
latency climbs as if the broker had reached its limit. Each worker reports its process CPU use,
as a share of its available cores, and its GC pause time, as a share of the period, with every
period's stats. The coordinator records the highest values across workers in the result's
`workerHealth` series.

When any measured period goes over `checks.maxWorkerCpu` (default `0.85`) or `checks.maxWorkerGc`
(default `0.1`), the `generatorSaturation` [run check](results.md#run-checks) logs a warning and
adds a finding, which makes the [validity verdict](results.md#validity-verdict) `warning`:

```text
Run check generatorSaturation (WARNING): Worker CPU use reached 93% in the measured window, above 85%; results may be limited by the load generator, add workers
```

Add workers or give them more cores, then rerun. Workers older than this check send no load, so
the series is absent and the check does not run.

The max-rate probe for `producerRate: 0` logs every step at info level, with the rate it chose,
the backlogs, the latest p99 publish delay and why it ramped up or backed off, and records the same
steps in the result's `rateProbe`:

```text
Rate probe at 30 s: 80000 -> 52000 msg/s (receiveBacklog; receive backlog 41000, publish backlog 0, publish delay p99 850 us)
```

## Hosts and brokers

Collect at least:

- CPU, memory, network throughput and retransmits on worker hosts. A saturated worker CPU or network
  interface invalidates the run.
- CPU, disk throughput, disk latency, network and broker-specific metrics on the broker hosts, such
  as under-replicated partitions, request queue time, journal or ledger write latency, and memory
  pressure.
- Clock offset between hosts when measuring end-to-end latency across machines.

Align the time ranges of these metrics with the run's measured period, which starts after the
warm-up. For backlog workloads it includes the backlog build-up and drain, followed by
`testDurationMinutes`. See
[Benchmark design](../methodology/benchmark-design.md).
