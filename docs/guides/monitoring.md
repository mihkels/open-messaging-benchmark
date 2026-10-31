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

## Worker metrics

Each worker serves Prometheus metrics on its HTTP port:

```bash
curl http://worker-1:8080/metrics
```

The response combines the worker supervisor's registry with the metrics of its current driver
process. Once a driver is initialized, it includes:

|              Metric              |  Type   |                   Meaning                   |
|----------------------------------|---------|---------------------------------------------|
| `messages_sent_total`            | Counter | Messages acknowledged by the broker         |
| `message_send_errors_total`      | Counter | Failed sends                                |
| `bytes_sent_total`               | Counter | Payload bytes sent                          |
| `messages_recv_total`            | Counter | Messages received by consumers              |
| `bytes_recv_total`               | Counter | Payload bytes received                      |
| `produce_latency_seconds`        | Timer   | Publish latency                             |
| `producer_delay_latency_seconds` | Timer   | Delay between the scheduled and actual send |
| `e2e_latency_seconds`            | Timer   | End-to-end latency                          |

Timers expose `_count`, `_sum` and `_max` series. A minimal Prometheus scrape configuration:

```yaml
scrape_configs:
  - job_name: omb-workers
    scrape_interval: 10s
    static_configs:
      - targets: ["worker-1:8080", "worker-2:8080", "worker-3:8080", "worker-4:8080"]
```

In local mode there is no worker HTTP server; use the JSON result and the logs.

## Logs

Processes log to the console and to rolling files of up to 100 MB in their working directory:

|                                   Process                                    |          File          |
|------------------------------------------------------------------------------|------------------------|
| Coordinator (`bin/benchmark`) and worker supervisor (`bin/benchmark-worker`) | `benchmark-worker.log` |
| Driver process started by either of them                                     | `benchmark-driver.log` |

Driver process output is also forwarded to the parent's console. Useful messages include
`Starting driver process for ...`, `All consumers are ready`, `Producing worker count: ...`,
`Failed to run the workload ...` and client library warnings about timeouts or disconnects.

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

## Hosts and brokers

Collect at least:

- CPU, memory, network throughput and retransmits on worker hosts. A saturated worker CPU or network
  interface invalidates the run.
- CPU, disk throughput, disk latency, network and broker-specific metrics on the broker hosts, such
  as under-replicated partitions, request queue time, journal or ledger write latency, and memory
  pressure.
- Clock offset between hosts when measuring end-to-end latency across machines.

Align the time ranges of these metrics with the run's measured period, which starts after the
warm-up and, for backlog workloads, after the drain. See
[Benchmark design](../methodology/benchmark-design.md).
