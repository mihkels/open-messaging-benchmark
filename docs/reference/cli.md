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

# CLI

Both commands are shell scripts in `bin/`. Run them from the distribution directory, the Docker
image's `/benchmark` directory or a built source checkout. Environment variables that affect them
are listed in [Runtime and plugins](runtime-and-plugins.md).

## bin/benchmark

The coordinator. It runs workloads against drivers and writes results.

```text
bin/benchmark [options] <workload.yaml>...
```

|         Option          |       Argument        |                                                             Description                                                             |
|-------------------------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `-d`, `--drivers`       | Comma-separated paths | Driver configuration files. Every workload runs once with each driver                                                               |
| `-w`, `--workers`       | Comma-separated URLs  | Worker addresses such as `http://10.0.0.1:8080`. Two or more enable distributed mode                                                |
| `-wf`, `--workers-file` | Path                  | YAML file with a `workers:` list of URLs. Cannot be combined with `--workers`                                                       |
| `-x`, `--extra`         |                       | In distributed mode, use about a third of the workers as producers instead of half                                                  |
| `-o`, `--output`        | Path                  | Write the result to this file instead of a generated name                                                                           |
| `-c`, `--csv`           | Directory             | Convert all `*.json` results in the directory to CSV and exit                                                                       |
| `--legacy-driver-name`  | Path                  | Local mode only: load the driver from this plugin bundle directory, driver module directory or jar instead of the installed plugins |
| `-h`, `--help`          |                       | Print usage and exit                                                                                                                |

Positional arguments are workload files.

### Behavior

- **Worker selection.** Without `--workers` and `--workers-file`, a `workers.yaml` file in the
  current directory is used if present. Otherwise the run is local and the coordinator starts the
  driver in a child JVM.
- **Order.** Workloads run in alphabetical order of their file names, and for each workload the
  drivers run in the order given. Two workload files with the same file name in different
  directories collide; only one runs.
- **Output.** Each workload and driver combination writes
  `<workload file name>-<driver name>-<yyyy-MM-dd-HH-mm-ss>.json` to the current directory, or the
  `--output` path. With `--output`, every combination overwrites the same file.
- **Failures.** If a combination fails, the coordinator logs
  `Failed to run the workload '<name>' for driver '<config>'` with the cause and continues with the
  next combination. The process exit status does not report the failure; check the log or the
  presence of each result file.
- **Validation errors** such as unknown options, or `--workers` combined with `--workers-file`,
  print a message and exit with a non-zero status. `--help` also exits with a non-zero status.
- **Logging.** Output goes to the console and to `benchmark-worker.log` in the current directory.

### Examples

```bash
# Local run
bin/benchmark -d driver-kafka/kafka-latency.yaml workloads/1-topic-16-partitions-1kb.yaml

# Distributed run, two drivers, two workloads
bin/benchmark -d driver-kafka/kafka-latency.yaml,driver-pulsar/pulsar.yaml -wf workers.yaml \
  workloads/1-topic-16-partitions-1kb.yaml workloads/1-topic-100-partitions-1kb.yaml

# Convert results to CSV
bin/benchmark --csv results/
```

## bin/benchmark-worker

A worker supervisor that accepts commands from a coordinator over HTTP and runs the selected driver
in a child JVM.

```text
bin/benchmark-worker [options]
```

|        Option         | Default |                     Description                     |
|-----------------------|---------|-----------------------------------------------------|
| `-p`, `--port`        | 8080    | HTTP port for coordinator commands and `/metrics`   |
| `-sp`, `--stats-port` | 8081    | Deprecated and unused. It must differ from `--port` |
| `-h`, `--help`        |         | Print usage and exit                                |

The worker keeps running until it is stopped. Initializing a different driver stops the previous
driver process. Stopping the worker stops its driver process.

### HTTP interface

The coordinator uses these endpoints; they are not intended to be called by hand, except
`/metrics`.

| Method |                            Path                             |                            Purpose                            |
|--------|-------------------------------------------------------------|---------------------------------------------------------------|
| POST   | `/initialize-driver`                                        | Start a driver process with the posted configuration          |
| POST   | `/create-topics`                                            | Create topics                                                 |
| POST   | `/create-producers`, `/create-consumers`                    | Create clients for the assigned topics and subscriptions      |
| POST   | `/probe-producers`                                          | Send one message per topic to check that consumers are ready  |
| POST   | `/start-load`, `/adjust-publish-rate`                       | Start publishing, change the rate                             |
| POST   | `/pause-consumers`, `/resume-consumers`                     | Used for backlog workloads                                    |
| POST   | `/reset-stats`, `/stop-all`                                 | Reset statistics, stop all clients                            |
| GET    | `/period-stats`, `/cumulative-latencies`, `/counters-stats` | Statistics polled by the coordinator                          |
| GET    | `/metrics`                                                  | Prometheus metrics; see [Monitoring](../guides/monitoring.md) |

The interface has no authentication. Run workers on a trusted network.

## bin/create_charts.py

```text
python3 bin/create_charts.py <result.json>...
```

Requires the `pygal` Python package. Writes SVG charts to the current directory. See
[Results](../guides/results.md).
