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

|         Option          |       Argument        |                                                                                           Description                                                                                            |
|-------------------------|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-d`, `--drivers`       | Comma-separated paths | Driver configuration files. Every workload runs once with each driver                                                                                                                            |
| `-w`, `--workers`       | Comma-separated URLs  | Worker addresses such as `http://10.0.0.1:8080`. Distributed mode needs at least two; a single worker is rejected with `At least two workers are required`, or one `dns://<host>:<port>` address |
| `-wf`, `--workers-file` | Path                  | YAML file with a `workers:` list of URLs, or one `dns://` address. Cannot be combined with `--workers`                                                                                           |
| `--expected-workers`    | Count                 | With a `dns://` address, wait until this many workers resolve. Default `2`, the minimum                                                                                                          |
| `--workers-timeout`     | Duration              | With a `dns://` address, how long to wait for `--expected-workers`, such as `30s` or `5m`. Default `5m`                                                                                          |
| `--worker-token-file`   | Path                  | File with the bearer token to send to remote workers; overrides `OMB_WORKER_TOKEN`. See [Authentication](#authentication)                                                                        |
| `--worker-ca-file`      | Path                  | Certificate that `https://` workers present (their `--tls-cert`); overrides `OMB_WORKER_CA_FILE`. With `dns://`, workers are reached over `https://`. See [TLS](#tls)                            |
| `-x`, `--extra`         |                       | In distributed mode, use about a third of the workers as producers instead of half                                                                                                               |
| `-o`, `--output`        | Path                  | Write the result to this file instead of a generated name                                                                                                                                        |
| `--report-html <file>`  |                       | Also write an [HTML report](../guides/html-report.md) of every run in this invocation to `<file>`                                                                                                |
| `--include-histograms`  |                       | Include compressed aggregated latency histograms in the result JSON                                                                                                                              |
| `--retries`             | Count                 | Rerun a combination up to this many times after a worker, IO, timeout or stall failure; default `3` (up to 4 attempts), `0` disables. See [Behavior](#behavior)                                  |
| `--fail-on-invalid`     |                       | Exit with status `3` when a completed run's [validity](../guides/results.md#validity-verdict) is `invalid`. See [Exit codes](#exit-codes)                                                        |
| `--fail-on-warning`     |                       | Exit with status `3` when a completed run's validity is `warning` or `invalid`                                                                                                                   |
| `--repeat`              | Count                 | Run each combination this many times and write a summary of the run-to-run spread; overrides the workload's `repeat`. Default `1`. See [Behavior](#behavior)                                     |
| `-c`, `--csv`           | Directory             | Convert all `*.json` results in the directory to `results-<epoch seconds>.csv` in the current directory and exit                                                                                 |
| `--driver-location`     | Path                  | Local mode only: load the driver from this plugin bundle directory, driver module directory or jar instead of the installed plugins                                                              |
| `-h`, `--help`          |                       | Print usage and exit                                                                                                                                                                             |

Positional arguments are workload files. `--legacy-driver-name` is accepted as a deprecated alias of
`--driver-location`.

### Exit codes

| Status |                                                           Meaning                                                           |
|--------|-----------------------------------------------------------------------------------------------------------------------------|
| `0`    | Every combination completed, passed its thresholds and, with `--fail-on-*`, met the validity bar                            |
| `2`    | A completed run failed a workload threshold                                                                                 |
| `3`    | With `--fail-on-invalid`, a completed run is `invalid`; with `--fail-on-warning`, a completed run is `warning` or `invalid` |
| other  | A run, cleanup or configuration failed, or the command line is invalid                                                      |

When a run both fails a threshold and fails the validity bar, the status is `3`: a measurement that
is not trustworthy makes its threshold result meaningless. A failed run or cleanup wins over both,
because the coordinator then exits with an exception.

With `--repeat`, `--fail-on-*` judges each repetition's result and the
[repetition summary](../guides/results.md#repeated-runs) verdict, which is the worst of its
repetitions plus its `runToRunVariance` and `failedRepetitions` findings. Thresholds use only the
summary's medians.

### Behavior

- **Worker selection.** Without `--workers` and `--workers-file`, a `workers.yaml` file in the
  current directory is used if present. Otherwise the run is local and the coordinator starts the
  driver in a child JVM.
- **DNS discovery.** `--workers dns://<host>:<port>` resolves every A/AAAA record of `<host>`
  and uses `http://<ip>:<port>` for each, in sorted order; the resolved list is logged. This suits
  a Kubernetes headless Service, which publishes only ready pods. The coordinator retries every
  second until `--expected-workers` addresses resolve and fails with
  `Found <n> of <N> expected workers at ...` after `--workers-timeout`. A `dns://` address cannot
  be combined with other worker addresses.
- **Order.** Workloads run in alphabetical order of their file names, and for each workload the
  drivers run in the order given. Two workload files with the same file name in different
  directories collide; only one runs.
- **Output.** Each workload and driver combination writes
  `<workload file name>-<driver name>-<yyyy-MM-dd-HH-mm-ss>.json` to the current directory, or the
  `--output` path. `--output` is accepted only for a single workload and driver; with more
  combinations the coordinator rejects it before running, because each would overwrite the file.
- **Failures.** If a combination fails, the coordinator logs
  `Failed to run the workload '<name>' for driver '<config>'` with the cause and continues with the
  next combination when it is safe to do so. The process exits with a non-zero status if any
  combination or required cleanup fails; results from successful combinations remain available.
  A completed run that fails a workload threshold exits with status `2`, distinct from a run or
  configuration error. See [Exit codes](#exit-codes) and [CI gating](../guides/ci-gating.md).
- **Retries.** A combination that fails with a worker, IO, HTTP timeout, connection, stall or
  health-watchdog failure is rerun from the start, up to `--retries` more times: the coordinator
  stops every worker, initializes the driver again and logs
  `Retrying the workload '<name>' for driver '<config>', attempt <n> of <N>, after: <reason>`.
  Results come only from the attempt that succeeds; its file records `attempts` and the reason of
  each earlier failure in `failedAttempts` (see [Results schema](results-schema.md#retries)). A
  failed attempt's failure is not reported again once a later attempt succeeds. Configuration
  errors (invalid driver YAML, a workload the driver rejects) and JVM errors fail at once, and a run
  that already produced its measurement is not rerun when only its cleanup fails. When every attempt
  fails, the coordinator continues with the next combination if the workers stopped cleanly.
  Treat a retried result as a warning rather than a clean pass.
- **Repetitions.** With `--repeat N` (or `repeat: N` in the workload) above 1, each combination
  runs N times in a row. Every repetition creates its own topics, producers and consumers and
  removes them when it ends, and gets its own `--retries`. Each repetition writes its own result:
  `-o result.json --repeat 3` writes `result-1.json`, `result-2.json` and `result-3.json`, and
  without `-o` the generated name gets the suffix, as in
  `<workload>-<driver>-<timestamp>-2.json`. After the last repetition the coordinator writes
  `result-summary.json` (or `<workload>-<driver>-<timestamp>-summary.json`) with the median, min,
  max and coefficient of variation of the main metrics; see
  [Repeated runs](../guides/results.md#repeated-runs). A repetition that fails after its retries
  is listed as failed in the summary and left out of the statistics, and the process still exits
  with a non-zero status. Workload thresholds decide the exit status from the medians, not from
  each repetition. `--repeat 1` behaves exactly like a run without it.
- **Validation errors** such as unknown options, `--workers` combined with `--workers-file`, no
  workload files, no `--drivers`, `--expected-workers` below 2, negative `--retries`, `--repeat` below 1, or a workload that fails
  validation (pool size, size distribution, backlog settings) print a message and exit with a
  non-zero status before any broker resources are created. Payload file errors (missing file, length mismatch) are detected only after topics,
  producers and consumers have been created. `--help` prints usage and exits successfully.
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

|        Option         | Default |                                                       Description                                                       |
|-----------------------|---------|-------------------------------------------------------------------------------------------------------------------------|
| `-p`, `--port`        | 8080    | HTTP port for coordinator commands and `/metrics`                                                                       |
| `-b`, `--bind`        | 0.0.0.0 | Address to listen on; `127.0.0.1` accepts only local connections                                                        |
| `--token-file`        |         | File with the bearer token every request must send; overrides `OMB_WORKER_TOKEN`. See [Authentication](#authentication) |
| `--protect-metrics`   |         | Require the token on `/metrics` too; it is open by default                                                              |
| `--insecure-no-auth`  |         | Start without a token on a non-loopback `--bind`; the API is then open to anyone who reaches the port                   |
| `--tls-cert`          |         | PEM certificate; with `--tls-key`, serve HTTPS instead of HTTP. See [TLS](#tls)                                         |
| `--tls-key`           |         | PEM private key (PKCS#8 or PKCS#1 RSA) for `--tls-cert`                                                                 |
| `-sp`, `--stats-port` |         | Deprecated and ignored; logs a warning if passed                                                                        |
| `-h`, `--help`        |         | Print usage and exit                                                                                                    |

Keep the default `0.0.0.0` for distributed workers that a coordinator on another host must reach;
it needs a token (see [Authentication](#authentication)). Use `--bind 127.0.0.1` when the
coordinator runs on the same host.

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

Every `POST` must send `Content-Type: application/json`, or `application/yaml` for
`/initialize-driver`; parameters such as `; charset=utf-8` are allowed. Any other type, or none,
gets `415 Unsupported Media Type`. Browsers can send `text/plain`, form and multipart requests to any
host without a CORS preflight, so this stops a web page from driving a worker on `localhost` or the
LAN. Workers and coordinators from before this check do not interoperate with newer ones: run the
same version on all of them.

### Authentication

Workers require a shared bearer token unless they listen only on a loopback address. Give the worker the token in the `OMB_WORKER_TOKEN`
environment variable or in a file named by `--token-file` (which wins when both are set);
surrounding whitespace is trimmed. There is no option that takes the token itself, so it never shows
in the process list. With a token, every request without `Authorization: Bearer <token>` gets
`401 Unauthorized`, checked before anything else, including the Content-Type check above.
`/metrics` stays open so Prometheus can scrape it without the token; `--protect-metrics` closes it
too.

The coordinator sends the header to every remote worker when `OMB_WORKER_TOKEN` is set or
`--worker-token-file` is given; use the same token on all workers. A coordinator without the token
gets `401` from a worker that requires one.

```bash
export OMB_WORKER_TOKEN="$(openssl rand -hex 32)"
bin/benchmark-worker                                        # on each worker host, same token
bin/benchmark -d driver-kafka/kafka-latency.yaml -w http://w1:8080,http://w2:8080 workloads/simple.yaml
```

A worker without a token refuses to start on a non-loopback `--bind` address, such as the default
`0.0.0.0`, and names the fix. On a loopback address, or with `--insecure-no-auth`, it starts with an
unauthenticated API and logs a warning. Over plain HTTP the token travels in clear text: use
[TLS](#tls), or run workers on a trusted network and keep the port closed to everything but the
coordinator (and Prometheus). The supervisor's own channel to its driver
process on `127.0.0.1` does not use this token, and the driver process does not get it.

The worker rejects any request with an `Origin` header with `403 Forbidden`, because only browsers
send one. A worker without a token also rejects a `Host` that is not an IP address or `localhost`,
which stops a web page from reaching it through DNS rebinding. Address a tokenless worker by IP,
for example `-w http://10.0.0.5:8080`, not by host name.

### TLS

Workers serve HTTPS instead of HTTP when given a PEM certificate and key. One self-signed
certificate is shared by all workers, and the coordinator trusts exactly that certificate:

```bash
bin/benchmark-tls-cert tls                       # writes tls/worker.crt and tls/worker.key (needs openssl)
bin/benchmark-worker --tls-cert tls/worker.crt --tls-key tls/worker.key   # on each worker, same files
bin/benchmark --worker-ca-file tls/worker.crt -w https://w1:8080,https://w2:8080 workloads/simple.yaml
```

- `OMB_WORKER_CA_FILE` can stand in for `--worker-ca-file`. An `https://` worker without either is
  a usage error. With a `dns://` address the discovered workers are reached over `https://` when a
  certificate is configured.
- The coordinator does not check the worker's host name, because workers are reached by IP (pod IPs
  in Kubernetes) or names the certificate does not list. Anyone holding the workers' private key can
  impersonate a worker, so keep `worker.key` on the workers only; the coordinator needs just
  `worker.crt`. The bearer token still authenticates the coordinator, so use both.
- The key may be PKCS#8 (`BEGIN PRIVATE KEY`, as `bin/benchmark-tls-cert` writes) or PKCS#1 RSA
  (`BEGIN RSA PRIVATE KEY`, as Helm and cert-manager write by default). Convert other keys with
  `openssl pkcs8 -topk8 -nocrypt -in KEY -out KEY.pk8`.
- With TLS the worker answers only HTTPS on `--port`, `/metrics` included. The supervisor's channel to
  its driver process stays plain HTTP on `127.0.0.1`.

## bin/benchmark-report

```text
bin/benchmark-report diff [options] BASELINE CANDIDATE
bin/benchmark-report diff [options] --baseline <file>... --candidate <file>...
bin/benchmark-report html [-o <file>] <result.json>...
```

### diff

Compares baseline result files with candidate result files. See
[Comparing runs](../guides/comparing-runs.md).

|            Option            | Default |                                   Description                                   |
|------------------------------|---------|---------------------------------------------------------------------------------|
| `--baseline <file>...`       |         | Baseline result files, repeated runs of the same setup                          |
| `--candidate <file>...`      |         | Candidate result files, repeated runs of the same setup                         |
| `--format <format>`          | `text`  | Output format: `text`, `markdown` or `json`                                     |
| `--fail-on-regression <pct>` |         | Exit with status `2` when a metric regresses by at least `<pct>`, such as `10%` |
| `--min-change <pct>`         | `1%`    | Smallest relative change reported as improved or regressed                      |
| `--allow-workload-mismatch`  |         | Compare runs of different workloads with a warning instead of refusing          |
| `--output <file>`            | stdout  | Write the report to a file                                                      |

Exit status `0` means the comparison passed, `1` means bad arguments, an unreadable file or a
workload mismatch, and `2` means a regression past `--fail-on-regression`. `HEAP_OPTS` overrides
the default `-Xmx512m`.

### html

Renders one or more result files as one HTML page with charts; each file is one run. See
[HTML report](../guides/html-report.md).

|         Option          | Default |        Description         |
|-------------------------|---------|----------------------------|
| `-o`, `--output <file>` | stdout  | Write the report to a file |

Exit status `0` means the report was written, `1` means bad arguments, an unreadable file or a
failed write.
