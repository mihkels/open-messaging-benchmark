#!/usr/bin/env python3
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Broker-free framework experiments. Uses only Python's standard library and the project JDK."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import signal
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
MODULE = ROOT / "performance-harness"
SCHEMA = 1
COMPATIBILITY_FLAGS = [
    "--enable-native-access=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-opens=java.base/java.util.zip=ALL-UNNAMED",
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Dorg.apache.pulsar.shade.io.netty.tryReflectionSetAccessible=true",
    "-Dio.netty.leakDetection.level=disabled",
]


def command(args, **kwargs):
    return subprocess.check_output(args, text=True, cwd=ROOT, **kwargs).strip()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def classpath():
    path = MODULE / "target/classpath.txt"
    plugin = MODULE / "target/omb-plugin"
    if not path.is_file() or not plugin.is_dir():
        raise ValueError(
            "Build first: mvn -Pperformance -pl performance-harness -am "
            "--no-transfer-progress --batch-mode install -DskipTests"
        )
    return os.pathsep.join([str(MODULE / "target/classes"), path.read_text().strip()])


def jvm_options(args, role, directory):
    options = [
        f"-Xms{args.initial_heap if role == 'driver' else args.supervisor_initial_heap}",
        f"-Xmx{args.heap if role == 'driver' else args.supervisor_heap}",
        {"g1": "-XX:+UseG1GC", "parallel": "-XX:+UseParallelGC", "zgc": "-XX:+UseZGC"}[
            args.collector
        ],
        *COMPATIBILITY_FLAGS,
    ]
    if args.processors:
        options.append(f"-XX:ActiveProcessorCount={args.processors}")
    if args.scheduler_parallelism:
        options.append(f"-Djdk.virtualThreadScheduler.parallelism={args.scheduler_parallelism}")
    options.extend(shlex.split(args.driver_jvm_opts if role == "driver" else args.coordinator_jvm_opts))
    if args.profile:
        # %p is expanded by HotSpot, keeping every process's artifacts distinct.
        options.extend([
            f"-XX:StartFlightRecording=filename={directory}/"
            f"{role}-%p.jfr,settings=profile,dumponexit=true",
            f"-Xlog:gc*:file={directory}/{role}-%p-gc.log:time,uptime",
        ])
    return options


def environment():
    return {
        "os": platform.system(),
        "osRelease": platform.release(),
        "arch": platform.machine(),
        "host": platform.node(),
        "processors": os.cpu_count(),
        "jdk": command(["java", "-version"], stderr=subprocess.STDOUT),
        "gitRevision": command(["git", "rev-parse", "HEAD"]),
        "gitDirty": bool(command(["git", "status", "--porcelain"])),
        "gitDiff": command(["git", "diff", "--binary", "HEAD"]),
        "untrackedSha256": {
            name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
            for name in command(["git", "ls-files", "--others", "--exclude-standard"]).splitlines()
            if (ROOT / name).is_file()
        },
    }


def rss_bytes(pid):
    if not isinstance(pid, int) or pid <= 0:
        raise ValueError("RSS sampling requires a positive numeric PID")
    if sys.platform.startswith("linux"):
        try:
            status = Path(f"/proc/{pid}/status").read_text()
        except FileNotFoundError:
            return None
        match = re.search(r"^VmRSS:\s+(\d+)\s+kB$", status, re.MULTILINE)
        return int(match.group(1)) * 1024 if match else None
    if sys.platform == "darwin":
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "rss="], capture_output=True, text=True, check=False
        )
        if result.returncode == 1:  # Process exited between the sample and ps.
            return None
        result.check_returncode()
        text = result.stdout.strip()
        return int(text) * 1024 if text else None
    return None


def terminate(process):
    """The CLI creates its own process group; never signal an inherited group."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        process.poll()
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    process.wait(timeout=10)


def execute_scenario(args, scenario, directory, cp):
    directory.mkdir()
    write_json(directory / "scenario.json", scenario)
    driver_options = jvm_options(args, "driver", directory)
    coordinator_options = jvm_options(args, "coordinator", directory)
    write_json(directory / "requested-jvm-options.json", {
        "driver": driver_options, "coordinator": coordinator_options,
    })
    # ProcessWorker parses this quoted string itself; no shell/eval is involved.
    invocation = [
        "java", *coordinator_options,
        "-Domb.driver.jvmOpts=" + shlex.join(driver_options),
        "-cp", cp, "io.openmessaging.benchmark.performance.ScenarioMain",
        str(directory / "scenario.json"), str(directory), str(MODULE / "target/omb-plugin"),
    ]
    write_json(directory / "command.json", invocation)
    timeout = (
        scenario["warmupSeconds"] + scenario["measureSeconds"]
        if scenario["smoke"]
        else 60 * (scenario["warmupMinutes"] + scenario["measureMinutes"])
    ) + 180 + scenario["workers"] * 30
    rss_peaks = {}
    tree_peak = None
    last_sample = None
    offset = 0
    with (directory / "console.log").open("w") as console, \
            (directory / "rss-samples.jsonl").open("w") as rss_log:
        process = subprocess.Popen(
            invocation, cwd=ROOT, stdout=console, stderr=subprocess.STDOUT, start_new_session=True
        )
        started = time.monotonic()
        try:
            while process.poll() is None:
                if time.monotonic() - started > timeout:
                    raise TimeoutError(f"Experiment exceeded deadline; see {directory}")
                samples = directory / "samples.jsonl"
                if samples.exists():
                    with samples.open() as stream:
                        stream.seek(offset)
                        while True:
                            position = stream.tell()
                            line = stream.readline()
                            if not line or not line.endswith("\n"):
                                offset = position
                                break
                            last_sample = json.loads(line)
                if last_sample:
                    readings = {
                        role: rss_bytes(int(values["pid"]))
                        for role, values in last_sample["resources"].items()
                    }
                    rss_log.write(json.dumps({
                        "phase": last_sample["phase"],
                        "monotonicSeconds": time.monotonic(), "rssBytes": readings,
                    }) + "\n")
                    rss_log.flush()
                    if last_sample["phase"] == "measurement":
                        for role, value in readings.items():
                            if value is not None:
                                rss_peaks[role] = max(rss_peaks.get(role, 0), value)
                        if all(v is not None for v in readings.values()):
                            total = sum(readings.values())
                            tree_peak = max(tree_peak or 0, total)
                time.sleep(0.25)
            if process.returncode != 0:
                raise RuntimeError(f"Scenario exited with {process.returncode}; see {directory}/console.log")
        finally:
            terminate(process)
    result = json.loads((directory / "result.json").read_text())
    result["rssPeakBytes"] = rss_peaks
    result["treeRssPeakBytes"] = tree_peak
    result["summary"] = summarize(result, args.min_rate_ratio)
    write_json(directory / "result.json", result)
    return result


def summarize(result, min_rate_ratio=0.98):
    completed = result["completed"]
    reasons = []
    scenario = result["scenario"]
    expected_roles = {"local": 1, "process": 2, "http": 3}.get(
        scenario["topology"], 1 + 2 * scenario.get("workers", 2)
    )
    if len(result["resourceStart"]) != expected_roles:
        reasons.append("Missing process-role resource samples")
    if completed <= 0:
        reasons.append("No successful completed work")
    if result["errors"]:
        reasons.append("Send failures occurred")
    if result.get("fixtureReceiveErrors", 0):
        reasons.append("Synthetic receive callbacks failed")
    if result["achievedRate"] < scenario["rate"] * min_rate_ratio:
        reasons.append("Missed requested fixed load")
    # Snapshot/counter boundaries are sequential RPCs, not an atomic distributed barrier.
    tolerance = max(
        scenario["producers"] * scenario["topics"] * 2,
        completed * 0.02,
        scenario["rate"] * result.get("snapshotBoundarySeconds", 0),
    )
    for metric in ("publishHistogramSamples", "delayHistogramSamples"):
        if abs(result[metric] - completed) > tolerance:
            reasons.append(f"{metric} differs from completed work by more than boundary tolerance")
    if scenario["topology"] != "distributed" and scenario["mode"] != "send-only":
        if abs(result["received"] - completed) > tolerance:
            reasons.append("Loopback delivery did not keep up with completed sends")
    per_role = {}
    for role, start in result["resourceStart"].items():
        end = result["resourceEnd"].get(role)
        if end is None or start.get("pid") != end.get("pid"):
            reasons.append(f"Process identity changed: {role}")
            continue
        delta = {}
        for key in ("cpu_seconds", "gc_count", "gc_collection_seconds"):
            a, b = start.get(key), end.get(key)
            delta[key] = b - a if a is not None and b is not None else None
            if delta[key] is not None and delta[key] < 0:
                reasons.append(f"Counter decreased: {role}/{key}")
        delta["cpuNsPerMessage"] = (
            delta["cpu_seconds"] * 1e9 / completed
            if delta["cpu_seconds"] is not None and completed > 0 else None
        )
        per_role[role] = delta
    cpus = [value["cpu_seconds"] for value in per_role.values()]
    tree_cpu = sum(cpus) if cpus and all(v is not None for v in cpus) else None
    return {
        "valid": not reasons, "invalidReasons": reasons, "perRole": per_role,
        "treeCpuNsPerMessage": tree_cpu * 1e9 / completed if tree_cpu is not None and completed > 0 else None,
        "treeRssPeakBytes": result["treeRssPeakBytes"],
        "achievedRate": result["achievedRate"],
        "publishP99Micros": result.get("publishP99Micros"),
        "delayP99Micros": result.get("delayP99Micros"),
        "pollSeconds": result["pollSeconds"],
    }


def scenario_from(args):
    return {
        "topology": args.topology, "mode": args.mode, "rate": args.rate,
        "topics": args.topics, "producers": args.producers, "payloadBytes": args.payload_bytes,
        "workers": args.workers, "queueCapacity": args.queue_capacity, "threads": args.threads,
        "warmupSeconds": args.warmup_seconds, "measureSeconds": args.measure_seconds,
        "warmupMinutes": args.warmup_minutes, "measureMinutes": args.measure_minutes,
        "smoke": args.smoke, "weighted": args.weighted, "key": args.key,
    }


def run(args):
    cp = classpath()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schemaVersion": SCHEMA, "environment": environment(), "scenario": scenario_from(args),
        "profiled": args.profile, "minRateRatio": args.min_rate_ratio,
        "repetitions": args.repetitions, "status": "running",
    }
    write_json(output / "manifest.json", manifest)
    failures = []
    for index in range(args.repetitions):
        directory = output / f"run-{index + 1:03}"
        try:
            result = execute_scenario(args, manifest["scenario"], directory, cp)
            summary = result["summary"]
            print(f"{directory.name}: " + json.dumps(summary, allow_nan=False), flush=True)
            if not summary["valid"]:
                failures.append({"run": directory.name, "error": summary["invalidReasons"]})
        except (OSError, ValueError, RuntimeError, TimeoutError) as error:
            failures.append({"run": directory.name, "error": str(error)})
            write_json(directory / "failure.json", failures[-1])
            print(str(error), file=sys.stderr, flush=True)
    manifest["status"] = "failed" if failures else "complete"
    manifest["failures"] = failures
    write_json(output / "manifest.json", manifest)
    if failures:
        raise ValueError(f"{len(failures)} invalid or failed repetitions; artifacts: {output}")


def load_run(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest["schemaVersion"] != SCHEMA or manifest["status"] != "complete":
        raise ValueError(f"Unsupported, incomplete, or failed experiment: {directory}")
    files = sorted(directory.glob("run-*/result.json"))
    if len(files) != manifest["repetitions"]:
        raise ValueError(f"Missing repetitions: {directory}")
    results = [json.loads(path.read_text()) for path in files]
    if any(
        r["schemaVersion"] != SCHEMA
        or r.get("status") != "complete"
        or r["scenario"] != manifest["scenario"]
        or not r["summary"]["valid"]
        or not summarize(r, manifest["minRateRatio"])["valid"]
        for r in results
    ):
        raise ValueError(f"Invalid repetitions: {directory}")
    return manifest, results


def distribution(values):
    if any(v is None or not math.isfinite(v) for v in values):
        return {"median": None, "min": None, "max": None, "relativeRange": None}
    median = statistics.median(values)
    return {
        "median": median, "min": min(values), "max": max(values),
        "relativeRange": (max(values) - min(values)) / abs(median) if median else None,
    }


def print_comparison(report):
    def number(value):
        return "unavailable" if value is None else f"{value:.5g}"

    print("Metric | Baseline median [min,max] | Candidate median [min,max] | Delta")
    print("--- | --- | --- | ---")
    for name, row in report["metrics"].items():
        ranges = [
            f"{number(row[side]['median'])} [{number(row[side]['min'])},{number(row[side]['max'])}]"
            for side in ("baseline", "candidate")
        ]
        delta = row["deltaPercent"]
        label = name + (" (" + row["unit"] + ")" if "unit" in row else "")
        print(f"{label} | {ranges[0]} | {ranges[1]} | "
              + ("n/a" if delta is None else f"{delta:+.1f}%")
              + ("; noisy" if row.get("noisy") else ""))
    if report.get("insufficientRepetitions"):
        print("WARNING: fewer than three repetitions; exploratory evidence only.")
    print(report["interpretation"])


def compare(args):
    if getattr(args, "micro", False):
        return compare_micro(args)
    base, before = load_run(args.baseline)
    candidate, after = load_run(args.candidate)
    if base["scenario"]["smoke"] or candidate["scenario"]["smoke"]:
        raise ValueError("Smoke checks are not performance baselines")
    mismatches = []
    for key in ("scenario", "profiled", "minRateRatio"):
        if base[key] != candidate[key]:
            mismatches.append(key)
    for key in ("os", "osRelease", "arch", "host", "processors", "jdk"):
        if base["environment"][key] != candidate["environment"][key]:
            mismatches.append("environment." + key)
    if mismatches:
        raise ValueError("Incompatible experiments: " + ", ".join(mismatches))
    metrics = {}
    summaries = {
        name: ([r["summary"][name] for r in before], [r["summary"][name] for r in after])
        for name in before[0]["summary"]
        if name not in ("valid", "invalidReasons", "perRole")
    }
    roles = before[0]["summary"]["perRole"]
    if any(set(r["summary"]["perRole"]) != set(roles) for r in before + after):
        raise ValueError("Incompatible process roles across repetitions")
    for role, readings in roles.items():
        for name in readings:
            summaries[f"{role}/{name}"] = (
                [r["summary"]["perRole"][role][name] for r in before],
                [r["summary"]["perRole"][role][name] for r in after],
            )
    for name, (baseline_values, candidate_values) in summaries.items():
        a = distribution(baseline_values)
        b = distribution(candidate_values)
        metrics[name] = {
            "baseline": a, "candidate": b,
            "deltaPercent": (b["median"] / a["median"] - 1) * 100
            if a["median"] not in (0, None) and b["median"] is not None else None,
            "noisy": any(d["relativeRange"] is not None and d["relativeRange"] > 0.10 for d in (a, b)),
        }
    report = {
        "schemaVersion": SCHEMA, "baseline": str(args.baseline), "candidate": str(args.candidate),
        "baselineRevision": base["environment"]["gitRevision"],
        "candidateRevision": candidate["environment"]["gitRevision"],
        "baselineRepetitions": len(before), "candidateRepetitions": len(after),
        "insufficientRepetitions": min(len(before), len(after)) < 3,
        "metrics": metrics,
        "interpretation": "Descriptive comparison only; inspect spread, latency, role metrics, and raw artifacts.",
    }
    text = json.dumps(report, indent=2, allow_nan=False)
    if args.output:
        args.output.write_text(text + "\n")
    print_comparison(report)


def compare_micro(args):
    environments = [
        json.loads((directory / "environment.json").read_text())
        for directory in (args.baseline, args.candidate)
    ]
    for key in ("os", "osRelease", "arch", "host", "processors", "jdk"):
        if environments[0][key] != environments[1][key]:
            raise ValueError(f"Incompatible JMH environment: {key}")
    tables = []
    for directory in (args.baseline, args.candidate):
        rows = json.loads((directory / "jmh.json").read_text())
        if not rows:
            raise ValueError(f"No JMH results: {directory}")
        table = {}
        for row in rows:
            key = row["benchmark"] + "/" + json.dumps(row.get("params", {}), sort_keys=True)
            if key in table:
                raise ValueError(f"Duplicate JMH result: {key}")
            table[key] = row
        tables.append(table)
    if tables[0].keys() != tables[1].keys():
        raise ValueError("Incompatible JMH benchmark/parameter sets")
    metrics = {}
    settings = (
        "mode", "threads", "forks", "jvmArgs", "jdkVersion", "vmName", "vmVersion",
        "warmupIterations", "warmupTime", "warmupBatchSize", "measurementIterations",
        "measurementTime", "measurementBatchSize",
    )
    for name, a in tables[0].items():
        b = tables[1][name]
        if any(a.get(key) != b.get(key) for key in settings):
            raise ValueError(f"Incompatible JMH settings: {name}")
        before = {"primary": a["primaryMetric"], **a.get("secondaryMetrics", {})}
        after = {"primary": b["primaryMetric"], **b.get("secondaryMetrics", {})}
        if before.keys() != after.keys():
            raise ValueError(f"Incompatible JMH profiler metrics: {name}")
        for metric, baseline in before.items():
            candidate = after[metric]
            if baseline["scoreUnit"] != candidate["scoreUnit"]:
                raise ValueError(f"Incompatible JMH units: {name}/{metric}")
            x = distribution([v for fork in baseline["rawData"] for v in fork])
            y = distribution([v for fork in candidate["rawData"] for v in fork])
            metrics[f"{name}/{metric}"] = {
                "unit": baseline["scoreUnit"], "baseline": x, "candidate": y,
                "deltaPercent": (y["median"] / x["median"] - 1) * 100
                if x["median"] not in (0, None) and y["median"] is not None else None,
                "noisy": any(d["relativeRange"] is not None and d["relativeRange"] > 0.10 for d in (x, y)),
            }
    report = {
        "schemaVersion": SCHEMA, "kind": "jmh", "metrics": metrics,
        "interpretation": "Descriptive fork/iteration distributions; verify in whole-framework runs.",
    }
    text = json.dumps(report, indent=2, allow_nan=False)
    if args.output:
        args.output.write_text(text + "\n")
    print_comparison(report)


def micro(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    cp = classpath()
    write_json(output / "environment.json", environment())
    invocation = [
        "java", "-Xmx1g", *COMPATIBILITY_FLAGS, "-cp", cp, "org.openjdk.jmh.Main",
        "-rf", "json", "-rff", str(output / "jmh.json"),
        *args.jmh_args,
    ]
    write_json(output / "command.json", invocation)
    with (output / "console.log").open("w") as stream:
        subprocess.run(invocation, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, check=True)
    if "-l" not in args.jmh_args:
        results = json.loads((output / "jmh.json").read_text())
        log = (output / "console.log").read_text()
        if not results or "<failure>" in log or "<failed" in log.lower():
            raise RuntimeError(f"JMH had failed/missing measurements; see {output}/console.log")
    print(f"JMH artifacts: {output}")


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("run", help="Run repeated fixed-load scenarios in fresh JVMs")
    runner.add_argument("--output", type=Path, required=True)
    runner.add_argument("--topology", choices=["local", "process", "http", "distributed"], default="process")
    runner.add_argument("--mode", choices=["sync", "async", "send-only"], default="sync")
    runner.add_argument("--collector", choices=["g1", "parallel", "zgc"], default="g1")
    for flag, default in [
        ("rate", 10000), ("topics", 1), ("producers", 1), ("payload-bytes", 1024),
        ("workers", 2), ("queue-capacity", 4096), ("threads", 1), ("warmup-seconds", 2),
        ("measure-seconds", 3), ("warmup-minutes", 1), ("measure-minutes", 1), ("repetitions", 3),
    ]:
        runner.add_argument("--" + flag, type=positive, default=default)
    runner.add_argument("--processors", type=positive)
    runner.add_argument("--scheduler-parallelism", type=positive)
    runner.add_argument("--heap", default="512m")
    runner.add_argument("--initial-heap", default="128m")
    runner.add_argument("--supervisor-heap", default="512m")
    runner.add_argument("--supervisor-initial-heap", default="128m")
    runner.add_argument("--driver-jvm-opts", default="")
    runner.add_argument("--coordinator-jvm-opts", default="")
    runner.add_argument("--key", choices=["NO_KEY", "RANDOM_NANO", "KEY_ROUND_ROBIN"], default="NO_KEY")
    runner.add_argument("--weighted", action="store_true")
    runner.add_argument("--smoke", action="store_true")
    runner.add_argument("--profile", action="store_true")
    runner.add_argument("--min-rate-ratio", type=float, default=0.98)
    runner.set_defaults(handler=run)
    comparison = commands.add_parser("compare", help="Compare equivalent full runs on the same host")
    comparison.add_argument("baseline", type=Path)
    comparison.add_argument("candidate", type=Path)
    comparison.add_argument("--output", type=Path)
    comparison.add_argument("--micro", action="store_true", help="Compare equivalent JMH experiments")
    comparison.set_defaults(handler=compare)
    jmh = commands.add_parser("micro", help="Pass arguments after -- to JMH (e.g. -prof gc)")
    jmh.add_argument("--output", type=Path, required=True)
    jmh.add_argument("jmh_args", nargs=argparse.REMAINDER)
    jmh.set_defaults(handler=micro)
    return result


def main():
    args = parser().parse_args()
    if args.command == "run":
        if not 0 < args.min_rate_ratio <= 1:
            raise ValueError("min-rate-ratio must be in (0,1]")
        if args.workers < 2:
            raise ValueError("workers must be at least two")
        if not args.smoke and args.mode == "send-only":
            raise ValueError("send-only uses the short Worker path; add --smoke")
        for value in (args.heap, args.initial_heap, args.supervisor_heap, args.supervisor_initial_heap):
            if not re.fullmatch(r"[1-9]\d*[kKmMgG]", value):
                raise ValueError("Heap values must be positive sizes such as 512m or 4g")
    if args.command == "micro" and args.jmh_args[:1] == ["--"]:
        args.jmh_args = args.jmh_args[1:]
    args.handler(args)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, TimeoutError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
