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
"""Validates the regression run artifacts and renders the reports.

Reads <report>/run.properties, <report>/stacks/*/stack.properties and
<report>/cases/*/{case.properties,result.json,...} written by run-e2e.sh and produces
results.json, junit.xml, summary.md and a self-contained index.html.

Exit code: 0 when every selected case passed, 1 otherwise, 2 on internal errors.
Only the Python standard library is used.
"""

import argparse
import datetime
import html
import json
import os
import re
import sys
import xml.etree.ElementTree as ET

SCHEMA_VERSION = 1
# WorkloadGenerator samples rates every 10 seconds.
STATS_INTERVAL_SECONDS = 10.0
PASS = "passed"
FAILED = "failed"
ERROR = "error"
BLOCKED = "blocked"
ABORTED = "aborted"
NOT_RUN = "not-run"
STATUS_ORDER = [FAILED, ERROR, BLOCKED, ABORTED, NOT_RUN, PASS]
STATUS_LABEL = {
    PASS: "Passed",
    FAILED: "Failed",
    ERROR: "Infrastructure error",
    BLOCKED: "Blocked (could not run)",
    ABORTED: "Aborted",
    NOT_RUN: "Not run",
}
ERROR_LINE = re.compile(r"(\bERROR\b|Exception|\bFATAL\b|Caused by:)")
SECRET_LINE = re.compile(r"(?i)(password|secret|token|accessKey|secretKey)(\s*[:=]\s*)(\S+)")


def read_properties(path):
    """Reads key=value lines; later keys override earlier ones, repeated keys are also collected."""
    props, multi = {}, {}
    if not os.path.isfile(path):
        return props, multi
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            props[key] = value
            multi.setdefault(key, []).append(value)
    return props, multi


def read_matrix(path):
    rows = []
    if not os.path.isfile(path):
        return rows
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if "|" in line:
                rows.append([c.strip() for c in line.split("|")])
    return rows


def read_simple_yaml(path):
    """Top-level scalar keys of a flat YAML file (workload definitions are flat)."""
    values = {}
    if not os.path.isfile(path):
        return values
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = re.match(r"^([A-Za-z0-9_]+):\s*(.*?)\s*$", line)
            if m and not line.startswith(" "):
                raw = m.group(2).strip('"\'')
                try:
                    values[m.group(1)] = float(raw) if "." in raw else int(raw)
                except ValueError:
                    values[m.group(1)] = raw
    return values


def tail(path, lines=60):
    if not os.path.isfile(path):
        return ""
    with open(path, encoding="utf-8", errors="replace") as f:
        content = f.readlines()
    return redact("".join(content[-lines:]))


def error_lines(path, limit=25):
    if not os.path.isfile(path):
        return []
    found = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if ERROR_LINE.search(line):
                found.append(redact(line.rstrip()))
                if len(found) >= limit:
                    break
    return found


def redact(text):
    return SECRET_LINE.sub(lambda m: m.group(1) + m.group(2) + "***", text)


def parse_metrics(path):
    """Sums Prometheus counters whose names indicate sent/received messages and errors."""
    totals = {}
    if not os.path.isfile(path):
        return totals
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN)", line)
            if not m or not m.group(1).endswith("_total"):
                continue
            try:
                totals[m.group(1)] = totals.get(m.group(1), 0.0) + float(m.group(3))
            except ValueError:
                pass
    return totals


def check(name, ok, detail):
    return {"name": name, "passed": bool(ok), "detail": detail}


def validate_result(case, cdir):
    """Applies the common messaging contract to the benchmark result of one case."""
    workload = read_simple_yaml(os.path.join(cdir, "workload.yaml"))
    result_path = os.path.join(cdir, "result.json")
    coordinator_log = os.path.join(cdir, "coordinator.log")
    checks, measurements = [], {}

    exit_code = case.get("coordinator_exit_code", "")
    timed_out = case.get("timed_out") == "true"
    if timed_out:
        checks.append(check("Coordinator finished in time", False,
                            "Coordinator was killed after the case timeout"))
    elif exit_code not in ("", "0"):
        checks.append(check("Coordinator exit code", False, "exit code %s" % exit_code))
    else:
        checks.append(check("Coordinator exit code", True, "exit code 0"))

    failures = [l for l in error_lines(coordinator_log, 200) if "Failed to run the workload" in l]
    checks.append(check("Workload completed", not failures,
                        failures[0] if failures else "no 'Failed to run the workload' entry"))

    if not os.path.isfile(result_path):
        checks.append(check("Result file written", False, "result.json is missing"))
        return checks, measurements
    try:
        with open(result_path, encoding="utf-8") as f:
            result = json.load(f)
    except (OSError, ValueError) as e:
        checks.append(check("Result file written", False, "result.json is not valid JSON: %s" % e))
        return checks, measurements
    checks.append(check("Result file written", True, "result.json parsed"))

    publish = [float(x) for x in result.get("publishRate") or []]
    consume = [float(x) for x in result.get("consumeRate") or []]
    errors = [float(x) for x in result.get("publishErrorRate") or []]
    backlog = [int(x) for x in result.get("backlog") or []]
    subscriptions = int(workload.get("subscriptionsPerTopic", 1) or 1)
    target_rate = float(workload.get("producerRate", 0) or 0)
    duration = float(workload.get("testDurationMinutes", 1) or 1) * 60.0

    published = sum(publish) * STATS_INTERVAL_SECONDS
    consumed = sum(consume) * STATS_INTERVAL_SECONDS
    expected_deliveries = published * subscriptions
    measurements.update({
        "samples": len(publish),
        "published_estimate": round(published),
        "consumed_estimate": round(consumed),
        "expected_deliveries": round(expected_deliveries),
        "subscriptions_per_topic": subscriptions,
        "target_publish_rate": target_rate,
        "avg_publish_rate": round(published / duration, 1) if duration else None,
        "avg_consume_rate": round(consumed / duration, 1) if duration else None,
        "final_backlog": backlog[-1] if backlog else None,
        "publish_errors_estimate": round(sum(errors) * STATS_INTERVAL_SECONDS),
        "publish_latency_avg_ms": result.get("aggregatedPublishLatencyAvg"),
        "publish_latency_p99_ms": result.get("aggregatedPublishLatency99pct"),
        "e2e_latency_avg_ms": result.get("aggregatedEndToEndLatencyAvg"),
        "e2e_latency_p99_ms": result.get("aggregatedEndToEndLatency99pct"),
        "publish_rate_series": [round(x, 1) for x in publish],
        "consume_rate_series": [round(x, 1) for x in consume],
        "backlog_series": backlog,
    })

    checks.append(check("Statistics collected", len(publish) >= 3,
                        "%d rate samples" % len(publish)))
    minimum = 0.5 * target_rate * duration if target_rate else 1
    checks.append(check("Messages published", published >= minimum,
                        "~%d published, at least %d expected (50%% of %g msg/s for %ds)"
                        % (published, minimum, target_rate, duration)))
    checks.append(check("No publish errors", sum(errors) == 0,
                        "~%d publish errors" % round(sum(errors) * STATS_INTERVAL_SECONDS)))
    ratio = consumed / expected_deliveries if expected_deliveries else 0.0
    measurements["delivery_ratio"] = round(ratio, 4)
    checks.append(check("Every subscription received the messages", 0.9 <= ratio <= 1.1,
                        "consumed/expected = %.3f (~%d of ~%d; %d subscription(s) per topic)"
                        % (ratio, consumed, expected_deliveries, subscriptions)))
    tolerance = max(100.0, target_rate * subscriptions * 2.0)
    if backlog:
        final = backlog[-1]
        checks.append(check("Backlog drained", -tolerance <= final <= tolerance,
                            "final backlog %d (sent x subscriptions - received), tolerance +/-%d"
                            % (final, tolerance)))
    else:
        checks.append(check("Backlog drained", False, "no backlog samples"))
    e2e = result.get("aggregatedEndToEndLatencyQuantiles") or {}
    checks.append(check("End-to-end latency recorded",
                        bool(e2e) or (result.get("aggregatedEndToEndLatencyAvg") or 0) > 0,
                        "average %.2f ms" % float(result.get("aggregatedEndToEndLatencyAvg") or 0)))

    metric_totals = {}
    for worker in ("worker-1", "worker-2"):
        for key, value in parse_metrics(os.path.join(cdir, worker + "-metrics.txt")).items():
            metric_totals[key] = metric_totals.get(key, 0.0) + value
    interesting = {k: v for k, v in metric_totals.items()
                   if re.search(r"(sent|received|error|bytes)", k)}
    measurements["worker_metric_totals"] = dict(sorted(interesting.items()))
    return checks, measurements


def collect_artifacts(cdir, report_dir):
    names = []
    for name in sorted(os.listdir(cdir)):
        if os.path.isfile(os.path.join(cdir, name)) and name != "case.properties":
            names.append(os.path.relpath(os.path.join(cdir, name), report_dir))
    return names


def evaluate_case(case_id, report_dir, run, stacks):
    cdir = os.path.join(report_dir, "cases", case_id)
    props, _ = read_properties(os.path.join(cdir, "case.properties"))
    case = {
        "id": case_id,
        "plugin": props.get("plugin", "?"),
        "stack": props.get("stack", "?"),
        "kind": props.get("kind", "?"),
        "description": props.get("description", ""),
        "driver_config": props.get("driver_config", ""),
        "workload": props.get("workload", ""),
        "architecture": props.get("architecture", ""),
        "started_at": props.get("started_at"),
        "duration_seconds": int(props["duration_seconds"]) if props.get("duration_seconds", "").isdigit() else None,
        "checks": [],
        "measurements": {},
        "artifacts": collect_artifacts(cdir, report_dir) if os.path.isdir(cdir) else [],
        "rerun": "e2e-tests/compose/run-e2e.sh --build skip --cases %s" % case_id,
    }
    preset = props.get("status")
    if preset in (BLOCKED, ERROR, ABORTED, NOT_RUN):
        case["status"] = preset
        case["phase"] = props.get("phase", "")
        case["reason"] = props.get("reason", "")
    elif not props.get("started_at"):
        case["status"] = NOT_RUN
        case["phase"] = "not-started"
        case["reason"] = ("The run was interrupted before this case started"
                          if run.get("interrupted") == "true" else "The case never started")
    else:
        checks, measurements = validate_result(props, cdir)
        case["checks"] = checks
        case["measurements"] = measurements
        failed = [c for c in checks if not c["passed"]]
        case["status"] = PASS if not failed else FAILED
        case["phase"] = "" if not failed else "verification"
        case["reason"] = "" if not failed else "; ".join("%s: %s" % (c["name"], c["detail"]) for c in failed[:3])

    if case["status"] != PASS:
        case["log_excerpt"] = tail(os.path.join(cdir, "coordinator.log"), 60)
        worker_errors = []
        for name in ("worker-1.log", "worker-1-driver.log", "worker-2.log", "worker-2-driver.log"):
            worker_errors += ["[%s] %s" % (name, l) for l in error_lines(os.path.join(cdir, name), 10)]
        case["worker_errors"] = worker_errors[:40]
        stack_dir = os.path.join(report_dir, "stacks", case["stack"])
        if case.get("phase") == "stack-startup":
            case["log_excerpt"] = tail(os.path.join(stack_dir, "startup.log"), 40) + "\n" + \
                "\n".join(error_lines(os.path.join(stack_dir, "compose.log"), 40))
    return case


def coverage_cases(report_dir, suite_dir, run, cases):
    """Full runs must cover every shipped plugin with at least one contract case."""
    if run.get("stack_filter") or run.get("case_filter"):
        return []
    shipped = []
    path = os.path.join(report_dir, "shipped-plugins.txt")
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                if "|" in line:
                    shipped.append(line.strip().split("|", 1)[1])
    matrix = read_matrix(os.path.join(suite_dir, "cases.txt"))
    contract = {row[1] for row in matrix if len(row) > 3 and row[3] == "contract"}
    extra = []
    for plugin in sorted(set(shipped)):
        if plugin not in contract:
            extra.append({
                "id": "coverage-" + plugin, "plugin": plugin, "stack": "-", "kind": "coverage",
                "description": "Shipped plugin without a contract case", "status": FAILED,
                "phase": "coverage", "reason": "No contract case in cases.txt covers plugin '%s'" % plugin,
                "checks": [], "measurements": {}, "artifacts": [], "architecture": "",
                "rerun": "", "duration_seconds": None,
            })
    if not shipped:
        extra.append({
            "id": "coverage-inventory", "plugin": "-", "stack": "-", "kind": "coverage",
            "description": "Shipped plugin inventory", "status": ERROR, "phase": "coverage",
            "reason": "shipped-plugins.txt is missing; plugin coverage could not be verified",
            "checks": [], "measurements": {}, "artifacts": [], "architecture": "",
            "rerun": "", "duration_seconds": None,
        })
    return extra


def load(report_dir, suite_dir):
    run, _ = read_properties(os.path.join(report_dir, "run.properties"))
    stacks = []
    stacks_root = os.path.join(report_dir, "stacks")
    order = [row[0] for row in read_matrix(os.path.join(suite_dir, "stacks.txt"))]
    if os.path.isdir(stacks_root):
        for name in sorted(os.listdir(stacks_root), key=lambda s: order.index(s) if s in order else 999):
            props, multi = read_properties(os.path.join(stacks_root, name, "stack.properties"))
            stacks.append({
                "stack": name,
                "description": props.get("description", ""),
                "architecture": props.get("architecture", ""),
                "status": props.get("status", ""),
                "reason": props.get("reason", ""),
                "startup_seconds": props.get("startup_seconds"),
                "cleanup": props.get("cleanup", ""),
                "images": multi.get("image", []),
                "artifacts": multi.get("artifact", []),
            })
    case_order = [row[0] for row in read_matrix(os.path.join(suite_dir, "cases.txt"))]
    cases_root = os.path.join(report_dir, "cases")
    ids = sorted(os.listdir(cases_root), key=lambda c: case_order.index(c) if c in case_order else 999) \
        if os.path.isdir(cases_root) else []
    cases = [evaluate_case(c, report_dir, run, stacks) for c in ids]
    cases += coverage_cases(report_dir, suite_dir, run, cases)
    counts = {s: 0 for s in STATUS_ORDER}
    for c in cases:
        counts[c["status"]] = counts.get(c["status"], 0) + 1
    cleanup_failures = [s["stack"] for s in stacks if s["cleanup"] == "failed"]
    success = bool(cases) and counts[PASS] == len(cases) and not run.get("preflight_error") \
        and run.get("interrupted") != "true" and not cleanup_failures
    plugins = {}
    for c in cases:
        plugins.setdefault(c["plugin"], []).append(c)
    return {
        "schemaVersion": SCHEMA_VERSION,
        "run": run,
        "subset": bool(run.get("stack_filter") or run.get("case_filter")),
        "success": success,
        "counts": counts,
        "total": len(cases),
        "cleanup_failures": cleanup_failures,
        "stacks": stacks,
        "cases": cases,
        "plugins": {p: worst([c["status"] for c in cs]) for p, cs in sorted(plugins.items())},
        "generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def worst(statuses):
    for s in STATUS_ORDER:
        if s in statuses:
            return s
    return NOT_RUN


def write_json(data, report_dir):
    with open(os.path.join(report_dir, "results.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=False)
        f.write("\n")


def write_junit(data, report_dir):
    root = ET.Element("testsuites", name="omb-plugin-regression", tests=str(data["total"]))
    by_stack = {}
    for c in data["cases"]:
        by_stack.setdefault(c["stack"], []).append(c)
    total_fail = total_err = 0
    for stack, cases in by_stack.items():
        failures = sum(1 for c in cases if c["status"] == FAILED)
        errors = sum(1 for c in cases if c["status"] not in (PASS, FAILED))
        total_fail += failures
        total_err += errors
        suite = ET.SubElement(root, "testsuite", name="stack." + stack, tests=str(len(cases)),
                              failures=str(failures), errors=str(errors), skipped="0",
                              time=str(sum(c.get("duration_seconds") or 0 for c in cases)))
        for c in cases:
            tc = ET.SubElement(suite, "testcase", classname="plugin." + c["plugin"], name=c["id"],
                               time=str(c.get("duration_seconds") or 0))
            if c["status"] == FAILED:
                el = ET.SubElement(tc, "failure", message=c.get("reason", "")[:500], type=c.get("phase", ""))
                el.text = "\n".join("[%s] %s: %s" % ("PASS" if k["passed"] else "FAIL", k["name"], k["detail"])
                                    for k in c["checks"]) or c.get("reason", "")
            elif c["status"] != PASS:
                el = ET.SubElement(tc, "error", message=c.get("reason", "")[:500], type=c["status"])
                el.text = c.get("reason", "")
            if c.get("log_excerpt"):
                ET.SubElement(tc, "system-out").text = c["log_excerpt"]
    root.set("failures", str(total_fail))
    root.set("errors", str(total_err))
    ET.ElementTree(root).write(os.path.join(report_dir, "junit.xml"), encoding="utf-8", xml_declaration=True)


def md_escape(text):
    return str(text).replace("|", "\\|").replace("\n", " ")


def write_markdown(data, report_dir):
    run = data["run"]
    out = []
    verdict = "PASSED" if data["success"] else "FAILED"
    out.append("# Plugin regression report: %s%s" % (verdict, " (subset run)" if data["subset"] else ""))
    out.append("")
    out.append("- Run: `%s` (%s to %s)" % (run.get("run_id", "?"), run.get("started_at", "?"), run.get("finished_at", "?")))
    out.append("- Revision: `%s` on `%s`, %s uncommitted file(s)" % (
        run.get("git_revision", "?")[:12], run.get("git_branch", "?"), run.get("git_dirty_files", "?")))
    out.append("- Host: %s/%s, Docker %s, Compose %s" % (run.get("host_os", "?"), run.get("host_arch", "?"),
                                                       run.get("docker_version", "?"), run.get("compose_version", "?")))
    out.append("- Runner: %s, %s" % (run.get("runner_java", "?"), run.get("distribution", "?")))
    counts = ", ".join("%d %s" % (data["counts"][s], STATUS_LABEL[s].lower()) for s in STATUS_ORDER if data["counts"].get(s))
    out.append("- Cases: %d total: %s" % (data["total"], counts or "none"))
    if data["subset"]:
        out.append("- **Subset run** (stacks `%s`, cases `%s`): this is not full plugin coverage." % (
            run.get("stack_filter") or "all", run.get("case_filter") or "all"))
    if run.get("preflight_error"):
        out.append("- **Preflight error:** %s" % run["preflight_error"])
    if run.get("interrupted") == "true":
        out.append("- **The run was interrupted.**")
    if data["cleanup_failures"]:
        out.append("- **Cleanup failed** for stacks: %s" % ", ".join(data["cleanup_failures"]))
    out.append("")
    out.append("## Plugins")
    out.append("")
    out.append("| Plugin | Result | Cases |")
    out.append("| --- | --- | --- |")
    for plugin, status in data["plugins"].items():
        cs = [c for c in data["cases"] if c["plugin"] == plugin]
        out.append("| %s | %s | %s |" % (plugin, STATUS_LABEL[status],
                                         ", ".join("%s (%s)" % (c["id"], c["status"]) for c in cs)))
    out.append("")
    out.append("## Cases")
    out.append("")
    out.append("| Case | Plugin | Stack | Kind | Status | Duration | Published / consumed | Reason |")
    out.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for c in data["cases"]:
        m = c.get("measurements") or {}
        flow = "%s / %s" % (m.get("published_estimate", "-"), m.get("consumed_estimate", "-")) if m else "-"
        out.append("| %s | %s | %s%s | %s | %s | %s | %s | %s |" % (
            c["id"], c["plugin"], c["stack"], " (emulated)" if c.get("architecture") == "emulated-amd64" else "",
            c["kind"], STATUS_LABEL[c["status"]],
            "%ss" % c["duration_seconds"] if c.get("duration_seconds") is not None else "-",
            flow, md_escape(c.get("reason", ""))))
    problems = [c for c in data["cases"] if c["status"] != PASS]
    if problems:
        out.append("")
        out.append("## Failures")
        for c in problems:
            out.append("")
            out.append("### %s: %s" % (c["id"], STATUS_LABEL[c["status"]]))
            out.append("")
            out.append("- Phase: %s" % (c.get("phase") or "-"))
            out.append("- Reason: %s" % md_escape(c.get("reason") or "-"))
            for k in c["checks"]:
                if not k["passed"]:
                    out.append("- Failed check: %s: %s" % (k["name"], md_escape(k["detail"])))
            if c.get("rerun"):
                out.append("- Rerun: `%s`" % c["rerun"])
            if c.get("artifacts"):
                out.append("- Artifacts: %s" % ", ".join("[%s](%s)" % (os.path.basename(a), a) for a in c["artifacts"]))
    out.append("")
    out.append("## Stacks")
    out.append("")
    out.append("| Stack | Architecture | Startup | Cleanup | Images |")
    out.append("| --- | --- | --- | --- | --- |")
    for s in data["stacks"]:
        out.append("| %s | %s | %s | %s | %s |" % (
            s["stack"], s["architecture"], ("%ss" % s["startup_seconds"]) if s["startup_seconds"] else (s["status"] or "-"),
            s["cleanup"] or "-", "<br>".join(md_escape(i) for i in s["images"]) or "-"))
    out.append("")
    out.append("Full run: `e2e-tests/compose/run-e2e.sh`. Detailed report: `index.html`.")
    with open(os.path.join(report_dir, "summary.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")


CSS = """
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;margin:0;color:#1f2328;background:#f6f8fa}
header{padding:24px 32px;color:#fff}header.ok{background:#1a7f37}header.bad{background:#cf222e}
header h1{margin:0 0 8px 0;font-size:24px}header p{margin:2px 0;opacity:.95}
main{padding:24px 32px;max-width:1400px}
section{background:#fff;border:1px solid #d0d7de;border-radius:8px;padding:16px 20px;margin-bottom:20px}
h2{font-size:18px;margin-top:0}
table{border-collapse:collapse;width:100%;font-size:13px}th,td{border-bottom:1px solid #d8dee4;padding:6px 8px;text-align:left;vertical-align:top}
th{background:#f6f8fa}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:12px;font-weight:600;color:#fff;white-space:nowrap}
.passed{background:#1a7f37}.failed{background:#cf222e}.error{background:#bc4c00}.blocked{background:#8250df}.aborted{background:#6e7781}.not-run{background:#6e7781}
.cards{display:flex;gap:12px;flex-wrap:wrap}.card{border:1px solid #d0d7de;border-radius:8px;padding:10px 16px;min-width:110px;background:#fff}
.card b{display:block;font-size:22px}
details{border:1px solid #d0d7de;border-radius:6px;margin:8px 0;background:#fff}summary{cursor:pointer;padding:8px 12px;font-weight:600}
details>div{padding:0 12px 12px 12px}
pre{background:#0d1117;color:#e6edf3;padding:10px;border-radius:6px;overflow:auto;max-height:420px;font-size:12px;white-space:pre-wrap}
.check-ok{color:#1a7f37}.check-bad{color:#cf222e;font-weight:600}
.muted{color:#57606a}.warn{background:#fff8c5;border:1px solid #d4a72c;padding:8px 12px;border-radius:6px;margin:8px 0}
svg{background:#fafbfc;border:1px solid #d8dee4;border-radius:4px}
"""


def badge(status):
    return '<span class="badge %s">%s</span>' % (status, html.escape(STATUS_LABEL.get(status, status)))


def sparkline(series_list, width=420, height=90):
    """Inline SVG line chart; series_list is [(label, color, values)]."""
    values = [v for _, _, s in series_list for v in s]
    if not values:
        return ""
    vmax = max(values) or 1.0
    n = max(len(s) for _, _, s in series_list)
    parts = ['<svg width="%d" height="%d" viewBox="0 0 %d %d" role="img">' % (width, height + 16, width, height + 16)]
    for i, (label, color, s) in enumerate(series_list):
        if not s:
            continue
        pts = []
        for j, v in enumerate(s):
            x = 6 + (width - 12) * (j / max(n - 1, 1))
            y = 4 + (height - 8) * (1 - v / vmax)
            pts.append("%.1f,%.1f" % (x, y))
        parts.append('<polyline fill="none" stroke="%s" stroke-width="2" points="%s"/>' % (color, " ".join(pts)))
        parts.append('<text x="%d" y="%d" font-size="11" fill="%s">%s</text>' % (8 + i * 140, height + 12, color, html.escape(label)))
    parts.append('<text x="%d" y="12" font-size="10" fill="#57606a" text-anchor="end">max %.0f</text>' % (width - 6, vmax))
    parts.append("</svg>")
    return "".join(parts)


def write_html(data, report_dir):
    run = data["run"]
    e = html.escape
    h = []
    h.append("<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>")
    h.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
    h.append("<title>OMB plugin regression %s</title><style>%s</style></head><body>" % (e(run.get("run_id", "")), CSS))
    h.append("<header class='%s'><h1>Plugin regression: %s%s</h1>" % (
        "ok" if data["success"] else "bad", "PASSED" if data["success"] else "FAILED",
        " (subset run)" if data["subset"] else ""))
    h.append("<p>Run %s &middot; %s &rarr; %s</p>" % (e(run.get("run_id", "?")), e(run.get("started_at", "?")), e(run.get("finished_at", "?"))))
    h.append("<p>Revision %s on %s &middot; %s uncommitted file(s) &middot; host %s/%s &middot; Docker %s &middot; Compose %s</p>" % (
        e(run.get("git_revision", "?")[:12]), e(run.get("git_branch", "?")), e(run.get("git_dirty_files", "?")),
        e(run.get("host_os", "?")), e(run.get("host_arch", "?")), e(run.get("docker_version", "?")), e(run.get("compose_version", "?"))))
    h.append("<p>Runner: %s &middot; %s</p></header><main>" % (e(run.get("runner_java", "?")), e(run.get("distribution", "?"))))

    warnings = []
    if data["subset"]:
        warnings.append("Subset run (stacks: %s, cases: %s). This report does not prove full plugin coverage." % (
            e(run.get("stack_filter") or "all"), e(run.get("case_filter") or "all")))
    if run.get("preflight_error"):
        warnings.append("Preflight error: %s" % e(run["preflight_error"]))
    if run.get("interrupted") == "true":
        warnings.append("The run was interrupted; remaining cases are reported as not run.")
    if data["cleanup_failures"]:
        warnings.append("Cleanup failed for stacks: %s" % e(", ".join(data["cleanup_failures"])))
    h.append("<section><h2>Summary</h2><div class='cards'>")
    h.append("<div class='card'><b>%d</b>cases</div>" % data["total"])
    for s in STATUS_ORDER:
        if data["counts"].get(s):
            h.append("<div class='card'><b>%d</b>%s</div>" % (data["counts"][s], badge(s)))
    h.append("</div>")
    for w in warnings:
        h.append("<div class='warn'>%s</div>" % w)
    h.append("<p class='muted'>Every case runs the benchmark coordinator against two benchmark workers, each "
             "starting the selected driver plugin in an isolated child JVM. The contract checks that messages "
             "are published without errors, every subscription receives them (consumed/expected within 0.9-1.1), "
             "the backlog drains and end-to-end latency is recorded. Counts are derived from the 10 second rate samples.</p>")
    h.append("</section>")

    h.append("<section><h2>Plugins</h2><table><tr><th>Plugin</th><th>Result</th><th>Cases</th></tr>")
    for plugin, status in data["plugins"].items():
        cs = [c for c in data["cases"] if c["plugin"] == plugin]
        h.append("<tr><td><b>%s</b></td><td>%s</td><td>%s</td></tr>" % (
            e(plugin), badge(status), " ".join("<a href='#case-%s'>%s</a> %s" % (e(c["id"]), e(c["id"]), badge(c["status"])) for c in cs)))
    h.append("</table></section>")

    h.append("<section><h2>Cases</h2><table><tr><th>Case</th><th>Plugin</th><th>Stack</th><th>Kind</th><th>Status</th>"
             "<th>Duration</th><th>Published</th><th>Consumed</th><th>Avg pub rate</th><th>E2E p99 (ms)</th><th>Reason</th></tr>")
    for c in data["cases"]:
        m = c.get("measurements") or {}
        h.append("<tr><td><a href='#case-%s'>%s</a></td><td>%s</td><td>%s%s</td><td>%s</td><td>%s</td><td>%s</td>"
                 "<td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                     e(c["id"]), e(c["id"]), e(c["plugin"]), e(c["stack"]),
                     " <span class='muted'>(emulated)</span>" if c.get("architecture") == "emulated-amd64" else "",
                     e(c["kind"]), badge(c["status"]),
                     "%ss" % c["duration_seconds"] if c.get("duration_seconds") is not None else "-",
                     m.get("published_estimate", "-"), m.get("consumed_estimate", "-"), m.get("avg_publish_rate", "-"),
                     "%.1f" % m["e2e_latency_p99_ms"] if isinstance(m.get("e2e_latency_p99_ms"), (int, float)) else "-",
                     e(c.get("reason", ""))))
    h.append("</table></section>")

    h.append("<section><h2>Case details</h2>")
    for c in data["cases"]:
        m = c.get("measurements") or {}
        h.append("<details id='case-%s'%s><summary>%s %s <span class='muted'>&middot; %s &middot; %s</span></summary><div>" % (
            e(c["id"]), " open" if c["status"] != PASS else "", badge(c["status"]), e(c["id"]), e(c["plugin"]), e(c.get("description", ""))))
        h.append("<p>Stack <b>%s</b> (%s) &middot; driver <code>%s</code> &middot; workload <code>%s</code></p>" % (
            e(c["stack"]), e(c.get("architecture", "")), e(c.get("driver_config", "")), e(c.get("workload", ""))))
        if c.get("reason"):
            h.append("<p><b>%s</b>: %s</p>" % (e(c.get("phase") or "reason"), e(c["reason"])))
        if c["checks"]:
            h.append("<table><tr><th>Check</th><th>Result</th><th>Detail</th></tr>")
            for k in c["checks"]:
                h.append("<tr><td>%s</td><td class='%s'>%s</td><td>%s</td></tr>" % (
                    e(k["name"]), "check-ok" if k["passed"] else "check-bad", "PASS" if k["passed"] else "FAIL", e(k["detail"])))
            h.append("</table>")
        if m.get("publish_rate_series"):
            h.append("<p>Rates (msg/s per 10s sample)</p>" + sparkline([
                ("publish", "#0969da", m["publish_rate_series"]), ("consume", "#1a7f37", m["consume_rate_series"])]))
            h.append("<p class='muted'>Publish latency avg %s ms, p99 %s ms &middot; end-to-end avg %s ms, p99 %s ms &middot; final backlog %s</p>" % (
                fmt(m.get("publish_latency_avg_ms")), fmt(m.get("publish_latency_p99_ms")),
                fmt(m.get("e2e_latency_avg_ms")), fmt(m.get("e2e_latency_p99_ms")), e(str(m.get("final_backlog")))))
        if m.get("worker_metric_totals"):
            h.append("<p class='muted'>Worker counters: %s</p>" % e(", ".join(
                "%s=%.0f" % (k, v) for k, v in m["worker_metric_totals"].items())))
        if c.get("worker_errors"):
            h.append("<p>Error lines from worker and driver logs</p><pre>%s</pre>" % e("\n".join(c["worker_errors"])))
        if c.get("log_excerpt"):
            h.append("<p>Log excerpt</p><pre>%s</pre>" % e(c["log_excerpt"]))
        if c.get("artifacts"):
            h.append("<p>Artifacts: %s</p>" % " &middot; ".join(
                "<a href='%s'>%s</a>" % (e(a), e(os.path.basename(a))) for a in c["artifacts"]))
        if c.get("rerun"):
            h.append("<p class='muted'>Rerun: <code>%s</code></p>" % e(c["rerun"]))
        h.append("</div></details>")
    h.append("</section>")

    h.append("<section><h2>Stacks</h2><table><tr><th>Stack</th><th>Description</th><th>Architecture</th><th>Startup</th>"
             "<th>Cleanup</th><th>Images (digest, platform)</th><th>Provider artifacts</th><th>Logs</th></tr>")
    for s in data["stacks"]:
        h.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
            e(s["stack"]), e(s["description"]), e(s["architecture"]),
            e("%ss" % s["startup_seconds"]) if s["startup_seconds"] else e(s["status"] or "-") + (": " + e(s["reason"]) if s["reason"] else ""),
            e(s["cleanup"] or "-"), "<br>".join(e(i) for i in s["images"]) or "-", "<br>".join(e(a) for a in s["artifacts"]) or "-",
            " ".join("<a href='stacks/%s/%s'>%s</a>" % (e(s["stack"]), f, f) for f in
                     ("startup.log", "compose.log", "compose-effective.yaml", "teardown.log")
                     if os.path.isfile(os.path.join(report_dir, "stacks", s["stack"], f)))))
    h.append("</table></section>")
    h.append("<p class='muted'>Generated %s &middot; machine-readable results: <a href='results.json'>results.json</a>, "
             "<a href='junit.xml'>junit.xml</a> &middot; <a href='summary.md'>summary.md</a> &middot; <a href='run.log'>run.log</a></p>" % e(data["generated_at"]))
    h.append("</main></body></html>")
    with open(os.path.join(report_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write("\n".join(h))


def fmt(value):
    return "%.2f" % value if isinstance(value, (int, float)) else "-"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, help="e2e-tests/compose directory")
    parser.add_argument("--report", required=True, help="run report directory")
    args = parser.parse_args()
    try:
        data = load(args.report, args.suite)
        write_json(data, args.report)
        write_junit(data, args.report)
        write_markdown(data, args.report)
        write_html(data, args.report)
    except Exception as e:  # noqa: BLE001 - any failure must be visible and non-zero
        print("Report generation failed: %r" % e, file=sys.stderr)
        raise SystemExit(2)
    print("Result: %s (%d cases: %s)" % (
        "PASSED" if data["success"] else "FAILED", data["total"],
        ", ".join("%d %s" % (v, k) for k, v in data["counts"].items() if v)))
    return 0 if data["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
