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

import json
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

import report


class ResultValidationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.case_dir = Path(self.tmp.name)
        (self.case_dir / "workload.yaml").write_text(
            "subscriptionsPerTopic: 1\nproducerRate: 200\ntestDurationMinutes: 1\n",
            encoding="utf-8",
        )
        (self.case_dir / "coordinator.log").write_text("Workload completed\n", encoding="utf-8")
        self.result = {
            "publishRate": [200] * 6,
            "consumeRate": [200] * 6,
            "publishErrorRate": [0] * 6,
            "backlog": [0] * 6,
            "aggregatedEndToEndLatencyAvg": 1,
            "aggregatedEndToEndLatencyQuantiles": {"100.0": 1},
        }

    def validate(self, props=None):
        (self.case_dir / "result.json").write_text(json.dumps(self.result), encoding="utf-8")
        return report.validate_result(
            {"coordinator_exit_code": "0"} if props is None else props, str(self.case_dir)
        )[0]

    def test_valid_result_passes(self):
        self.assertTrue(all(check["passed"] for check in self.validate()))

    def test_missing_exit_code_fails(self):
        checks = self.validate({})
        exit_check = next(check for check in checks if check["name"] == "Coordinator exit code")
        self.assertFalse(exit_check["passed"])

    def test_missing_coordinator_log_fails(self):
        (self.case_dir / "coordinator.log").unlink()
        checks = self.validate()
        workload_check = next(check for check in checks if check["name"] == "Workload completed")
        self.assertFalse(workload_check["passed"])

    def test_workload_failure_after_other_errors_is_detected(self):
        (self.case_dir / "coordinator.log").write_text(
            "ERROR unrelated message\n" * 200 + "ERROR Failed to run the workload\n",
            encoding="utf-8",
        )
        checks = self.validate()
        workload_check = next(check for check in checks if check["name"] == "Workload completed")
        self.assertFalse(workload_check["passed"])

    def test_missing_publish_error_samples_cannot_pass(self):
        del self.result["publishErrorRate"]
        checks = self.validate()
        self.assertFalse(all(check["passed"] for check in checks))

    def test_incomplete_publish_error_samples_cannot_pass(self):
        self.result["publishErrorRate"] = [0]
        checks = self.validate()
        self.assertFalse(all(check["passed"] for check in checks))

    def test_invalid_rate_fails_without_crashing_report(self):
        self.result["publishRate"][0] = "not-a-number"
        checks = self.validate()
        self.assertFalse(all(check["passed"] for check in checks))

    def test_nonfinite_rate_fails_without_crashing_report(self):
        self.result["publishRate"][0] = float("nan")
        checks = self.validate()
        self.assertFalse(all(check["passed"] for check in checks))

    def test_numeric_overflow_fails_without_crashing_report(self):
        self.result["publishRate"] = [1e308] * 6
        checks = self.validate()
        self.assertFalse(all(check["passed"] for check in checks))

    def test_invalid_latency_fails_without_crashing_report(self):
        self.result["aggregatedEndToEndLatency99pct"] = float("inf")
        checks = self.validate()
        self.assertFalse(all(check["passed"] for check in checks))

    def test_fractional_backlog_fails(self):
        self.result["backlog"][0] = 0.5
        self.assertFalse(all(check["passed"] for check in self.validate()))

    def test_unreadable_workload_fails_without_crashing_report(self):
        (self.case_dir / "workload.yaml").write_bytes(b"\xff")
        self.assertFalse(all(check["passed"] for check in self.validate()))

    def test_invalid_worker_counters_fail_without_invalid_json(self):
        for value in ("NaN", "+Inf", "-Inf", "1e309", "not-a-number"):
            with self.subTest(value=value):
                (self.case_dir / "worker-1-metrics.txt").write_text(
                    "messages_sent_total %s\n" % value, encoding="utf-8"
                )
                (self.case_dir / "result.json").write_text(json.dumps(self.result), encoding="utf-8")
                checks, measurements = report.validate_result(
                    {"coordinator_exit_code": "0"}, str(self.case_dir)
                )
                self.assertTrue(any(check["name"] == "Worker metrics" and not check["passed"]
                                    for check in checks))
                json.dumps(measurements, allow_nan=False)

    def test_overflowing_combined_worker_counters_fail_without_invalid_json(self):
        for worker in ("worker-1", "worker-2"):
            (self.case_dir / (worker + "-metrics.txt")).write_text(
                "messages_sent_total 1e308\n", encoding="utf-8"
            )
        (self.case_dir / "result.json").write_text(json.dumps(self.result), encoding="utf-8")
        checks, measurements = report.validate_result(
            {"coordinator_exit_code": "0"}, str(self.case_dir)
        )
        self.assertTrue(any(check["name"] == "Worker metrics" and not check["passed"]
                            for check in checks))
        json.dumps(measurements, allow_nan=False)

    def test_overflowing_counter_labels_fail_without_invalid_json(self):
        (self.case_dir / "worker-1-metrics.txt").write_text(
            'messages_sent_total{label="one"} 1e308\n'
            'messages_sent_total{label="two"} 1e308\n',
            encoding="utf-8",
        )
        self.assertTrue(any(check["name"] == "Worker metrics" and not check["passed"]
                            for check in self.validate()))

    def test_worker_counter_labels_are_summed(self):
        (self.case_dir / "worker-1-metrics.txt").write_text(
            'messages_sent_total{label="one"} 3\n'
            'messages_sent_total{label="two"} 4\n',
            encoding="utf-8",
        )
        (self.case_dir / "worker-2-metrics.txt").write_text(
            "messages_sent_total 5\n", encoding="utf-8"
        )
        (self.case_dir / "result.json").write_text(json.dumps(self.result), encoding="utf-8")
        checks, measurements = report.validate_result(
            {"coordinator_exit_code": "0"}, str(self.case_dir)
        )
        self.assertTrue(all(check["passed"] for check in checks))
        self.assertEqual(measurements["worker_metric_totals"]["messages_sent_total"], 12)


class ReportIntegrityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.suite = self.root / "suite"
        self.suite.mkdir()
        (self.suite / "cases.txt").write_text(
            "kafka-contract|kafka|missing-stack|contract|driver.yaml|workload.yaml|contract\n",
            encoding="utf-8",
        )
        (self.root / "shipped-plugins.txt").write_text("drivers/driver-kafka|kafka\n", encoding="utf-8")

    def test_contract_must_be_registered_in_the_run(self):
        cases = report.coverage_cases(str(self.root), str(self.suite), {}, [])
        self.assertTrue(any(case["plugin"] == "kafka" and case["status"] != report.PASS for case in cases))

    def test_cleanup_failure_is_visible_to_junit_consumers(self):
        data = {
            "total": 1,
            "cases": [{
                "id": "redis-contract", "plugin": "redis", "stack": "redis",
                "status": report.PASS, "duration_seconds": 1, "checks": [],
            }],
            "cleanup_failures": ["redis"],
            "run": {},
        }
        report.write_junit(data, str(self.root))
        xml = ET.parse(self.root / "junit.xml").getroot()
        self.assertGreater(int(xml.get("errors")), 0)
        self.assertIsNotNone(xml.find(".//error"))

    def test_interruption_is_visible_to_junit_consumers(self):
        data = {"total": 0, "cases": [], "cleanup_failures": [], "run": {"interrupted": "true"}}
        report.write_junit(data, str(self.root))
        xml = ET.parse(self.root / "junit.xml").getroot()
        self.assertEqual(int(xml.get("errors")), 2)
        self.assertIsNotNone(xml.find(".//testcase[@name='interrupted']/error"))

    def test_invalid_worker_metrics_render_all_report_formats(self):
        (self.root / "run.properties").write_text("case_filter=redis-contract\n", encoding="utf-8")
        case_dir = self.root / "cases" / "redis-contract"
        case_dir.mkdir(parents=True)
        (case_dir / "case.properties").write_text(
            "plugin=redis\nstack=redis\nkind=contract\nstarted_at=now\ncoordinator_exit_code=0\n",
            encoding="utf-8",
        )
        (case_dir / "workload.yaml").write_text(
            "subscriptionsPerTopic: 1\nproducerRate: 200\ntestDurationMinutes: 1\n",
            encoding="utf-8",
        )
        (case_dir / "coordinator.log").write_text("Workload completed\n", encoding="utf-8")
        (case_dir / "worker-1-metrics.txt").write_text("messages_sent_total NaN\n", encoding="utf-8")
        (case_dir / "result.json").write_text(json.dumps({
            "publishRate": [200] * 6,
            "consumeRate": [200] * 6,
            "publishErrorRate": [0] * 6,
            "backlog": [0] * 6,
            "aggregatedEndToEndLatencyAvg": 1,
        }), encoding="utf-8")

        data = report.load(str(self.root), str(self.suite))
        self.assertEqual(data["cases"][0]["status"], report.FAILED)
        for writer in (report.write_json, report.write_junit, report.write_markdown, report.write_html):
            writer(data, str(self.root))
        for name in ("results.json", "junit.xml", "summary.md", "index.html"):
            self.assertGreater((self.root / name).stat().st_size, 0)
        parsed = json.loads((self.root / "results.json").read_text(encoding="utf-8"),
                            parse_constant=lambda value: self.fail("invalid JSON value " + value))
        self.assertFalse(parsed["success"])
        self.assertIsNotNone(ET.parse(self.root / "junit.xml").find(".//failure"))


if __name__ == "__main__":
    unittest.main()
