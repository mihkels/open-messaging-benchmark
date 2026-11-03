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

import argparse
import contextlib
import copy
import importlib.util
import io
import os
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("harness", Path(__file__).parents[1] / "run.py")
harness = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(harness)


class HarnessTest(unittest.TestCase):
    def result(self):
        return {
            "schemaVersion": 1,
            "status": "complete",
            "scenario": {
                "topology": "local", "mode": "sync", "rate": 1000,
                "topics": 1, "producers": 1, "smoke": False,
            },
            "completed": 1000, "received": 1000, "errors": 0, "achievedRate": 1000,
            "publishHistogramSamples": 1000, "delayHistogramSamples": 1000,
            "pollSeconds": 0.1, "treeRssPeakBytes": 123,
            "resourceStart": {"driver": {"pid": 42, "cpu_seconds": 2, "gc_count": 0, "gc_collection_seconds": 0}},
            "resourceEnd": {"driver": {"pid": 42, "cpu_seconds": 3, "gc_count": 1, "gc_collection_seconds": 0.01}},
        }

    def test_cpu_normalizes_by_completed_work_and_gc_uses_seconds(self):
        summary = harness.summarize(self.result())
        self.assertTrue(summary["valid"])
        self.assertEqual(summary["treeCpuNsPerMessage"], 1_000_000)
        self.assertEqual(summary["perRole"]["driver"]["gc_collection_seconds"], 0.01)

    def test_missing_cpu_is_unavailable_not_zero(self):
        result = self.result()
        result["resourceEnd"]["driver"]["cpu_seconds"] = None
        self.assertIsNone(harness.summarize(result)["treeCpuNsPerMessage"])

    def test_missed_load_is_not_an_efficiency_win(self):
        result = self.result()
        result["achievedRate"] = 900
        self.assertIn("Missed requested fixed load", harness.summarize(result)["invalidReasons"])

    def test_histogram_loss_and_send_failure_are_visible(self):
        result = self.result()
        result["publishHistogramSamples"] = 500
        result["errors"] = 1
        self.assertFalse(harness.summarize(result)["valid"])
        self.assertEqual(len(harness.summarize(result)["invalidReasons"]), 2)

    def test_delivery_backlog_is_not_a_win(self):
        result = self.result()
        result["received"] = 500
        self.assertFalse(harness.summarize(result)["valid"])

    def test_decreased_counters_and_restarted_processes_are_invalid(self):
        result = self.result()
        result["resourceEnd"]["driver"]["cpu_seconds"] = 1
        self.assertFalse(harness.summarize(result)["valid"])
        result["resourceEnd"]["driver"]["pid"] = 43
        self.assertFalse(harness.summarize(result)["valid"])

    def test_distribution_retains_spread_and_missing_values(self):
        self.assertEqual(harness.distribution([1, 2, 6])["median"], 2)
        self.assertEqual(harness.distribution([1, 2, 6])["relativeRange"], 2.5)
        self.assertIsNone(harness.distribution([1, None])["median"])
        self.assertIsNone(harness.distribution([float("nan")])["median"])

    def test_rss_requires_numeric_pid(self):
        with self.assertRaises(ValueError):
            harness.rss_bytes("42; malicious")
        if os.name == "posix":
            self.assertGreater(harness.rss_bytes(os.getpid()), 0)

    def make_experiment(self, root, name):
        directory = root / name
        directory.mkdir()
        env = dict.fromkeys(("os", "osRelease", "arch", "host", "processors", "jdk"), "same")
        env["gitRevision"] = name
        manifest = {
            "schemaVersion": 1, "status": "complete", "repetitions": 3,
            "profiled": False, "minRateRatio": 0.98,
            "scenario": self.result()["scenario"], "environment": env,
        }
        harness.write_json(directory / "manifest.json", manifest)
        result = self.result()
        result["summary"] = harness.summarize(result)
        for i in range(3):
            iteration = directory / f"run-{i + 1:03}"
            iteration.mkdir()
            harness.write_json(iteration / "result.json", result)
        return directory, manifest

    def test_comparison_accepts_revision_changes_but_rejects_environment_changes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            before, _ = self.make_experiment(root, "before")
            after, manifest = self.make_experiment(root, "after")
            args = argparse.Namespace(baseline=before, candidate=after, output=root / "report.json")
            with contextlib.redirect_stdout(io.StringIO()):
                harness.compare(args)
            self.assertIn('"deltaPercent": 0.0', args.output.read_text())
            manifest["environment"]["host"] = "different"
            harness.write_json(after / "manifest.json", manifest)
            with self.assertRaisesRegex(ValueError, "environment.host"):
                harness.compare(args)

    def test_smoke_and_incomplete_runs_are_not_baselines(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            before, manifest = self.make_experiment(root, "before")
            after, _ = self.make_experiment(root, "after")
            manifest["scenario"]["smoke"] = True
            harness.write_json(before / "manifest.json", manifest)
            for path in before.glob("run-*/result.json"):
                result = self.result()
                result["scenario"]["smoke"] = True
                result["summary"] = harness.summarize(result)
                harness.write_json(path, result)
            args = argparse.Namespace(baseline=before, candidate=after, output=None)
            with self.assertRaisesRegex(ValueError, "Smoke"):
                harness.compare(args)
            manifest["status"] = "failed"
            harness.write_json(before / "manifest.json", manifest)
            with self.assertRaisesRegex(ValueError, "failed"):
                harness.load_run(before)

    def test_profile_and_scenario_mismatches_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            before, _ = self.make_experiment(root, "before")
            after, manifest = self.make_experiment(root, "after")
            for key, value in (("profiled", True), ("scenario", {"smoke": False, "rate": 2000})):
                altered = copy.deepcopy(manifest)
                altered[key] = value
                harness.write_json(after / "manifest.json", altered)
                with self.assertRaises(ValueError):
                    harness.compare(argparse.Namespace(baseline=before, candidate=after, output=None))

    def test_jmh_comparison_checks_settings_and_reports_allocation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            before, manifest = self.make_experiment(root, "before")
            after, _ = self.make_experiment(root, "after")
            row = {
                "benchmark": "test.aggregate", "params": {"workers": "8"}, "mode": "avgt",
                "jvmArgs": ["-Xmx1g"], "forks": 3,
                "primaryMetric": {"scoreUnit": "us/op", "rawData": [[10, 11], [9, 10]]},
                "secondaryMetrics": {
                    "gc.alloc.rate.norm": {"scoreUnit": "B/op", "rawData": [[100, 100], [100, 100]]},
                },
            }
            for directory in (before, after):
                harness.write_json(directory / "environment.json", manifest["environment"])
                harness.write_json(directory / "jmh.json", [row])
            candidate = copy.deepcopy(row)
            candidate["secondaryMetrics"]["gc.alloc.rate.norm"]["rawData"] = [[50, 50], [50, 50]]
            harness.write_json(after / "jmh.json", [candidate])
            args = argparse.Namespace(baseline=before, candidate=after, output=root / "report.json", micro=True)
            with contextlib.redirect_stdout(io.StringIO()):
                harness.compare(args)
            self.assertIn('"deltaPercent": -50.0', args.output.read_text())
            candidate["jvmArgs"] = ["-Xmx2g"]
            harness.write_json(after / "jmh.json", [candidate])
            with self.assertRaisesRegex(ValueError, "settings"):
                harness.compare(args)

    def test_fixture_callback_failures_are_invalid(self):
        result = self.result()
        result["fixtureReceiveErrors"] = 1
        self.assertIn("Synthetic receive callbacks failed", harness.summarize(result)["invalidReasons"])


if __name__ == "__main__":
    unittest.main()
