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

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SUITE = Path(__file__).resolve().parents[1]


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="omb e2e ")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.suite = self.root / "e2e-tests" / "compose"
        shutil.copytree(SUITE, self.suite, ignore=shutil.ignore_patterns("__pycache__"))
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env = os.environ.copy()
        self.env["PATH"] = str(self.bin) + os.pathsep + self.env["PATH"]
        self.env["HOME"] = str(self.root)

    def executable(self, name, text):
        path = self.bin / name
        path.write_text(text, encoding="utf-8")
        path.chmod(0o755)

    def run_suite(self, *args):
        return subprocess.run(
            [str(self.suite / "run-e2e.sh"), *args],
            cwd=self.root,
            env=self.env,
            text=True,
            capture_output=True,
            timeout=30,
        )

    def test_unknown_case_filter_is_rejected(self):
        result = self.run_suite("--list", "--cases", "redis-contract,misspelled-case")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("misspelled-case", result.stderr)

    def test_missing_and_empty_filter_values_are_rejected(self):
        for args in (("--cases",), ("--cases=",), ("--stacks", "--list")):
            with self.subTest(args=args):
                result = self.run_suite(*args)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn("Missing value", result.stderr)

    def test_invalid_timeouts_are_rejected(self):
        for timeout in ("0", "abc", "-1"):
            with self.subTest(timeout=timeout):
                result = self.run_suite("--list", "--case-timeout", timeout)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn("Invalid case timeout", result.stderr)

    def test_case_filter_rejects_cases_excluded_by_stack_filter(self):
        result = self.run_suite("--list", "--stacks", "redis", "--cases", "redis-contract,kafka-contract")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("kafka-contract", result.stderr)

    def test_case_filter_rejects_case_assigned_to_missing_stack(self):
        with (self.suite / "cases.txt").open("a", encoding="utf-8") as cases:
            cases.write("\norphan|redis|missing-stack|contract|redis.yaml|workload.yaml|orphan\n")
        result = self.run_suite("--list", "--cases", "redis-contract,orphan")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("missing-stack", result.stderr)

    def test_report_falls_back_to_host_python_when_container_exits_one(self):
        self.executable("mvn", "#!/bin/sh\nexit 1\n")
        self.executable("docker", """#!/bin/sh
case "$1" in
    info) exit 0 ;;
    version) echo "29.0 (linux/arm64)"; exit 0 ;;
    compose) echo "2.0"; exit 0 ;;
    run) echo "report image unavailable" >&2; exit 1 ;;
esac
exit 1
""")
        result = self.run_suite("--build", "host", "--cases", "redis-contract")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        latest = self.root / "e2e-tests" / "target" / "e2e-reports" / "latest"
        for name in ("index.html", "summary.md", "junit.xml", "results.json"):
            self.assertTrue((latest / name).is_file(), name + ": " + result.stdout + result.stderr)

    def test_compose_files_work_in_a_workspace_with_spaces(self):
        target = self.root / "package" / "target"
        target.mkdir(parents=True)
        (target / "openmessaging-benchmark-test-bin.tar.gz").write_bytes(b"fixture")
        self.executable("docker", """#!/bin/sh
case "$1" in
    info) exit 0 ;;
    version) echo "29.0 (linux/arm64)"; exit 0 ;;
    build) echo fake-image; exit 0 ;;
    image) echo "fake-image linux/arm64"; exit 0 ;;
    run)
        case "$*" in
            *java*) echo 'openjdk version "25"' >&2; exit 0 ;;
            *'for f in drivers/'*) echo 'drivers/driver-redis|redis'; exit 0 ;;
            *) exit 1 ;;
        esac ;;
    compose)
        shift
        if [ "$1" = version ]; then echo "2.0"; exit 0; fi
        command=""
        while [ "$#" -gt 0 ]; do
            if [ "$1" = "-f" ]; then
                shift
                [ -f "$1" ] || { echo "invalid compose file: $1" >&2; exit 97; }
            fi
            case "$1" in config|up|run|exec|logs|cp|ps|down) command="$1" ;; esac
            shift
        done
        if [ "$command" = run ]; then
            printf '%s\\n' '{"publishRate":[200,200,200,200,200,200],"consumeRate":[200,200,200,200,200,200],"publishErrorRate":[0,0,0,0,0,0],"backlog":[0,0,0,0,0,0],"aggregatedEndToEndLatencyAvg":1}' > "$OMB_E2E_CASE_DIR/result.json"
            echo 'Workload completed'
        fi
        exit 0 ;;
esac
exit 1
""")
        result = self.run_suite("--build", "skip", "--cases", "redis-contract")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
