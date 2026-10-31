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
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

DRIVER_DIR = Path(__file__).resolve().parents[1]
DIST_NAME = "openmessaging-benchmark-test"
PULSAR_SHA = "d4c744c8e5bdedb8f373ece959ebf2256128fc31248d5c2db909bce913ed7f3a"


class PackageScriptsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="omb jms ")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.driver = self.root / "driver-jms"
        self.driver.mkdir()
        for name in ("package-common.sh", "package-kafka.sh", "package-pulsar.sh"):
            shutil.copy(DRIVER_DIR / name, self.driver / name)
        self.target = self.root / "package" / "target"
        self.target.mkdir(parents=True)
        fixture = self.root / DIST_NAME
        ext = fixture / "drivers" / "driver-jms" / "ext"
        ext.mkdir(parents=True)
        (ext / "pulsar-jms-all-1.2.2.jar").write_text("old", encoding="utf-8")
        (ext / "other-provider.jar").write_text("keep", encoding="utf-8")
        self.archive = self.target / (DIST_NAME + "-bin.tar.gz")
        with tarfile.open(self.archive, "w:gz") as archive:
            archive.add(fixture, arcname=DIST_NAME)
        stale = self.target / DIST_NAME / "drivers" / "driver-jms" / "ext"
        stale.mkdir(parents=True)
        (stale / "stale.jar").write_text("stale", encoding="utf-8")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env = os.environ.copy()
        self.env["PATH"] = str(self.bin) + os.pathsep + self.env["PATH"]
        self.env["FAKE_CURL_LOG"] = str(self.root / "curl.log")

    def executable(self, name, content):
        path = self.bin / name
        path.write_text(content, encoding="utf-8")
        path.chmod(0o755)

    def run_script(self, name, *args):
        return subprocess.run(
            ["bash", str(self.driver / name), *args],
            cwd=self.root,
            env=self.env,
            text=True,
            capture_output=True,
        )

    def members(self):
        with tarfile.open(self.archive, "r:gz") as archive:
            return {member.name: archive.extractfile(member).read()
                    for member in archive if member.isfile()}

    def fake_download(self, digest=PULSAR_SHA):
        self.executable("curl", """#!/bin/sh
printf '%s\\n' "$2" > "$FAKE_CURL_LOG"
printf 'fake provider' > "$4"
""")
        self.executable("sha256sum", "#!/bin/sh\nprintf '%s  %s\\n' " + digest + ' "$1"\n')

    def test_kafka_relative_path_and_clean_extraction(self):
        jar = self.root / "provider files" / "kafka-client.jar"
        jar.parent.mkdir()
        jar.write_text("kafka provider", encoding="utf-8")
        result = self.run_script("package-kafka.sh", "provider files/kafka-client.jar")
        self.assertEqual(result.returncode, 0, result.stderr)
        files = self.members()
        ext = DIST_NAME + "/drivers/driver-jms/ext/"
        self.assertEqual(files[ext + "kafka-client.jar"], b"kafka provider")
        self.assertNotIn(ext + "stale.jar", files)
        self.assertEqual(files[ext + "other-provider.jar"], b"keep")

    def test_pulsar_uses_verified_version_and_replaces_older_versions(self):
        self.fake_download()
        result = self.run_script("package-pulsar.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("/6.0.11/pulsar-jms-all-6.0.11.jar",
                      (self.root / "curl.log").read_text(encoding="utf-8"))
        files = self.members()
        ext = DIST_NAME + "/drivers/driver-jms/ext/"
        self.assertEqual(files[ext + "pulsar-jms-all-6.0.11.jar"], b"fake provider")
        self.assertNotIn(ext + "pulsar-jms-all-1.2.2.jar", files)
        self.assertEqual(files[ext + "other-provider.jar"], b"keep")

    def test_checksum_failure_preserves_original_tarball(self):
        original = self.archive.read_bytes()
        self.fake_download("0" * 64)
        result = self.run_script("package-pulsar.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("SHA-256 mismatch", result.stderr)
        self.assertEqual(self.archive.read_bytes(), original)
        self.assertFalse(list(self.target.glob(".omb-jms.*")))

    def test_archive_failure_preserves_original_tarball(self):
        original = self.archive.read_bytes()
        jar = self.root / "provider.jar"
        jar.write_text("provider", encoding="utf-8")
        system_tar = shlex.quote(shutil.which("tar"))
        self.executable("tar", """#!/bin/sh
if [ "$1" = "-czf" ]; then echo "archive failed" >&2; exit 1; fi
exec """ + system_tar + ' "$@"\n')
        result = self.run_script("package-kafka.sh", str(jar))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("archive failed", result.stderr)
        self.assertEqual(self.archive.read_bytes(), original)
        self.assertFalse(list(self.target.glob(".omb-jms.*")))

    def test_multiple_archives_are_rejected(self):
        shutil.copy(self.archive, self.target / "openmessaging-benchmark-other-bin.tar.gz")
        result = self.run_script("package-pulsar.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Expected exactly one distribution tarball", result.stderr)


if __name__ == "__main__":
    unittest.main()
