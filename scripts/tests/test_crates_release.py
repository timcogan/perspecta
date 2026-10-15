"""Exercise the release workflow's registry check without network access."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]


def registry_check_script():
    workflow = (ROOT / ".github/workflows/release.yml").read_text()
    job = workflow.split("\n  publish-crate:\n", 1)[1]
    step = job.split("      - name: Check crates.io version\n", 1)[1]
    block = step.split("        run: |\n", 1)[1]
    lines = []
    for line in block.splitlines():
        if line and not line.startswith("          "):
            break
        lines.append(line)
    return textwrap.dedent("\n".join(lines))


class CratesReleaseCheckTests(unittest.TestCase):
    def run_check(self, *, status="404", body=None, tag="v0.6.0", curl_exit=0):
        self.assertIsNotNone(shutil.which("jq"), "The workflow tests require jq")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            output = directory / "output"
            curl_called = directory / "curl-called"
            cargo = directory / "cargo"
            cargo.write_text(
                "#!/usr/bin/env python3\n"
                "import json, sys\n"
                "assert sys.argv[1:] == ['metadata', '--no-deps', '--locked', '--format-version=1']\n"
                "print(json.dumps({'packages': [{'name': 'perspecta', 'version': '0.6.0'}]}))\n"
            )
            curl = directory / "curl"
            curl.write_text(
                "#!/usr/bin/env python3\n"
                "import os, pathlib, sys\n"
                "pathlib.Path(os.environ['TEST_CURL_CALLED']).touch()\n"
                "assert sys.argv[-1] == 'https://crates.io/api/v1/crates/perspecta/0.6.0'\n"
                "if int(os.environ['TEST_CURL_EXIT']):\n"
                "    sys.exit(int(os.environ['TEST_CURL_EXIT']))\n"
                "destination = sys.argv[sys.argv.index('--output') + 1]\n"
                "pathlib.Path(destination).write_text(os.environ['TEST_RESPONSE'])\n"
                "print(os.environ['TEST_STATUS'], end='')\n"
            )
            cargo.chmod(0o755)
            curl.chmod(0o755)
            environment = dict(
                os.environ,
                PATH=str(directory) + os.pathsep + os.environ.get("PATH", ""),
                GITHUB_OUTPUT=str(output),
                RELEASE_TAG=tag,
                TEST_STATUS=status,
                TEST_RESPONSE=json.dumps(body) if body is not None else "not JSON",
                TEST_CURL_EXIT=str(curl_exit),
                TEST_CURL_CALLED=str(curl_called),
            )
            result = subprocess.run(
                ["bash", "-c", registry_check_script()],
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            outputs = output.read_text() if output.exists() else ""
            return result, outputs, curl_called.exists()

    def test_missing_version_requests_publication(self):
        result, outputs, called = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(outputs, "publish=true\n")
        self.assertTrue(called)

    def test_existing_version_skips_publication(self):
        body = {"version": {"crate": "perspecta", "num": "0.6.0", "yanked": False}}
        result, outputs, _ = self.run_check(status="200", body=body)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(outputs, "publish=false\n")

    def test_tag_mismatch_fails_before_registry_request(self):
        result, outputs, called = self.run_check(tag="v0.7.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not match", result.stderr)
        self.assertEqual(outputs, "")
        self.assertFalse(called)

    def test_network_error_does_not_request_publication(self):
        result, outputs, _ = self.run_check(curl_exit=7)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(outputs, "")

    def test_registry_errors_do_not_request_publication(self):
        for status in ("401", "403", "429", "500", "503"):
            with self.subTest(status=status):
                result, outputs, _ = self.run_check(status=status)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Could not check crates.io", result.stderr)
                self.assertEqual(outputs, "")

    def test_invalid_existing_version_does_not_request_publication(self):
        bodies = [
            None,
            {},
            {"version": {"crate": "perspecta", "num": "0.6.0", "yanked": True}},
            {"version": {"crate": "perspecta", "num": "0.5.3", "yanked": False}},
            {"version": {"crate": "other-crate", "num": "0.6.0", "yanked": False}},
        ]
        for body in bodies:
            with self.subTest(body=body):
                result, outputs, _ = self.run_check(status="200", body=body)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(outputs, "")


if __name__ == "__main__":
    unittest.main()
