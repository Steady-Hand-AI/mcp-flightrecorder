"""Exercise release orchestration at the actual command and promotion seams."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release
from release_artifacts import ReleaseError, WHEEL


class OrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="release-orchestration-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.repo = self.root / "repo"
        self.repo.mkdir()

    def test_validate_only_refuses_every_publication_entry_before_service_access(self):
        with patch.object(release, "verify_bundle") as bundle, patch.object(release, "recheck_identity") as identity:
            for function, extra in (
                (release.plan_registry_stage, ("testpypi", self.root / "plan")),
                (release.verify_registry_stage, ("pypi", self.root / "verify")),
                (release.finalize_github_stage, ()),
            ):
                with self.subTest(function=function.__name__):
                    with self.assertRaisesRegex(ReleaseError, "explicit staged-release"):
                        function(self.repo, self.root / "bundle", "a" * 64, *extra, {"RELEASE_MODE": "validate-only"})
            bundle.assert_not_called()
            identity.assert_not_called()

    def test_stage_requires_digest_tag_and_authoritative_artifact_proof(self):
        env = {"RELEASE_MODE": "staged-release"}
        manifest = {"tag_object": "a" * 40}
        with patch.object(release, "verify_bundle", return_value=manifest) as bundle, \
             patch.object(release, "recheck_identity") as identity, \
             patch.object(release, "verify_promotion_artifact") as artifact:
            self.assertIs(release.stage_identity(self.repo, self.root, "b" * 64, env), manifest)
            bundle.assert_called_once_with(self.root, "b" * 64)
            identity.assert_called_once_with(self.repo, manifest, env)
            artifact.assert_called_once_with(manifest, env)
            artifact.side_effect = ReleaseError("artifact digest mismatch")
            with self.assertRaisesRegex(ReleaseError, "artifact digest mismatch"):
                release.stage_identity(self.repo, self.root, "b" * 64, env)
            bundle.return_value = {"tag_object": None}
            with self.assertRaisesRegex(ReleaseError, "annotated tag"):
                release.stage_identity(self.repo, self.root, "b" * 64, env)

    def test_plan_rechecks_identity_after_lookup_and_does_not_report_success_on_drift(self):
        with patch.object(release, "stage_identity", side_effect=[{}, ReleaseError("main advanced")]) as identity, \
             patch.object(release, "registry_plan", return_value=[WHEEL]) as plan:
            with self.assertRaisesRegex(ReleaseError, "main advanced"):
                release.plan_registry_stage(self.repo, self.root / "bundle", "b" * 64,
                                            "pypi", self.root / "work", {})
            self.assertEqual(identity.call_count, 2)
            plan.assert_called_once_with(self.root / "bundle", "pypi", self.root / "work/upload", "b" * 64)

    def test_registry_verification_installs_the_downloaded_wheel_and_then_rechecks(self):
        downloaded = self.root / "downloaded-from-registry"
        events = []
        with patch.object(release, "stage_identity", side_effect=lambda *a: events.append("identity")), \
             patch.object(release, "registry_verify", side_effect=lambda *a: events.append("download") or downloaded), \
             patch.object(release, "install_smoke", side_effect=lambda *a: events.append(("install", a[0]))):
            release.verify_registry_stage(self.repo, self.root / "bundle", "b" * 64,
                                          "testpypi", self.root / "work", {})
        self.assertEqual(events, ["identity", "download", ("install", downloaded / WHEEL), "identity"])

    def test_failed_download_or_install_never_completes_verification(self):
        for failure in ("download", "install"):
            with self.subTest(failure=failure), \
                 patch.object(release, "stage_identity") as identity, \
                 patch.object(release, "registry_verify", return_value=self.root / "downloaded") as download, \
                 patch.object(release, "install_smoke") as smoke:
                (download if failure == "download" else smoke).side_effect = ReleaseError("stage failure")
                with self.assertRaisesRegex(ReleaseError, "stage failure"):
                    release.verify_registry_stage(self.repo, self.root / "bundle", "b" * 64,
                                                  "pypi", self.root / failure, {})
                self.assertEqual(identity.call_count, 1)
                if failure == "download":
                    smoke.assert_not_called()

    def test_installer_uses_no_index_no_deps_and_propagates_real_command_failure(self):
        calls = []
        def invoke(label, argv, work, **kwargs):
            calls.append((label, list(map(str, argv))))
            if label == "install":
                raise ReleaseError("install failed")
        wheel = self.root / WHEEL
        with patch.object(release, "run", side_effect=invoke):
            with self.assertRaisesRegex(ReleaseError, "install failed"):
                release.install_smoke(wheel, self.repo, self.root / "installed")
        self.assertEqual([name for name, _ in calls], ["venv", "install"])
        argv = calls[-1][1]
        self.assertIn("--no-index", argv)
        self.assertIn("--no-deps", argv)
        self.assertEqual(argv[-1], str(wheel))

    def test_raw_subprocess_output_stays_local_and_failure_is_fatal(self):
        # Real child process, not a mocked return code.
        work = self.root / "work"
        work.mkdir()
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
            with self.assertRaisesRegex(ReleaseError, "release check failed"):
                release.run("synthetic", [sys.executable, "-I", "-B", "-c",
                                          "print('synthetic diagnostic'); raise SystemExit(9)"], work)
        self.assertEqual(captured.getvalue(), "")
        self.assertIn("synthetic diagnostic", (work / "logs/synthetic.log").read_text())
        self.assertEqual(json.loads((work / "commands.jsonl").read_text())["exit"], 9)

    def test_child_build_and_install_environment_has_no_workflow_credentials(self):
        with patch.dict(os.environ, {"GH_TOKEN": "synthetic", "GITHUB_TOKEN": "synthetic",
                                     "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "synthetic",
                                     "PYTHONPATH": "synthetic", "PYTEST_ADDOPTS": "synthetic",
                                     "EXAMPLE_PASSWORD": "synthetic"}):
            env = release.child_environment()
        for name in ("GH_TOKEN", "GITHUB_TOKEN", "ACTIONS_ID_TOKEN_REQUEST_TOKEN", "PYTHONPATH",
                     "PYTEST_ADDOPTS", "EXAMPLE_PASSWORD"):
            self.assertNotIn(name, env)
        self.assertEqual(env["PYTHONDONTWRITEBYTECODE"], "1")

    def test_cli_missing_identity_refuses_before_any_build(self):
        env = release.child_environment()
        env.pop("RELEASE_MODE", None)
        result = subprocess.run([sys.executable, "-I", "-B", str(Path(release.__file__)), "build",
                                 "--work", str(self.root / "never-created")],
                                env=env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Invalid release mode", result.stderr)
        self.assertFalse((self.root / "never-created").exists())

    def test_work_cannot_overwrite_source_or_existing_evidence(self):
        for path in (self.repo / "work", self.root):
            with self.subTest(path=path), self.assertRaises(ReleaseError):
                release.new_work(path, self.repo)


if __name__ == "__main__":
    unittest.main(verbosity=2)
