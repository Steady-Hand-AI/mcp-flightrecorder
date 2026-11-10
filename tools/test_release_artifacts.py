"""Behavioral checks for the immutable release pair, bundle and rebuild parity."""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_artifacts as artifacts


def identity():
    return {
        "repository": artifacts.REPOSITORY, "version": artifacts.VERSION, "tag": artifacts.TAG,
        "commit": "1" * 40, "tree": "2" * 40,
        "workflow": {"path": artifacts.WORKFLOW_PATH, "sha256": "3" * 64, "run_id": 10, "run_attempt": 1},
        "ci": {"workflow_id": 403832745, "run_id": 9, "run_attempt": 1, "head_sha": "1" * 40},
        "tag_object": None,
    }


def make_bundle(work):
    dist = work / "dist"
    dist.mkdir()
    (dist / artifacts.WHEEL).write_bytes(b"synthetic approved wheel bytes\n")
    (dist / artifacts.SDIST).write_bytes(b"synthetic approved sdist bytes\n")
    bundle = work / "bundle"
    manifest = artifacts.write_bundle(dist, bundle, identity(), {"python": "3.12.13", "build": "1.3.0"})
    return bundle, manifest, artifacts.manifest_digest(bundle)


class ReleaseArtifactsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="mfr-release-artifacts-")
        self.addCleanup(self.temporary.cleanup)
        self.work = Path(self.temporary.name)
        self.bundle, self.manifest, self.digest = make_bundle(self.work)

    def test_valid_bundle_and_exact_checksums(self):
        self.assertEqual(artifacts.verify_bundle(self.bundle, self.digest), self.manifest)
        self.assertEqual({p.name for p in self.bundle.iterdir()}, artifacts.BUNDLE_FILES)
        lines = (self.bundle / "SHA256SUMS").read_text().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            digest, name = line.split("  ")
            self.assertEqual(hashlib.sha256((self.bundle / name).read_bytes()).hexdigest(), digest)

    def test_manifest_cannot_authenticate_its_own_replacement(self):
        changed = copy.deepcopy(self.manifest)
        item = next(a for a in changed["artifacts"] if a["filename"] == artifacts.WHEEL)
        data = b"replaced artifact with self-consistent new manifest\n"
        (self.bundle / artifacts.WHEEL).write_bytes(data)
        item.update(size=len(data), sha256=hashlib.sha256(data).hexdigest())
        (self.bundle / "manifest.json").write_text(json.dumps(changed))
        (self.bundle / "SHA256SUMS").write_bytes(artifacts.checksums(changed))
        with self.assertRaisesRegex(artifacts.ReleaseError, "manifest digest mismatch"):
            artifacts.verify_bundle(self.bundle, self.digest)

    def test_changed_file_and_checksum_list_fail(self):
        wheel = self.bundle / artifacts.WHEEL
        original = wheel.read_bytes()
        wheel.write_bytes(b"X" + original[1:])
        with self.assertRaisesRegex(artifacts.ReleaseError, "distribution digest"):
            artifacts.verify_bundle(self.bundle, self.digest)
        wheel.write_bytes(original + b"X")
        with self.assertRaisesRegex(artifacts.ReleaseError, "distribution size"):
            artifacts.verify_bundle(self.bundle, self.digest)
        wheel.write_bytes(original)
        (self.bundle / "SHA256SUMS").write_bytes(b"incorrect\n")
        with self.assertRaisesRegex(artifacts.ReleaseError, "checksum list"):
            artifacts.verify_bundle(self.bundle, self.digest)

    def test_bundle_rejects_extra_missing_and_linked_members(self):
        for extra in ("rebuilt.whl", "report.json", ".hidden"):
            with self.subTest(extra=extra):
                path = self.bundle / extra
                path.write_bytes(b"synthetic\n")
                with self.assertRaisesRegex(artifacts.ReleaseError, "inventory"):
                    artifacts.verify_bundle(self.bundle, self.digest)
                path.unlink()
        path = self.bundle / artifacts.WHEEL
        data = path.read_bytes()
        path.unlink()
        with self.assertRaisesRegex(artifacts.ReleaseError, "inventory"):
            artifacts.verify_bundle(self.bundle, self.digest)
        external = self.work / "external"
        external.write_bytes(data)
        path.symlink_to(external)
        with self.assertRaisesRegex(artifacts.ReleaseError, "non-regular"):
            artifacts.verify_bundle(self.bundle, self.digest)

    def test_strict_json_rejects_duplicate_keys_and_nonfinite_values(self):
        for data in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}', b'{"a":'):
            with self.subTest(data=data):
                with self.assertRaises(artifacts.ReleaseError):
                    artifacts.strict_json(data)

    def test_manifest_shapes_identity_and_types_fail_closed(self):
        changes = [
            ("repository", "different/project"), ("version", "0.5.016"), ("tag", "v0.5.19"),
            ("commit", "A" * 40), ("commit", "1" * 7), ("tree", "2" * 39), ("schema", True),
            ("tag_object", "1" * 7), ("toolchain", {"python": "version with paths"}),
            ("artifacts", self.manifest["artifacts"] * 2),
        ]
        for key, value in changes:
            with self.subTest(key=key, value_type=type(value).__name__):
                manifest = copy.deepcopy(self.manifest)
                manifest[key] = value
                with self.assertRaises(artifacts.ReleaseError):
                    artifacts.validate_manifest(manifest)
        for field, key, value in (
            ("workflow", "path", ".github/workflows/other.yml"), ("workflow", "run_id", True),
            ("workflow", "run_attempt", 0), ("ci", "workflow_id", 1), ("ci", "head_sha", "4" * 40),
        ):
            with self.subTest(field=field, key=key):
                manifest = copy.deepcopy(self.manifest)
                manifest[field][key] = value
                with self.assertRaises(artifacts.ReleaseError):
                    artifacts.validate_manifest(manifest)
        for key, value in (("filename", "../outside.whl"), ("size", True), ("size", -1), ("sha256", "bad")):
            manifest = copy.deepcopy(self.manifest)
            manifest["artifacts"][0][key] = value
            with self.assertRaises(artifacts.ReleaseError):
                artifacts.validate_manifest(manifest)
        duplicate = copy.deepcopy(self.manifest)
        duplicate["artifacts"][1] = duplicate["artifacts"][0]
        with self.assertRaisesRegex(artifacts.ReleaseError, "duplicate"):
            artifacts.validate_manifest(duplicate)
        extra = copy.deepcopy(self.manifest)
        extra["report_path"] = "runner-local"
        with self.assertRaisesRegex(artifacts.ReleaseError, "fields"):
            artifacts.validate_manifest(extra)

    def test_bundle_writer_refuses_extra_dist_and_overwrite(self):
        dist = self.work / "dist"
        (dist / "extra.whl").write_bytes(b"third distribution")
        with self.assertRaisesRegex(artifacts.ReleaseError, "inventory"):
            artifacts.write_bundle(dist, self.work / "new", identity(), {"python": "3.12.13"})
        (dist / "extra.whl").unlink()
        with self.assertRaisesRegex(artifacts.ReleaseError, "already exists"):
            artifacts.write_bundle(dist, self.bundle, identity(), {"python": "3.12.13"})

    def test_parity_ignores_zip_timestamps_but_not_member_bytes(self):
        original, rebuilt = self.work / "original.whl", self.work / "rebuilt.whl"

        def write(path, year, data=b"pass\n", extra=False):
            with zipfile.ZipFile(path, "w") as archive:
                entry = zipfile.ZipInfo("package/module.py", date_time=(year, 1, 1, 0, 0, 0))
                archive.writestr(entry, data)
                if extra:
                    archive.writestr("package/extra.py", b"pass\n")

        write(original, 2020)
        write(rebuilt, 2025)
        self.assertNotEqual(original.read_bytes(), rebuilt.read_bytes())
        artifacts.check_parity(original, rebuilt)
        write(rebuilt, 2025, b"changed\n")
        with self.assertRaisesRegex(artifacts.ReleaseError, "member bytes"):
            artifacts.check_parity(original, rebuilt)
        write(rebuilt, 2025, extra=True)
        with self.assertRaisesRegex(artifacts.ReleaseError, "inventory"):
            artifacts.check_parity(original, rebuilt)
        with zipfile.ZipFile(rebuilt, "w") as archive:
            archive.writestr("../outside.py", b"pass\n")
        with self.assertRaisesRegex(artifacts.ReleaseError, "unsafe"):
            artifacts.check_parity(original, rebuilt)
        rebuilt.write_bytes(b"not a ZIP archive")
        with self.assertRaisesRegex(artifacts.ReleaseError, "unreadable"):
            artifacts.check_parity(original, rebuilt)

    def test_actual_candidate_pair_control_when_supplied(self):
        if not os.environ.get("MFR_GUARD_WHEEL") or not os.environ.get("MFR_GUARD_SDIST"):
            self.skipTest("actual candidate pair not supplied")
        dist = self.work / "actual-dist"
        dist.mkdir()
        for variable in ("MFR_GUARD_WHEEL", "MFR_GUARD_SDIST"):
            source = Path(os.environ[variable])
            shutil.copyfile(source, dist / source.name)
        bundle = self.work / "actual-bundle"
        artifacts.write_bundle(dist, bundle, identity(), {"python": "3.12.13"})
        artifacts.verify_bundle(bundle, artifacts.manifest_digest(bundle))


if __name__ == "__main__":
    unittest.main(verbosity=2)
