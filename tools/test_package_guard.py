"""Exercise real clean artifacts and disposable, deliberately invalid neighbors."""
import importlib.util
import base64
import csv
from email.parser import BytesParser
import hashlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
import warnings
import zipfile

spec = importlib.util.spec_from_file_location("check_package", Path(__file__).with_name("check_package.py"))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class PackageGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = Path(os.environ["MFR_GUARD_SOURCE"]).resolve()
        cls.wheel = Path(os.environ["MFR_GUARD_WHEEL"]).resolve()
        cls.sdist = Path(os.environ["MFR_GUARD_SDIST"]).resolve()
        for path in (cls.source, cls.wheel, cls.sdist):
            if not path.exists():
                raise RuntimeError("real clean source/wheel/sdist required")

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="mfr-guard-")
        self.addCleanup(self.temporary.cleanup)
        self.work = Path(self.temporary.name)
        self.assertFalse(self.work.is_relative_to(self.source))

    def source_copy(self):
        return Path(shutil.copytree(self.source, self.work / "source"))

    def wheel_copy(self, *, additions=(), replacement=None, drop=None, update_record=False):
        target = self.work / "changed.whl"
        with zipfile.ZipFile(self.wheel) as original, zipfile.ZipFile(target, "w") as changed:
            contents = {}
            for entry in original.infolist():
                if entry.filename != drop:
                    data = original.read(entry)
                    if replacement and entry.filename == replacement[0]:
                        data = replacement[1]
                    contents[entry.filename] = (entry, data)
            if update_record:
                record = next(name for name in contents if name.endswith("/RECORD"))
                output = io.StringIO(newline="")
                writer = csv.writer(output)
                for name, (_, data) in contents.items():
                    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
                    writer.writerow((name, "" if name == record else "sha256=" + digest,
                                     "" if name == record else len(data)))
                contents[record] = (contents[record][0], output.getvalue().encode())
            for entry, data in contents.values():
                changed.writestr(entry, data)
            for name, data in additions:
                changed.writestr(name, data)
        return target

    def sdist_copy(self, *, addition=None, replacement=None, replacements=None):
        target = self.work / "changed.tar.gz"
        replacements = dict(replacements or {})
        if replacement:
            replacements[replacement[0]] = replacement[1]
        with tarfile.open(self.sdist) as original, tarfile.open(target, "w:gz") as changed:
            for entry in original:
                data = original.extractfile(entry).read() if entry.isfile() else None
                if entry.name in replacements:
                    data = replacements[entry.name]
                    entry.size = len(data)
                changed.addfile(entry, io.BytesIO(data) if data is not None else None)
            if addition:
                entry, data = addition
                changed.addfile(entry, io.BytesIO(data) if data is not None else None)
        return target

    def test_real_clean_source_wheel_and_sdist(self):
        result = guard.check(self.source, self.wheel, self.sdist)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(len(result["wheel"]["members"]), 39)
        self.assertGreater(len(result["sdist"]["members"]), 140)

    def test_unlisted_source_files_are_rejected(self):
        source = self.source_copy()
        for name in ("PLAN.md", "docs/notes.md", "evidence/raw.json", "data.sqlite", "nested.zip", ".env"):
            with self.subTest(name=name):
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("synthetic fixture\n")
                with self.assertRaises(guard.Rejected):
                    guard.check(source)
                path.unlink()
                if path.parent.name == "evidence":
                    path.parent.rmdir()

    def test_forbidden_name_cannot_be_admitted_by_inventory(self):
        source = self.source_copy()
        (source / "TASK.md").write_text("synthetic task\n")
        with (source / "tools/public_files.txt").open("a") as output:
            output.write("TASK.md\n")
        with self.assertRaisesRegex(guard.Rejected, "forbidden member"):
            guard.check(source)

    def test_missing_source_file(self):
        source = self.source_copy()
        (source / "NOTICE").unlink()
        with self.assertRaisesRegex(guard.Rejected, "source inventory differs"):
            guard.check(source)

    def test_source_symlink_is_rejected(self):
        source = self.source_copy()
        (source / "README.md").unlink()
        (source / "README.md").symlink_to(self.source / "README.md")
        with self.assertRaisesRegex(guard.Rejected, "source link"):
            guard.check(source)

    def test_specific_content_is_rejected_without_echoing_it(self):
        source = self.source_copy()
        examples = [
            "/" + "Users/synthetic-person/private/notes",
            "/" + "Volumes/synthetic-disk/private/notes",
            "-----BEGIN " + "PRIVATE KEY-----",
            "ghp_" + "a" * 36,
            "AKIA" + "A" * 16,
            "sk-proj-" + "z" * 40,
        ]
        for value in examples:
            with self.subTest(kind=value[:3]):
                (source / "README.md").write_text(value)
                with self.assertRaisesRegex(guard.Rejected, "value omitted") as caught:
                    guard.check(source)
                self.assertNotIn(value, str(caught.exception))
        completed = subprocess.run([sys.executable, "-I", "-B", str(Path(guard.__file__)),
                                    "--source", str(source)], capture_output=True, text=True)
        self.assertEqual(completed.returncode, 1)
        self.assertIn("value omitted", completed.stdout)
        self.assertNotIn(examples[-1], completed.stdout + completed.stderr)

    def test_ordinary_security_documentation_is_valid(self):
        source = self.source_copy()
        with (source / "README.md").open("a") as output:
            output.write("\nDo not record passwords, tokens or secrets. Use an API key from your environment.\n")
        self.assertEqual(guard.check(source)["status"], "PASS")

    def test_unexpected_wheel_member(self):
        wheel = self.wheel_copy(additions=[("mcp_flightrecorder/extra.py", b"pass\n")])
        with self.assertRaisesRegex(guard.Rejected, "wheel inventory differs"):
            guard.check(self.source, wheel)

    def test_missing_wheel_schema(self):
        wheel = self.wheel_copy(drop="mcp_flightrecorder/schema.sql")
        with self.assertRaisesRegex(guard.Rejected, "wheel inventory differs"):
            guard.check(self.source, wheel)

    def test_wheel_content_cannot_diverge_from_source(self):
        wheel = self.wheel_copy(replacement=("mcp_flightrecorder/schema.sql", b"select 1;\n"))
        with self.assertRaisesRegex(guard.Rejected, "wheel content differs"):
            guard.check(self.source, wheel)

    def test_wheel_metadata_and_record_are_checked(self):
        with zipfile.ZipFile(self.wheel) as archive:
            metadata = next(n for n in archive.namelist() if n.endswith("/METADATA"))
            record = next(n for n in archive.namelist() if n.endswith("/RECORD"))
            data = archive.read(metadata)
            for original, changed, reason in (
                (b"Version: 0.5.16", b"Version: 0.0.0", "metadata Version"),
                (b"https://github.com/Steady-Hand-AI/mcp-flightrecorder/issues", b"https://example.invalid/issues", "Project-URL"),
                (b"Author: Volodymyr Barylyak", b"Author: Somebody Else", "metadata Author"),
                (b"Requires-Python: >=3.11", b"Requires-Dist: unexpected-package>=1\nRequires-Python: >=3.11", "dependency metadata"),
            ):
                with self.subTest(reason=reason):
                    self.assertIn(original, data)
                    wheel = self.wheel_copy(replacement=(metadata, data.replace(original, changed)))
                    with self.assertRaisesRegex(guard.Rejected, reason):
                        guard.check(self.source, wheel)
            wheel = self.wheel_copy(replacement=(record, archive.read(record).replace(b"sha256=", b"sha256=X", 1)))
        with self.assertRaisesRegex(guard.Rejected, "RECORD mismatch"):
            guard.check(self.source, wheel)

    def assert_valid_record(self, wheel):
        with zipfile.ZipFile(wheel) as archive:
            record = next(name for name in archive.namelist() if name.endswith("/RECORD"))
            rows = list(csv.reader(io.StringIO(archive.read(record).decode())))
            self.assertEqual(len(rows), len(archive.namelist()))
            self.assertEqual({row[0] for row in rows}, set(archive.namelist()))
            for name, digest, size in rows:
                if name == record:
                    self.assertEqual((digest, size), ("", ""))
                else:
                    data = archive.read(name)
                    self.assertEqual(digest, "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode())
                    self.assertEqual(size, str(len(data)))

    def test_wheel_description_and_content_type_match_source(self):
        with zipfile.ZipFile(self.wheel) as archive:
            name = next(n for n in archive.namelist() if n.endswith("/METADATA"))
            original = archive.read(name)
        header, separator, body = original.partition(b"\n\n")
        self.assertEqual(separator, b"\n\n")
        self.assertEqual(body, (self.source / "README.md").read_bytes())
        for data, reason in (
            (header + separator + b"Unreviewed generated description.\n", "metadata description"),
            (header + separator, "metadata description"),
            (original.replace(b"Description-Content-Type: text/markdown", b"Description-Content-Type: text/plain", 1), "Description-Content-Type"),
        ):
            with self.subTest(reason=reason):
                self.assertNotEqual(data, original)
                wheel = self.wheel_copy(replacement=(name, data), update_record=True)
                self.assert_valid_record(wheel)
                with self.assertRaisesRegex(guard.Rejected, reason):
                    guard.check(self.source, wheel)

    def test_both_sdist_descriptions_and_content_types_match_source(self):
        with tarfile.open(self.sdist) as archive:
            originals = {entry.name: archive.extractfile(entry).read() for entry in archive
                         if entry.isfile() and entry.name.endswith("/PKG-INFO")}
        self.assertEqual(len(originals), 2)
        for name, original in originals.items():
            header, separator, body = original.partition(b"\n\n")
            self.assertEqual(body, (self.source / "README.md").read_bytes())
            for data, reason in (
                (header + separator + b"Unreviewed generated description.\n", "metadata description"),
                (original.replace(b"Description-Content-Type: text/markdown", b"Description-Content-Type: text/plain", 1), "Description-Content-Type"),
            ):
                with self.subTest(member=name, reason=reason):
                    self.assertNotEqual(data, original)
                    sdist = self.sdist_copy(replacement=(name, data))
                    with self.assertRaisesRegex(guard.Rejected, reason):
                        guard.check(self.source, sdist=sdist)

    def test_utf8_readme_is_preserved_in_all_metadata(self):
        source = self.source_copy()
        readme = (source / "README.md").read_bytes() + "\nUnicode control: café, π, 日本語.\n".encode("utf-8")
        (source / "README.md").write_bytes(readme)
        with zipfile.ZipFile(self.wheel) as archive:
            name = next(n for n in archive.namelist() if n.endswith("/METADATA"))
            metadata = archive.read(name).partition(b"\n\n")[0] + b"\n\n" + readme
        self.assertEqual(BytesParser().parsebytes(metadata).get_payload(decode=True), readme)
        wheel = self.wheel_copy(replacement=(name, metadata), update_record=True)
        self.assert_valid_record(wheel)
        with tarfile.open(self.sdist) as archive:
            replacements = {entry.name: archive.extractfile(entry).read().partition(b"\n\n")[0] + b"\n\n" + readme
                            for entry in archive if entry.isfile() and entry.name.endswith("/PKG-INFO")}
        self.assertEqual(len(replacements), 2)
        replacements["mcp_flightrecorder-0.5.16/README.md"] = readme
        sdist = self.sdist_copy(replacements=replacements)
        self.assertEqual(guard.check(source, wheel, sdist)["status"], "PASS")

    def test_unsafe_wheel_paths_and_duplicates(self):
        for name in ("../outside.py", "/absolute.py", "C:/drive.py", "a\\b.py", "a//b.py"):
            with self.subTest(name=name):
                wheel = self.wheel_copy(additions=[(name, b"synthetic\n")])
                with self.assertRaisesRegex(guard.Rejected, "unsafe member path"):
                    guard.check(self.source, wheel)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            wheel = self.wheel_copy(additions=[("mcp_flightrecorder/__init__.py", b"pass\n")])
        with self.assertRaisesRegex(guard.Rejected, "duplicate archive member"):
            guard.check(self.source, wheel)

    def test_archive_links_are_rejected(self):
        entry = zipfile.ZipInfo("link")
        entry.external_attr = 0o120777 << 16
        wheel = self.wheel_copy(additions=[(entry, b"outside")])
        with self.assertRaisesRegex(guard.Rejected, "non-regular wheel"):
            guard.check(self.source, wheel)
        entry = tarfile.TarInfo("mcp_flightrecorder-0.5.16/link")
        entry.type = tarfile.SYMTYPE
        entry.linkname = "../../outside"
        sdist = self.sdist_copy(addition=(entry, None))
        with self.assertRaisesRegex(guard.Rejected, "non-regular sdist"):
            guard.check(self.source, sdist=sdist)

    def test_sdist_path_and_private_member_are_rejected(self):
        for name, reason in (("../outside", "unsafe member path"),
                             ("mcp_flightrecorder-0.5.16/AGENTS.md", "forbidden member"),
                             ("mcp_flightrecorder-0.5.16/extra.py", "sdist inventory differs")):
            with self.subTest(name=name):
                entry = tarfile.TarInfo(name)
                entry.size = 10
                sdist = self.sdist_copy(addition=(entry, b"synthetic\n"))
                with self.assertRaisesRegex(guard.Rejected, reason):
                    guard.check(self.source, sdist=sdist)

    def test_sdist_source_bytes_and_manifest_are_checked(self):
        sdist = self.sdist_copy(replacement=("mcp_flightrecorder-0.5.16/NOTICE", b"changed\n"))
        with self.assertRaisesRegex(guard.Rejected, "sdist content differs"):
            guard.check(self.source, sdist=sdist)
        sdist = self.sdist_copy(replacement=("mcp_flightrecorder-0.5.16/src/mcp_flightrecorder.egg-info/SOURCES.txt", b"README.md\n"))
        with self.assertRaisesRegex(guard.Rejected, "sdist build manifest"):
            guard.check(self.source, sdist=sdist)


if __name__ == "__main__":
    unittest.main(verbosity=2)
