"""Exercise registry state transitions and anonymous byte verification offline."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_artifacts as artifacts
import release_registry as registry
from test_release_artifacts import make_bundle


class RegistryFixture:
    def __init__(self, bundle, manifest, name="testpypi"):
        self.bundle, self.manifest = bundle, manifest
        self.api_host, self.file_host = registry.REGISTRIES[name]
        self.endpoint = f"https://{self.api_host}/pypi/mcp-flightrecorder/{artifacts.VERSION}/json"
        self.rows = [{"filename": item["filename"], "size": item["size"],
                      "digests": {"sha256": item["sha256"]}, "yanked": False,
                      "url": f"https://{self.file_host}/packages/synthetic/{item['filename']}"}
                     for item in manifest["artifacts"]]
        self.states = [(200, self.response(self.rows))]
        self.downloads = {row["url"]: (200, (bundle / row["filename"]).read_bytes()) for row in self.rows}
        self.calls = []

    def response(self, rows):
        return json.dumps({"info": {"name": "mcp-flightrecorder", "version": artifacts.VERSION}, "urls": rows}).encode()

    def __call__(self, url, bound):
        self.calls.append((url, bound))
        if url == self.endpoint:
            response = self.states[0]
            if len(self.states) > 1:
                self.states.pop(0)
            return response
        if url in self.downloads:
            return self.downloads[url]
        raise AssertionError("unexpected endpoint")


class ReleaseRegistryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="mfr-release-registry-")
        self.addCleanup(self.temporary.cleanup)
        self.work = Path(self.temporary.name)
        self.bundle, self.manifest, self.digest = make_bundle(self.work)
        self.fixture = RegistryFixture(self.bundle, self.manifest)

    def plan(self):
        return registry.registry_plan(self.bundle, "testpypi", self.work / "upload", self.digest, transport=self.fixture)

    def test_explicit_404_stages_both_original_files(self):
        self.fixture.states = [(404, b"")]
        self.assertEqual(set(self.plan()), artifacts.PAIR)
        for name in artifacts.PAIR:
            self.assertEqual((self.work / "upload" / name).read_bytes(), (self.bundle / name).read_bytes())

    def test_identical_partial_state_proves_existing_bytes_and_stages_missing_only(self):
        present = self.fixture.rows[0]
        self.fixture.states = [(200, self.fixture.response([present]))]
        missing = self.plan()
        self.assertEqual(set(missing), artifacts.PAIR - {present["filename"]})
        self.assertEqual({p.name for p in (self.work / "upload").iterdir()}, set(missing))
        self.assertIn(present["url"], [url for url, _ in self.fixture.calls])

    def test_completed_state_is_verified_idempotent_plan(self):
        self.assertEqual(self.plan(), [])
        self.assertEqual(list((self.work / "upload").iterdir()), [])
        self.assertEqual(len(self.fixture.calls), 3)

    def test_only_404_is_absence(self):
        for status in (401, 403, 429, 500, 503):
            with self.subTest(status=status):
                self.fixture.states = [(status, b"")]
                with self.assertRaisesRegex(artifacts.ReleaseError, "lookup did not succeed"):
                    self.plan()
                self.assertFalse((self.work / "upload").exists())
        self.fixture.states = [(200, b"not-json")]
        with self.assertRaisesRegex(artifacts.ReleaseError, "invalid JSON"):
            self.plan()
        self.fixture.states = [(200, self.fixture.response([]))]
        with self.assertRaisesRegex(artifacts.ReleaseError, "inventory"):
            self.plan()

    def test_unavailable_or_malformed_registry_cannot_stage_files(self):
        def timeout(*_):
            raise TimeoutError("synthetic timeout")

        with self.assertRaisesRegex(artifacts.ReleaseError, "request failed"):
            registry.registry_plan(self.bundle, "testpypi", self.work / "upload", self.digest, transport=timeout)
        for info in ({"name": "other", "version": artifacts.VERSION},
                     {"name": 1, "version": artifacts.VERSION},
                     {"name": "mcp-flightrecorder", "version": "0.0.0"}):
            self.fixture.states = [(200, json.dumps({"info": info, "urls": self.fixture.rows}).encode())]
            with self.assertRaisesRegex(artifacts.ReleaseError, "project/version"):
                self.plan()
        self.fixture.states = [(200, b"x" * (registry.MAX_RESPONSE_BYTES + 1))]
        with self.assertRaisesRegex(artifacts.ReleaseError, "oversized"):
            self.plan()
        self.assertFalse((self.work / "upload").exists())

    def test_conflicting_duplicate_extra_and_yanked_files_stop_before_staging(self):
        cases = []
        for field, value in (("size", 1), ("size", True), ("digests", {"sha256": "0" * 64}),
                             ("filename", "unexpected.whl"), ("yanked", True)):
            rows = copy.deepcopy(self.fixture.rows)
            rows[0][field] = value
            cases.append(rows)
        cases.append([self.fixture.rows[0], self.fixture.rows[0]])
        cases.append(self.fixture.rows + [self.fixture.rows[0]])
        for rows in cases:
            with self.subTest(kind=list(rows[0])):
                self.fixture.states = [(200, self.fixture.response(rows))]
                with self.assertRaises(artifacts.ReleaseError):
                    self.plan()
                self.assertFalse((self.work / "upload").exists())

    def test_advertised_hash_is_not_download_proof(self):
        row = self.fixture.rows[0]
        approved = self.fixture.downloads[row["url"]][1]
        self.fixture.downloads[row["url"]] = (200, b"X" + approved[1:])
        with self.assertRaisesRegex(artifacts.ReleaseError, "downloaded registry bytes"):
            self.plan()
        self.assertFalse((self.work / "upload").exists())
        self.fixture.downloads[row["url"]] = (200, approved + b"extra")
        with self.assertRaisesRegex(artifacts.ReleaseError, "oversized"):
            self.plan()

    def test_download_origins_and_paths_are_checked_before_request(self):
        urls = [
            "http://test-files.pythonhosted.org/packages/file.whl",
            "https://files.pythonhosted.org/packages/file.whl",
            "https://test-files.pythonhosted.org.evil.invalid/packages/file.whl",
            "https://user@test-files.pythonhosted.org/packages/file.whl",
            "https://test-files.pythonhosted.org:444/packages/file.whl",
            "https://test-files.pythonhosted.org/packages/file.whl?token=synthetic",
        ]
        for url in urls:
            rows = copy.deepcopy(self.fixture.rows)
            rows[0]["url"] = url
            self.fixture.states = [(200, self.fixture.response(rows))]
            self.fixture.calls.clear()
            with self.assertRaises(artifacts.ReleaseError):
                self.plan()
            self.assertEqual(len(self.fixture.calls), 1)

    def test_bounded_propagation_404_partial_complete_downloads_verified_pair(self):
        self.fixture.states = [(404, b""), (200, self.fixture.response(self.fixture.rows[:1])),
                               (200, self.fixture.response(self.fixture.rows))]
        sleeps = []
        destination = registry.registry_verify(self.bundle, "testpypi", self.work / "verified", self.digest,
                                               transport=self.fixture, attempts=3, delay=1, sleep=sleeps.append)
        self.assertEqual(sleeps, [1, 1])
        self.assertEqual({p.name for p in destination.iterdir()}, artifacts.PAIR)
        for name in artifacts.PAIR:
            self.assertEqual((destination / name).read_bytes(), (self.bundle / name).read_bytes())
        download_calls = [url for url, _ in self.fixture.calls if url != self.fixture.endpoint]
        self.assertEqual(len(download_calls), 3)

    def test_propagation_exhaustion_and_conflict_do_not_create_success_directory(self):
        self.fixture.states = [(404, b"")]
        with self.assertRaisesRegex(artifacts.ReleaseError, "propagation incomplete"):
            registry.registry_verify(self.bundle, "testpypi", self.work / "verified", self.digest,
                                     transport=self.fixture, attempts=2, delay=0, sleep=lambda _: None)
        self.assertFalse((self.work / "verified").exists())
        self.assertEqual(len(self.fixture.calls), 2)
        rows = copy.deepcopy(self.fixture.rows)
        rows[0]["size"] += 1
        self.fixture.states = [(200, self.fixture.response(rows))]
        self.fixture.calls.clear()
        with self.assertRaisesRegex(artifacts.ReleaseError, "conflicting"):
            registry.registry_verify(self.bundle, "testpypi", self.work / "verified", self.digest,
                                     transport=self.fixture, attempts=3, delay=0, sleep=lambda _: self.fail("must not retry conflict"))
        self.assertEqual(len(self.fixture.calls), 1)

    def test_pypi_performs_its_own_lookup_and_downloads(self):
        fixture = RegistryFixture(self.bundle, self.manifest, "pypi")
        registry.registry_verify(self.bundle, "pypi", self.work / "pypi-verified", self.digest, transport=fixture)
        self.assertEqual(len(fixture.calls), 3)
        self.assertTrue(all("test" not in url for url, _ in fixture.calls))
        with self.assertRaises(artifacts.ReleaseError):
            registry.registry_verify(self.bundle, "pypi", self.work / "wrong-registry", self.digest,
                                     transport=self.fixture)

    def test_manifest_tamper_and_missing_digest_prevent_network_access(self):
        for digest in (None, "0" * 64):
            with self.assertRaises(artifacts.ReleaseError):
                registry.registry_plan(self.bundle, "testpypi", self.work / "upload", digest,
                                       transport=lambda *_: self.fail("network must not run"))

    def test_local_change_during_registry_read_cannot_change_upload_bytes(self):
        def changed_during_lookup(*_):
            path = self.bundle / artifacts.WHEEL
            original = path.read_bytes()
            path.write_bytes(b"X" + original[1:])
            return 404, b""

        with self.assertRaisesRegex(artifacts.ReleaseError, "staged distribution"):
            registry.registry_plan(self.bundle, "testpypi", self.work / "upload", self.digest,
                                   transport=changed_during_lookup)
        self.assertFalse((self.work / "upload").exists())

    def test_anonymous_transport_rejects_off_origin_redirect(self):
        def opener(handler):
            class Opener:
                def open(self, request, timeout):
                    self_request = request
                    self.assert_no_auth = self_request.get_header("Authorization") is None
                    if not self.assert_no_auth:
                        raise AssertionError("unexpected credentials")
                    handler.redirect_request(request, None, 302, "redirect", {}, "https://example.invalid/file")
            return Opener()
        with patch.object(registry, "build_opener", side_effect=opener):
            with self.assertRaisesRegex(artifacts.ReleaseError, "allowed HTTPS"):
                registry.anonymous_get(self.fixture.rows[0]["url"], 100)

    def test_anonymous_transport_bounds_download_and_omits_authorization(self):
        requests = []

        class Response:
            status = 200
            headers = {}
            data = b"verified bytes"

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self, count):
                return self.data[:count]

        response = Response()

        class Opener:
            def open(self, request, timeout):
                requests.append((request, timeout))
                return response

        with patch.object(registry, "build_opener", return_value=Opener()):
            self.assertEqual(registry.anonymous_get(self.fixture.rows[0]["url"], 100), (200, b"verified bytes"))
            self.assertIsNone(requests[0][0].get_header("Authorization"))
            self.assertEqual(requests[0][1], 30)
            response.headers = {"Content-Length": "101"}
            with self.assertRaisesRegex(artifacts.ReleaseError, "size bound"):
                registry.anonymous_get(self.fixture.rows[0]["url"], 100)
            response.headers = {}
            response.data = b"x" * 101
            with self.assertRaisesRegex(artifacts.ReleaseError, "size bound"):
                registry.anonymous_get(self.fixture.rows[0]["url"], 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
