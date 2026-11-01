"""Offline proofs for authoritative CI and conflict-safe release reconciliation."""

import base64
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from release_artifacts import ReleaseError, SDIST, WHEEL, write_bundle
import release_github as github


COMMIT = "a" * 40
TREE = "b" * 40
TAG_OBJECT = "c" * 40
WORKFLOW_BYTES = b"name: publish\n"


def identity():
    return {"repository": github.REPOSITORY, "version": github.VERSION, "tag": github.TAG,
            "commit": COMMIT, "tree": TREE, "tag_object": TAG_OBJECT,
            "workflow": {"path": github.WORKFLOW_PATH, "sha256": hashlib.sha256(WORKFLOW_BYTES).hexdigest(),
                         "run_id": 20, "run_attempt": 1},
            "ci": {"workflow_id": github.CI_WORKFLOW_ID, "run_id": 10, "run_attempt": 2, "head_sha": COMMIT}}


def environment(commit=COMMIT, mode="staged-release"):
    return {"RELEASE_MODE": mode, "RELEASE_VERSION": github.VERSION, "PLANNED_TAG": github.TAG,
            "EXPECTED_COMMIT": commit, "GITHUB_SHA": commit, "GITHUB_WORKFLOW_SHA": commit,
            "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/main",
            "GITHUB_REPOSITORY": github.REPOSITORY,
            "GITHUB_WORKFLOW_REF": f"{github.REPOSITORY}/{github.WORKFLOW_PATH}@refs/heads/main",
            "GITHUB_RUN_ID": "20", "GITHUB_RUN_ATTEMPT": "1"}


class IdentityClient:
    def __init__(self, source=None):
        source = identity() if source is None else source
        commit = source["commit"]
        root = github.API_ROOT
        self.values = {
            root: {"full_name": github.REPOSITORY, "default_branch": "main"},
            root + "/git/ref/heads/main": {"ref": "refs/heads/main", "object": {"type": "commit", "sha": commit}},
            root + "/git/commits/" + commit: {"sha": commit, "tree": {"sha": source["tree"]}},
            root + "/contents/" + github.WORKFLOW_PATH + "?ref=" + commit:
                {"type": "file", "path": github.WORKFLOW_PATH, "encoding": "base64",
                 "content": base64.b64encode(WORKFLOW_BYTES).decode()},
            root + "/git/ref/tags/" + github.TAG:
                {"ref": "refs/tags/" + github.TAG, "object": {"type": "tag", "sha": TAG_OBJECT}},
            root + "/git/tags/" + TAG_OBJECT:
                {"sha": TAG_OBJECT, "tag": github.TAG, "object": {"type": "commit", "sha": commit}},
            root + f"/actions/workflows/{github.CI_WORKFLOW_ID}":
                {"id": github.CI_WORKFLOW_ID, "path": github.CI_PATH},
            root + "/actions/runs/10": {
                "id": 10, "workflow_id": github.CI_WORKFLOW_ID, "path": github.CI_PATH,
                "repository": {"full_name": github.REPOSITORY}, "head_repository": {"full_name": github.REPOSITORY},
                "head_sha": commit, "head_branch": "main", "event": "push", "status": "completed",
                "conclusion": "success", "run_attempt": 2, "check_suite_id": 30,
            },
            root + "/check-suites/30": {"id": 30, "app": {"id": 15368}, "head_sha": commit,
                                        "status": "completed", "conclusion": "success"},
        }
        self.jobs = [{"id": index, "name": name, "run_id": 10, "run_attempt": 2, "head_sha": commit,
                      "status": "completed", "conclusion": "success"}
                     for index, name in enumerate(sorted(github.CI_JOBS), start=1)]
        self.runs = [{"id": 10}]
        self.reads = []

    def get_json(self, path, *, allow_404=False):
        self.reads.append(path)
        if path not in self.values:
            if allow_404:
                return None
            raise ReleaseError("Synthetic API unavailable")
        value = self.values[path]
        if isinstance(value, Exception):
            raise value
        return deepcopy(value)

    def pages(self, path, key=None):
        self.reads.append(path)
        if key == "workflow_runs":
            return deepcopy(self.runs)
        if key == "jobs" and path.endswith("/actions/runs/10/attempts/2/jobs"):
            return deepcopy(self.jobs)
        raise ReleaseError("Wrong attempt-specific request")


class IdentityTests(unittest.TestCase):
    def test_valid_ci_uses_exact_attempt_and_actions_app(self):
        client = IdentityClient()
        self.assertEqual(github._ci_identity(client, COMMIT), identity()["ci"])
        self.assertIn(github.API_ROOT + "/check-suites/30", client.reads)
        self.assertIn(github.API_ROOT + "/actions/runs/10/attempts/2/jobs", client.reads)

    def test_wrong_missing_failed_or_unverifiable_ci_is_fatal(self):
        for field, value in [("workflow_id", 99), ("path", ".github/workflows/other.yml"),
                             ("head_sha", "d" * 40), ("head_branch", "other"), ("event", "pull_request"),
                             ("status", "in_progress"), ("conclusion", "failure"),
                             ("repository", {"full_name": "elsewhere/repo"}),
                             ("head_repository", {"full_name": "elsewhere/repo"})]:
            with self.subTest(field=field):
                client = IdentityClient()
                client.values[github.API_ROOT + "/actions/runs/10"][field] = value
                with self.assertRaises(ReleaseError):
                    github._ci_identity(client, COMMIT)
        for fault in ("missing", "duplicate", "skipped", "pending", "wrong_sha", "wrong_attempt", "spoof_app", "api_denied"):
            with self.subTest(fault=fault):
                client = IdentityClient()
                if fault == "missing":
                    client.jobs.pop()
                elif fault == "duplicate":
                    client.jobs[-1] = deepcopy(client.jobs[0])
                elif fault == "skipped":
                    client.jobs[0]["conclusion"] = "skipped"
                elif fault == "pending":
                    client.jobs[0]["status"] = "queued"
                elif fault == "wrong_sha":
                    client.jobs[0]["head_sha"] = "d" * 40
                elif fault == "wrong_attempt":
                    client.jobs[0]["run_attempt"] = 1
                elif fault == "spoof_app":
                    client.values[github.API_ROOT + "/check-suites/30"]["app"]["id"] = 1
                else:
                    client.values[github.API_ROOT + "/check-suites/30"] = ReleaseError("API denied")
                with self.assertRaises(ReleaseError):
                    github._ci_identity(client, COMMIT)

    def test_bound_attempt_cannot_change_and_no_old_success_fallback(self):
        client = IdentityClient()
        changed = deepcopy(identity()["ci"])
        changed["run_attempt"] = 1
        with self.assertRaises(ReleaseError):
            github._ci_identity(client, COMMIT, changed)
        client.runs.append({"id": 11})
        newest = deepcopy(client.values[github.API_ROOT + "/actions/runs/10"])
        newest.update(id=11, conclusion="failure")
        client.values[github.API_ROOT + "/actions/runs/11"] = newest
        with self.assertRaises(ReleaseError):
            github._ci_identity(client, COMMIT)

    def test_malformed_api_objects_are_safe_refusals(self):
        for fault in ("run_list", "run_null", "repository_null", "app_list"):
            with self.subTest(fault=fault):
                client = IdentityClient()
                if fault == "run_list":
                    client.values[github.API_ROOT + "/actions/runs/10"] = []
                elif fault == "run_null":
                    client.values[github.API_ROOT + "/actions/runs/10"] = None
                elif fault == "repository_null":
                    client.values[github.API_ROOT + "/actions/runs/10"]["repository"] = None
                else:
                    client.values[github.API_ROOT + "/check-suites/30"]["app"] = []
                with self.assertRaises(ReleaseError):
                    github._ci_identity(client, COMMIT)

    def test_dispatch_inputs_refuse_noncanonical_or_wrong_origin(self):
        for field, value in [("RELEASE_VERSION", "0.5.17"), ("PLANNED_TAG", "v0.5.16"),
                             ("EXPECTED_COMMIT", "a" * 39), ("EXPECTED_COMMIT", "A" * 40),
                             ("GITHUB_EVENT_NAME", "push"), ("GITHUB_REF", "refs/tags/0.5.16"),
                             ("GITHUB_REPOSITORY", "other/repo"), ("GITHUB_WORKFLOW_SHA", "d" * 40),
                             ("GITHUB_WORKFLOW_REF", "untrusted"), ("RELEASE_MODE", "direct")]:
            with self.subTest(field=field, value=value):
                env = environment()
                env[field] = value
                with self.assertRaises(ReleaseError):
                    github._dispatch_values(env)

    def test_public_identity_annotated_tag_and_validate_only_absence(self):
        client = IdentityClient()
        self.assertEqual(github._public_identity(client, identity(), require_tag=True), TAG_OBJECT)
        del client.values[github.API_ROOT + "/git/ref/tags/" + github.TAG]
        self.assertIsNone(github._public_identity(client, identity(), require_tag=False))
        with self.assertRaises(ReleaseError):
            github._public_identity(client, identity(), require_tag=True)

    def test_main_tree_workflow_and_tag_conflicts_refuse(self):
        cases = [
            ("/git/ref/heads/main", lambda row: row["object"].update(sha="d" * 40)),
            ("/git/commits/" + COMMIT, lambda row: row["tree"].update(sha="d" * 40)),
            ("/contents/" + github.WORKFLOW_PATH + "?ref=" + COMMIT,
             lambda row: row.update(content=base64.b64encode(b"changed").decode())),
            ("/git/ref/tags/" + github.TAG, lambda row: row["object"].update(type="commit")),
            ("/git/tags/" + TAG_OBJECT, lambda row: row["object"].update(sha="d" * 40)),
        ]
        for suffix, mutation in cases:
            with self.subTest(suffix=suffix):
                client = IdentityClient()
                mutation(client.values[github.API_ROOT + suffix])
                with self.assertRaises(ReleaseError):
                    github._public_identity(client, identity(), require_tag=True)

    def test_actual_checkout_dispatch_and_recheck(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            (repo / ".github/workflows").mkdir(parents=True)
            (repo / github.WORKFLOW_PATH).write_bytes(WORKFLOW_BYTES)
            (repo / "pyproject.toml").write_text('[project]\nname="mcp-flightrecorder"\nversion="0.5.16"\n')
            def git(*args):
                return subprocess.check_output(["git", "-C", str(repo), *args], text=True, stderr=subprocess.DEVNULL).strip()
            git("init", "-q")
            git("add", ".")
            git("-c", "user.name=Package Tests", "-c", "user.email=tests@example.invalid", "commit", "-qm", "fixture")
            source = identity()
            source["commit"] = git("rev-parse", "HEAD")
            source["tree"] = git("rev-parse", "HEAD^{tree}")
            source["ci"]["head_sha"] = source["commit"]
            client = IdentityClient(source)
            env = environment(source["commit"])
            self.assertEqual(github.dispatch_identity(repo, env, client), source)
            github.recheck_identity(repo, source, env, client)
            env["GITHUB_RUN_ATTEMPT"] = "2"
            github.recheck_identity(repo, source, env, client)
            future = deepcopy(source)
            future["workflow"]["run_attempt"] = 3
            with self.assertRaises(ReleaseError):
                github.recheck_identity(repo, future, env, client)
            replacement = "e" * 40
            client.values[github.API_ROOT + "/git/ref/tags/" + github.TAG]["object"]["sha"] = replacement
            client.values[github.API_ROOT + "/git/tags/" + replacement] = {
                "sha": replacement, "tag": github.TAG, "object": {"type": "commit", "sha": source["commit"]}}
            with self.assertRaisesRegex(ReleaseError, "tag identity changed"):
                github.recheck_identity(repo, source, env, client)
            (repo / "unexpected.txt").write_text("dirty")
            with self.assertRaisesRegex(ReleaseError, "clean"):
                github.dispatch_identity(repo, env, client)


class PromotionClient(IdentityClient):
    def __init__(self):
        super().__init__()
        self.values[github.API_ROOT + "/actions/runs/20"] = {
            "id": 20, "run_attempt": 2, "path": github.WORKFLOW_PATH, "event": "workflow_dispatch",
            "head_sha": COMMIT, "head_branch": "main", "repository": {"id": 80, "full_name": github.REPOSITORY},
            "head_repository": {"id": 80, "full_name": github.REPOSITORY},
        }
        self.values[github.API_ROOT + "/actions/artifacts/60"] = {
            "id": 60, "expired": False, "name": "mcp-flightrecorder-0.5.16-20-1", "digest": "sha256:" + "d" * 64,
            "created_at": "2026-01-01T00:05:00Z",
            "workflow_run": {"id": 20, "head_sha": COMMIT, "head_branch": "main", "repository_id": 80,
                             "head_repository_id": 80},
        }
        self.producers = [{"name": "Build and validate one candidate", "run_id": 20, "run_attempt": 1,
                           "head_sha": COMMIT, "status": "completed", "conclusion": "success",
                           "started_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-01T00:10:00Z"}]

    def pages(self, path, key=None):
        if path == github.API_ROOT + "/actions/runs/20/attempts/1/jobs" and key == "jobs":
            return deepcopy(self.producers)
        return super().pages(path, key)


class PromotionArtifactTests(unittest.TestCase):
    def env(self):
        return {**environment(), "GITHUB_RUN_ATTEMPT": "2", "RELEASE_ARTIFACT_ID": "60",
                "RELEASE_ARTIFACT_DIGEST": "d" * 64}

    def test_attempt_two_accepts_exact_successful_attempt_one_artifact(self):
        client = PromotionClient()
        github.verify_promotion_artifact(identity(), self.env(), client)
        self.assertIn(github.API_ROOT + "/actions/artifacts/60", client.reads)

    def test_wrong_run_future_attempt_id_or_digest_refuses(self):
        for fault in ("wrong_run", "future_attempt", "missing_id", "wrong_id", "wrong_digest", "missing_digest"):
            with self.subTest(fault=fault):
                manifest, env, client = identity(), self.env(), PromotionClient()
                if fault == "wrong_run":
                    manifest["workflow"]["run_id"] = 21
                elif fault == "future_attempt":
                    manifest["workflow"]["run_attempt"] = 3
                elif fault == "missing_id":
                    del env["RELEASE_ARTIFACT_ID"]
                elif fault == "wrong_id":
                    client.values[github.API_ROOT + "/actions/artifacts/60"]["id"] = 61
                elif fault == "wrong_digest":
                    env["RELEASE_ARTIFACT_DIGEST"] = "e" * 64
                else:
                    del env["RELEASE_ARTIFACT_DIGEST"]
                with self.assertRaises(ReleaseError):
                    github.verify_promotion_artifact(manifest, env, client)

    def test_artifact_source_name_expiry_and_creation_time_are_bound(self):
        for fault in ("wrong_name", "expired", "other_run", "other_sha", "other_branch", "other_repo", "early", "late"):
            with self.subTest(fault=fault):
                client = PromotionClient()
                artifact = client.values[github.API_ROOT + "/actions/artifacts/60"]
                if fault == "wrong_name":
                    artifact["name"] = "mcp-flightrecorder-0.5.16-20-2"
                elif fault == "expired":
                    artifact["expired"] = True
                elif fault == "other_run":
                    artifact["workflow_run"]["id"] = 21
                elif fault == "other_sha":
                    artifact["workflow_run"]["head_sha"] = "f" * 40
                elif fault == "other_branch":
                    artifact["workflow_run"]["head_branch"] = "other"
                elif fault == "other_repo":
                    artifact["workflow_run"]["head_repository_id"] = 81
                else:
                    artifact["created_at"] = "2025-12-31T23:59:59Z" if fault == "early" else "2026-01-01T00:10:01Z"
                with self.assertRaises(ReleaseError):
                    github.verify_promotion_artifact(identity(), self.env(), client)

    def test_producer_must_be_unique_successful_correct_attempt(self):
        for fault in ("missing", "duplicate", "failure", "pending", "wrong_attempt", "wrong_sha", "no_time"):
            with self.subTest(fault=fault):
                client = PromotionClient()
                if fault == "missing":
                    client.producers = []
                elif fault == "duplicate":
                    client.producers *= 2
                elif fault == "failure":
                    client.producers[0]["conclusion"] = "failure"
                elif fault == "pending":
                    client.producers[0]["status"] = "in_progress"
                elif fault == "wrong_attempt":
                    client.producers[0]["run_attempt"] = 2
                elif fault == "wrong_sha":
                    client.producers[0]["head_sha"] = "f" * 40
                else:
                    client.producers[0]["completed_at"] = None
                with self.assertRaises(ReleaseError):
                    github.verify_promotion_artifact(identity(), self.env(), client)

    def test_current_release_run_identity_is_independent_of_manifest(self):
        for field, value in [("event", "push"), ("run_attempt", 1), ("path", github.CI_PATH),
                             ("head_sha", "f" * 40), ("head_branch", "other")]:
            with self.subTest(field=field):
                client = PromotionClient()
                client.values[github.API_ROOT + "/actions/runs/20"][field] = value
                with self.assertRaises(ReleaseError):
                    github.verify_promotion_artifact(identity(), self.env(), client)


class ReleaseClient:
    def __init__(self, notes, blobs, *, draft=None, present=()):
        self.release = None if draft is None else {
            "id": 40, "tag_name": github.TAG, "name": f"MCP Flight Recorder {github.VERSION}",
            "prerelease": False, "draft": draft, "body": notes,
        }
        self.assets = {name: {"id": index, "name": name, "state": "uploaded", "size": len(blobs[name]),
                              "bytes": blobs[name]}
                       for index, name in enumerate(present, start=50)}
        self.events = []

    def pages(self, path, key=None):
        if path == github.API_ROOT + "/releases":
            return [] if self.release is None else [deepcopy(self.release)]
        if path == github.API_ROOT + "/releases/40/assets":
            return [dict(asset) for asset in self.assets.values()]
        raise ReleaseError("Unexpected synthetic request")

    def download_asset(self, asset_id, expected_size):
        return next(asset["bytes"] for asset in self.assets.values() if asset["id"] == asset_id)

    def request(self, method, path, payload):
        self.events.append(method)
        if method == "POST" and path == github.API_ROOT + "/releases":
            self.release = {"id": 40, **deepcopy(payload)}
        elif method == "PATCH" and path == github.API_ROOT + "/releases/40":
            self.release.update(payload)
        else:
            raise ReleaseError("Unexpected synthetic mutation")

    def upload_asset(self, release_id, name, blob):
        self.events.append("UPLOAD:" + name)
        if name in self.assets:
            raise ReleaseError("Upload would overwrite")
        self.assets[name] = {"id": 50 + len(self.assets), "name": name, "state": "uploaded",
                             "size": len(blob), "bytes": blob}


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        dist = self.root / "dist"
        dist.mkdir()
        (dist / WHEEL).write_bytes(b"synthetic-wheel")
        (dist / SDIST).write_bytes(b"synthetic-sdist")
        self.bundle = self.root / "bundle"
        self.manifest = write_bundle(dist, self.bundle, identity(), {"python": "3.12.13"})
        self.blobs = {path.name: path.read_bytes() for path in self.bundle.iterdir()}
        self.notes = "Release notes.\n"
        self.env = patch.dict(os.environ, {"RELEASE_MODE": "staged-release"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def finalize(self, client, recheck=None):
        def identity_check():
            client.events.append("RECHECK")
        return github.finalize_release(self.bundle, self.manifest, self.notes, recheck or identity_check, client)

    def assert_guarded_mutations(self, events):
        for index, event in enumerate(events):
            if event != "RECHECK":
                self.assertGreater(index, 0)
                self.assertEqual(events[index - 1], "RECHECK")

    def test_absent_creates_only_draft_uploads_exact_assets_and_publishes_last(self):
        client = ReleaseClient(self.notes, self.blobs)
        result = self.finalize(client)
        self.assertTrue(result["created_draft"])
        self.assertTrue(result["published_draft"])
        self.assertEqual(set(result["uploaded"]), set(self.blobs))
        self.assertEqual(client.events[-1], "PATCH")
        self.assertFalse(client.release["draft"])
        self.assert_guarded_mutations(client.events)

    def test_complete_published_is_idempotent_without_mutation(self):
        client = ReleaseClient(self.notes.removesuffix("\n"), self.blobs, draft=False, present=self.blobs)
        result = self.finalize(client)
        self.assertEqual(result["uploaded"], [])
        self.assertFalse(result["created_draft"] or result["published_draft"])
        self.assertEqual(client.events, ["RECHECK"])

    def test_matching_partial_draft_and_published_only_upload_missing(self):
        for draft in (True, False):
            with self.subTest(draft=draft):
                client = ReleaseClient(self.notes, self.blobs, draft=draft, present=[WHEEL])
                result = self.finalize(client)
                self.assertEqual(set(result["uploaded"]), set(self.blobs) - {WHEEL})
                self.assertEqual(result["published_draft"], draft)
                self.assertNotIn("POST", client.events)
                self.assert_guarded_mutations(client.events)

    def test_conflicting_existing_metadata_or_asset_never_mutates(self):
        for fault in ("notes", "title", "prerelease", "bytes", "extra", "size", "pending"):
            with self.subTest(fault=fault):
                client = ReleaseClient(self.notes, self.blobs, draft=True, present=[WHEEL])
                if fault == "notes":
                    client.release["body"] = "conflicting notes"
                elif fault == "title":
                    client.release["name"] = "different"
                elif fault == "prerelease":
                    client.release["prerelease"] = True
                elif fault == "bytes":
                    client.assets[WHEEL]["bytes"] = b"conflicting"
                elif fault == "extra":
                    client.assets["extra.txt"] = {"name": "extra.txt", "state": "uploaded"}
                elif fault == "size":
                    client.assets[WHEEL]["size"] += 1
                else:
                    client.assets[WHEEL]["state"] = "starter"
                with self.assertRaises(ReleaseError):
                    self.finalize(client)
                self.assertEqual(client.events, ["RECHECK"])

    def test_bundle_tampering_and_validate_only_cannot_mutate(self):
        client = ReleaseClient(self.notes, self.blobs)
        with patch.dict(os.environ, {"RELEASE_MODE": "validate-only"}):
            with self.assertRaises(ReleaseError):
                self.finalize(client)
        (self.bundle / WHEEL).write_bytes(b"tampered")
        with self.assertRaises(ReleaseError):
            self.finalize(client)
        self.assertEqual(client.events, [])

    def test_identity_change_after_approval_prevents_next_mutation(self):
        client = ReleaseClient(self.notes, self.blobs)
        calls = 0
        def changed():
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ReleaseError("Main advanced during approval")
        with self.assertRaises(ReleaseError):
            self.finalize(client, changed)
        self.assertEqual(client.events, [])

    def test_partial_upload_failure_is_preserved_and_retry_resumes(self):
        client = ReleaseClient(self.notes, self.blobs)
        real_upload = client.upload_asset
        calls = 0
        def interrupted(release_id, name, blob):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ReleaseError("Synthetic transport failure")
            return real_upload(release_id, name, blob)
        with patch.object(client, "upload_asset", side_effect=interrupted):
            with self.assertRaises(ReleaseError):
                self.finalize(client)
        self.assertEqual(len(client.assets), 1)
        self.assertTrue(client.release["draft"])
        retained = set(client.assets)
        result = self.finalize(client)
        self.assertEqual(set(result["uploaded"]), set(self.blobs) - retained)
        self.assertFalse(client.release["draft"])

    def test_duplicate_existing_asset_names_refuse_before_write(self):
        client = ReleaseClient(self.notes, self.blobs, draft=True, present=[WHEEL])
        real_pages = client.pages
        def duplicated(path, key=None):
            rows = real_pages(path, key)
            return rows + rows if path.endswith("/assets") else rows
        with patch.object(client, "pages", side_effect=duplicated):
            with self.assertRaises(ReleaseError):
                self.finalize(client)
        self.assertEqual(client.events, ["RECHECK"])


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class TransportTests(unittest.TestCase):
    def test_redirected_download_never_forwards_authorization(self):
        class Opener:
            requests = []
            def open(self, request, timeout):
                self.requests.append(request)
                if len(self.requests) == 1:
                    raise HTTPError(request.full_url, 302, "redirect", {
                        "Location": "https://release-assets.githubusercontent.com/asset?opaque=fixture"}, None)
                return Response(b"data")
        opener = Opener()
        client = github.GitHubClient(token="synthetic-credential", opener=opener)
        self.assertEqual(client.download_asset(1, 4), b"data")
        self.assertIn("Authorization", opener.requests[0].headers)
        self.assertNotIn("Authorization", opener.requests[1].headers)

    def test_unsafe_redirect_oversize_and_non404_are_not_absence(self):
        for fault in ("redirect", "oversize", "403", "429", "500", "invalid-json", "duplicate-json"):
            with self.subTest(fault=fault):
                class Opener:
                    def open(self, request, timeout):
                        if fault == "redirect":
                            raise HTTPError(request.full_url, 302, "redirect", {"Location": "https://example.invalid/file"}, None)
                        if fault in {"403", "429", "500"}:
                            raise HTTPError(request.full_url, int(fault), "failure", {}, None)
                        if fault == "duplicate-json":
                            return Response(b'{"id": 1, "id": 2}')
                        return Response(b"oversized" if fault == "oversize" else b"not-json")
                client = github.GitHubClient(token="synthetic-credential", opener=Opener())
                with self.assertRaises(ReleaseError) as caught:
                    if fault in {"redirect", "oversize"}:
                        client.download_asset(1, 4)
                    else:
                        client.get_json(github.API_ROOT, allow_404=True)
                self.assertNotIn("synthetic-credential", str(caught.exception))

    def test_only_explicit_404_can_mean_absence(self):
        class Opener:
            def open(self, request, timeout):
                raise HTTPError(request.full_url, 404, "missing", {}, None)
        client = github.GitHubClient(token="synthetic-credential", opener=Opener())
        self.assertIsNone(client.get_json(github.API_ROOT, allow_404=True))
        with self.assertRaises(ReleaseError):
            client.get_json(github.API_ROOT)

    def test_upload_host_is_exact_and_uses_scoped_token_without_redirect(self):
        class Opener:
            request = None
            def open(self, request, timeout):
                self.request = request
                return Response(b'{"id": 1}')
        opener = Opener()
        client = github.GitHubClient(token="synthetic-credential", opener=opener)
        client.upload_asset(40, WHEEL, b"data")
        self.assertTrue(opener.request.full_url.startswith("https://uploads.github.com" + github.API_ROOT + "/releases/40/assets?"))
        self.assertEqual(opener.request.method, "POST")
        self.assertIn("Authorization", opener.request.headers)
        with self.assertRaises(ReleaseError):
            github.GitHubClient(token="", opener=opener).upload_asset(40, WHEEL, b"data")

    def test_paginated_jobs_are_complete_and_count_changes_fail(self):
        client = github.GitHubClient(token="")
        rows = [{"id": number} for number in range(101)]
        with patch.object(client, "get_json", side_effect=[{"total_count": 101, "jobs": rows[:100]},
                                                          {"total_count": 101, "jobs": rows[100:]}]):
            self.assertEqual(client.pages(github.API_ROOT + "/actions/runs/1/attempts/1/jobs", "jobs"), rows)
        with patch.object(client, "get_json", side_effect=[{"total_count": 101, "jobs": rows[:100]},
                                                          {"total_count": 102, "jobs": rows[100:]}]):
            with self.assertRaises(ReleaseError):
                client.pages(github.API_ROOT + "/actions/runs/1/attempts/1/jobs", "jobs")


if __name__ == "__main__":
    unittest.main()
