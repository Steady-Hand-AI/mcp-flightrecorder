"""Exact GitHub source/CI identity and resumable release finalization."""

import base64
import binascii
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tomllib
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from release_artifacts import (
    ReleaseError, REPOSITORY, VERSION, TAG, WORKFLOW_PATH, require, strict_json, verify_bundle,
)

CI_WORKFLOW_ID = 403832745
CI_PATH = ".github/workflows/ci.yml"
CI_JOBS = {"core / Python 3.11", "core / Python 3.14", "integration / Python 3.14"}
API_ROOT = f"/repos/{REPOSITORY}"
SHA = re.compile(r"[0-9a-f]{40}")
MAX_JSON = 2 * 1024 * 1024
MAX_ASSET = 64 * 1024 * 1024


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GitHubClient:
    """Bounded transport; credentials never follow a redirect."""

    def __init__(self, token=None, opener=None):
        self.token = os.environ.get("GH_TOKEN", "") if token is None else token
        self.opener = opener or build_opener(_NoRedirect())

    def _read(self, method, url, *, body=None, content_type=None, binary=False,
              limit=MAX_JSON, allow_404=False, authenticate=True, redirects=0):
        parsed = urlsplit(url)
        allowed_api = parsed.netloc == "api.github.com" and (parsed.path == API_ROOT or parsed.path.startswith(API_ROOT + "/"))
        allowed_upload = (parsed.netloc == "uploads.github.com" and method == "POST"
                          and re.fullmatch(re.escape(API_ROOT) + r"/releases/[1-9][0-9]*/assets", parsed.path))
        allowed_download = (method == "GET" and not authenticate and parsed.netloc in {
            "release-assets.githubusercontent.com", "objects.githubusercontent.com",
        })
        require(parsed.scheme == "https" and not parsed.fragment and
                (allowed_api or allowed_upload or allowed_download), "Unexpected GitHub endpoint")
        headers = {"Accept": "application/octet-stream" if binary else "application/vnd.github+json",
                   "User-Agent": "mcp-flightrecorder-release", "X-GitHub-Api-Version": "2022-11-28"}
        if authenticate and self.token:
            headers["Authorization"] = "Bearer " + self.token
        if method != "GET":
            require(bool(self.token), "GitHub mutation requires the scoped job token")
        if content_type:
            headers["Content-Type"] = content_type
        try:
            with self.opener.open(Request(url, data=body, headers=headers, method=method), timeout=30) as response:
                blob = response.read(limit + 1)
        except HTTPError as error:
            status = error.code
            location = error.headers.get("Location", "")
            error.close()
            if status == 404 and allow_404:
                return None
            if binary and method == "GET" and status in (301, 302, 303, 307, 308):
                require(redirects < 2, "Too many asset redirects")
                return self._read("GET", location, binary=True,
                                  limit=limit, authenticate=False, redirects=redirects + 1)
            raise ReleaseError(f"GitHub request failed with HTTP {status}") from None
        except (URLError, TimeoutError, OSError):
            raise ReleaseError("GitHub request failed before a verified response") from None
        require(len(blob) <= limit, "GitHub response exceeds the allowed size")
        return blob

    def request(self, method, path, payload=None, *, allow_404=False):
        require(path == API_ROOT or path.startswith(API_ROOT + "/"), "Unexpected GitHub API path")
        body = None if payload is None else json.dumps(payload).encode()
        blob = self._read(method, "https://api.github.com" + path, body=body,
                          content_type="application/json" if body is not None else None,
                          allow_404=allow_404)
        if blob is None:
            return None
        try:
            return strict_json(blob)
        except (UnicodeError, ValueError):
            raise ReleaseError("GitHub returned invalid JSON") from None

    def get_json(self, path, *, allow_404=False):
        return self.request("GET", path, allow_404=allow_404)

    def pages(self, path, key=None):
        rows = []
        total = None
        for page in range(1, 21):
            separator = "&" if "?" in path else "?"
            result = self.get_json(path + separator + urlencode({"per_page": 100, "page": page}))
            if key is None:
                batch = result
            else:
                require(isinstance(result, dict), "GitHub collection is not an object")
                count = result.get("total_count")
                require(type(count) is int and 0 <= count <= 2000, "GitHub collection count is invalid")
                require(total is None or total == count, "GitHub collection changed during pagination")
                total = count
                batch = result.get(key)
            require(isinstance(batch, list) and len(batch) <= 100, "GitHub collection page is invalid")
            rows.extend(batch)
            if len(batch) < 100 or (total is not None and len(rows) >= total):
                require(total is None or len(rows) == total, "GitHub collection is incomplete")
                return rows
        raise ReleaseError("GitHub collection exceeds the page bound")

    def download_asset(self, asset_id, expected_size):
        _positive_id(asset_id)
        require(type(expected_size) is int and 0 < expected_size <= MAX_ASSET, "Invalid asset size")
        blob = self._read("GET", f"https://api.github.com{API_ROOT}/releases/assets/{asset_id}",
                          binary=True, limit=expected_size)
        require(len(blob) == expected_size, "Downloaded asset size differs")
        return blob

    def upload_asset(self, release_id, name, blob):
        _positive_id(release_id)
        require(0 < len(blob) <= MAX_ASSET, "Invalid upload size")
        url = f"https://uploads.github.com{API_ROOT}/releases/{release_id}/assets?" + urlencode({"name": name})
        response = self._read("POST", url, body=blob, content_type="application/octet-stream")
        try:
            return strict_json(response)
        except (UnicodeError, ValueError):
            raise ReleaseError("GitHub upload returned invalid JSON") from None


def _positive_id(value):
    require(type(value) is int and value > 0, "Invalid GitHub identity number")
    return value


def _object(value):
    require(isinstance(value, dict), "GitHub returned an invalid object")
    return value


def _get(client, path, *, allow_404=False):
    value = client.get_json(path, allow_404=allow_404)
    return None if value is None and allow_404 else _object(value)


def _env_number(env, name):
    value = env.get(name, "")
    require(isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value), "Invalid workflow run identity")
    return int(value)


def _git(repo, *args):
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)
    require(result.returncode == 0, "Cannot verify local Git identity")
    return result.stdout.strip()


def _dispatch_values(env):
    require(env.get("RELEASE_MODE") in {"validate-only", "staged-release"}, "Invalid release mode")
    require(env.get("RELEASE_VERSION") == VERSION and env.get("PLANNED_TAG") == TAG, "Invalid version or tag")
    commit = env.get("EXPECTED_COMMIT", "")
    require(isinstance(commit, str) and SHA.fullmatch(commit), "Expected commit must be a full lowercase SHA")
    require(env.get("GITHUB_EVENT_NAME") == "workflow_dispatch" and env.get("GITHUB_REF") == "refs/heads/main",
            "Release dispatch must originate from main")
    require(env.get("GITHUB_REPOSITORY") == REPOSITORY, "Wrong release repository")
    require(env.get("GITHUB_SHA") == commit and env.get("GITHUB_WORKFLOW_SHA") == commit,
            "Run and workflow commits must equal the approved commit")
    require(env.get("GITHUB_WORKFLOW_REF") == f"{REPOSITORY}/{WORKFLOW_PATH}@refs/heads/main",
            "Wrong release workflow reference")
    return commit


def _public_identity(client, identity, *, require_tag):
    commit = identity["commit"]
    repository = _get(client, API_ROOT)
    require(repository.get("full_name") == REPOSITORY and repository.get("default_branch") == "main",
            "Public repository identity differs")
    branch = _get(client, API_ROOT + "/git/ref/heads/main")
    require(branch.get("ref") == "refs/heads/main" and _object(branch.get("object")).get("type") == "commit" and
            branch["object"].get("sha") == commit, "Main no longer points to the approved commit")
    public_commit = _get(client, API_ROOT + "/git/commits/" + commit)
    require(public_commit.get("sha") == commit and _object(public_commit.get("tree")).get("sha") == identity["tree"],
            "Public commit tree differs")
    workflow = _get(client, API_ROOT + "/contents/" + WORKFLOW_PATH + "?ref=" + commit)
    require(workflow.get("type") == "file" and workflow.get("path") == WORKFLOW_PATH and
            workflow.get("encoding") == "base64" and isinstance(workflow.get("content"), str),
            "Public workflow response is invalid")
    try:
        content = base64.b64decode(workflow["content"].replace("\n", ""), validate=True)
    except (binascii.Error, ValueError):
        raise ReleaseError("Public workflow content is invalid") from None
    require(content and hashlib.sha256(content).hexdigest() == identity["workflow"]["sha256"],
            "Public workflow bytes differ")
    ref = _get(client, API_ROOT + "/git/ref/tags/" + TAG, allow_404=True)
    if ref is None:
        require(not require_tag, "The approved annotated tag is missing")
        return None
    require(ref.get("ref") == "refs/tags/" + TAG and _object(ref.get("object")).get("type") == "tag",
            "Release tag must be annotated")
    tag_id = ref["object"].get("sha", "")
    require(isinstance(tag_id, str) and SHA.fullmatch(tag_id), "Invalid annotated tag identity")
    tag = _get(client, API_ROOT + "/git/tags/" + tag_id)
    require(tag.get("sha") == tag_id and tag.get("tag") == TAG and
            _object(tag.get("object")).get("type") == "commit" and tag["object"].get("sha") == commit,
            "Annotated tag does not point to the approved commit")
    return tag_id


def _ci_identity(client, commit, bound=None):
    workflow = _get(client, API_ROOT + f"/actions/workflows/{CI_WORKFLOW_ID}")
    require(workflow.get("id") == CI_WORKFLOW_ID and workflow.get("path") == CI_PATH,
            "Wrong authoritative CI workflow")
    if bound is None:
        runs = client.pages(API_ROOT + f"/actions/workflows/{CI_WORKFLOW_ID}/runs?" +
                            urlencode({"branch": "main", "event": "push", "head_sha": commit}), "workflow_runs")
        require(runs and all(isinstance(run, dict) for run in runs), "No authoritative CI run")
        run_id = max(_positive_id(run.get("id")) for run in runs)
    else:
        require(bound.get("workflow_id") == CI_WORKFLOW_ID and bound.get("head_sha") == commit,
                "CI binding differs")
        run_id = _positive_id(bound.get("run_id"))
    run = _get(client, API_ROOT + f"/actions/runs/{run_id}")
    require(run.get("id") == run_id and run.get("workflow_id") == CI_WORKFLOW_ID and run.get("path") == CI_PATH,
            "CI run belongs to another workflow")
    require(_object(run.get("repository")).get("full_name") == REPOSITORY and
            _object(run.get("head_repository")).get("full_name") == REPOSITORY,
            "CI run belongs to another repository")
    require(run.get("head_sha") == commit and run.get("head_branch") == "main" and run.get("event") == "push",
            "CI run does not prove this main push")
    require(run.get("status") == "completed" and run.get("conclusion") == "success", "CI run has not succeeded")
    attempt = _positive_id(run.get("run_attempt"))
    require(bound is None or bound.get("run_attempt") == attempt, "CI attempt changed after validation")
    suite_id = _positive_id(run.get("check_suite_id"))
    suite = _get(client, API_ROOT + f"/check-suites/{suite_id}")
    require(suite.get("id") == suite_id and _object(suite.get("app")).get("id") == 15368 and
            suite.get("head_sha") == commit and suite.get("status") == "completed" and
            suite.get("conclusion") == "success", "CI check suite identity is unverified")
    jobs = client.pages(API_ROOT + f"/actions/runs/{run_id}/attempts/{attempt}/jobs", "jobs")
    require(len(jobs) == len(CI_JOBS) and all(isinstance(job, dict) for job in jobs) and
            {job.get("name") for job in jobs} == CI_JOBS, "Required CI jobs are missing or duplicated")
    for job in jobs:
        require(job.get("run_id") == run_id and job.get("run_attempt") == attempt and job.get("head_sha") == commit and
                job.get("status") == "completed" and job.get("conclusion") == "success",
                "A required CI job has not succeeded for this commit")
    return {"workflow_id": CI_WORKFLOW_ID, "run_id": run_id, "run_attempt": attempt, "head_sha": commit}


def dispatch_identity(repo: Path, env, client=None):
    client = client or GitHubClient(token=env.get("GH_TOKEN", ""))
    commit = _dispatch_values(env)
    require(_git(repo, "rev-parse", "HEAD") == commit and not _git(repo, "status", "--porcelain"),
            "Checkout must be clean at the approved commit")
    project = tomllib.loads((repo / "pyproject.toml").read_text())["project"]
    require(project.get("name") == "mcp-flightrecorder" and project.get("version") == VERSION,
            "Source package identity differs")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    require(SHA.fullmatch(tree), "Invalid source tree identity")
    identity = {"repository": REPOSITORY, "version": VERSION, "tag": TAG, "commit": commit, "tree": tree,
                "workflow": {"path": WORKFLOW_PATH, "sha256": hashlib.sha256((repo / WORKFLOW_PATH).read_bytes()).hexdigest(),
                             "run_id": _env_number(env, "GITHUB_RUN_ID"),
                             "run_attempt": _env_number(env, "GITHUB_RUN_ATTEMPT")}}
    identity["tag_object"] = _public_identity(client, identity, require_tag=env["RELEASE_MODE"] == "staged-release")
    identity["ci"] = _ci_identity(client, commit)
    return identity


def recheck_identity(repo: Path, manifest: dict, env, client=None):
    client = client or GitHubClient(token=env.get("GH_TOKEN", ""))
    commit = _dispatch_values(env)
    require(manifest.get("repository") == REPOSITORY and manifest.get("version") == VERSION and
            manifest.get("tag") == TAG and manifest.get("commit") == commit, "Manifest release identity differs")
    require(_git(repo, "rev-parse", "HEAD") == commit and _git(repo, "rev-parse", "HEAD^{tree}") == manifest.get("tree") and
            not _git(repo, "status", "--porcelain"),
            "Checkout identity differs")
    workflow = manifest.get("workflow", {})
    require(workflow.get("path") == WORKFLOW_PATH and workflow.get("run_id") == _env_number(env, "GITHUB_RUN_ID") and
            _positive_id(workflow.get("run_attempt")) <= _env_number(env, "GITHUB_RUN_ATTEMPT") and
            workflow.get("sha256") == hashlib.sha256((repo / WORKFLOW_PATH).read_bytes()).hexdigest(),
            "Manifest workflow identity differs")
    tag_id = _public_identity(client, manifest, require_tag=env["RELEASE_MODE"] == "staged-release")
    require(tag_id == manifest.get("tag_object"), "Annotated tag identity changed")
    require(_ci_identity(client, commit, manifest.get("ci", {})) == manifest["ci"], "CI identity changed")


def _timestamp(value):
    require(isinstance(value, str), "Missing artifact provenance time")
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ReleaseError("Invalid artifact provenance time") from None
    require(timestamp.tzinfo is not None, "Artifact provenance time must have a timezone")
    return timestamp


def verify_promotion_artifact(manifest: dict, env, client=None):
    """Bind a downloaded candidate to its successful producing job, including reruns."""
    client = client or GitHubClient(token=env.get("GH_TOKEN", ""))
    commit = _dispatch_values(env)
    workflow = manifest.get("workflow", {})
    run_id = _env_number(env, "GITHUB_RUN_ID")
    attempt = _env_number(env, "GITHUB_RUN_ATTEMPT")
    producing_attempt = _positive_id(workflow.get("run_attempt"))
    require(manifest.get("commit") == commit and workflow.get("run_id") == run_id and
            producing_attempt <= attempt, "Candidate belongs to another or future workflow attempt")
    artifact_id = _env_number(env, "RELEASE_ARTIFACT_ID")
    digest = env.get("RELEASE_ARTIFACT_DIGEST", "")
    require(isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest), "Invalid trusted artifact digest")
    run = _get(client, API_ROOT + f"/actions/runs/{run_id}")
    require(run.get("id") == run_id and run.get("run_attempt") == attempt and
            run.get("path") == WORKFLOW_PATH and run.get("event") == "workflow_dispatch" and
            run.get("head_sha") == commit and run.get("head_branch") == "main" and
            _object(run.get("repository")).get("full_name") == REPOSITORY and
            _object(run.get("head_repository")).get("full_name") == REPOSITORY,
            "Current release workflow identity differs")
    repository_id = _positive_id(run["repository"].get("id"))
    require(run["head_repository"].get("id") == repository_id, "Release run head repository differs")
    artifact = _get(client, API_ROOT + f"/actions/artifacts/{artifact_id}")
    require(artifact.get("id") == artifact_id and artifact.get("expired") is False and
            artifact.get("name") == f"mcp-flightrecorder-{VERSION}-{run_id}-{producing_attempt}" and
            artifact.get("digest") == "sha256:" + digest, "Candidate artifact identity or digest differs")
    artifact_run = _object(artifact.get("workflow_run"))
    require(artifact_run.get("id") == run_id and artifact_run.get("head_sha") == commit and
            artifact_run.get("head_branch") == "main" and artifact_run.get("repository_id") == repository_id and
            artifact_run.get("head_repository_id") == repository_id, "Candidate artifact workflow binding differs")
    jobs = client.pages(API_ROOT + f"/actions/runs/{run_id}/attempts/{producing_attempt}/jobs", "jobs")
    require(all(isinstance(job, dict) for job in jobs), "Invalid producing job listing")
    producers = [job for job in jobs if job.get("name") == "Build and validate one candidate"]
    require(len(producers) == 1, "Candidate has no unique producing job")
    producer = producers[0]
    require(producer.get("run_id") == run_id and producer.get("run_attempt") == producing_attempt and
            producer.get("head_sha") == commit and producer.get("status") == "completed" and
            producer.get("conclusion") == "success", "Candidate producing job did not succeed")
    require(_timestamp(producer.get("started_at")) <= _timestamp(artifact.get("created_at")) <=
            _timestamp(producer.get("completed_at")), "Candidate artifact was not created during its producing job")


def _release_state(client, notes, blobs):
    releases = client.pages(API_ROOT + "/releases")
    require(all(isinstance(release, dict) for release in releases), "Invalid release listing")
    matching = [release for release in releases if isinstance(release, dict) and release.get("tag_name") == TAG]
    require(len(matching) <= 1, "Multiple releases use the planned tag")
    if not matching:
        return None, set()
    release = matching[0]
    _positive_id(release.get("id"))
    require(release.get("name") == f"MCP Flight Recorder {VERSION}" and release.get("prerelease") is False and
            type(release.get("draft")) is bool and isinstance(release.get("body"), str) and
            release["body"] in {notes, notes.removesuffix("\n")},
            "Existing release metadata conflicts")
    assets = client.pages(API_ROOT + f"/releases/{release['id']}/assets")
    seen = set()
    for asset in assets:
        require(isinstance(asset, dict), "Invalid existing asset")
        name = asset.get("name")
        require(isinstance(name, str) and name in blobs and name not in seen and asset.get("state") == "uploaded",
                "Existing asset inventory conflicts")
        seen.add(name)
        require(type(asset.get("size")) is int and asset["size"] == len(blobs[name]), "Existing asset size conflicts")
        require(client.download_asset(asset.get("id"), asset["size"]) == blobs[name], "Existing asset bytes conflict")
    return release, seen


def finalize_release(bundle: Path, manifest: dict, notes: str, recheck, client=None):
    """Resume matching state; never delete or replace an existing public object."""
    client = client or GitHubClient()
    require(os.environ.get("RELEASE_MODE") == "staged-release", "GitHub finalization requires staged-release mode")
    require(isinstance(notes, str) and notes.strip(), "Curated release notes are required")
    require(verify_bundle(bundle) == manifest, "Bundle manifest changed")
    blobs = {path.name: path.read_bytes() for path in bundle.iterdir()}
    bundle_hashes = {name: hashlib.sha256(blob).hexdigest() for name, blob in blobs.items()}

    def before_mutation():
        require(verify_bundle(bundle) == manifest and
                {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in bundle.iterdir()} == bundle_hashes,
                "Bundle changed before mutation")
        recheck()

    recheck()
    release, present = _release_state(client, notes, blobs)
    created = release is None
    uploaded = []
    published = False
    if release is None:
        before_mutation()
        client.request("POST", API_ROOT + "/releases", {
            "tag_name": TAG, "target_commitish": manifest["commit"], "name": f"MCP Flight Recorder {VERSION}",
            "body": notes, "draft": True, "prerelease": False,
        })
        release, present = _release_state(client, notes, blobs)
        require(release is not None and release["draft"], "New release draft is unverified")
    for name in sorted(set(blobs) - present):
        before_mutation()
        client.upload_asset(release["id"], name, blobs[name])
        uploaded.append(name)
    release, present = _release_state(client, notes, blobs)
    require(release is not None and present == set(blobs), "Release remains incomplete")
    if release["draft"]:
        before_mutation()
        client.request("PATCH", API_ROOT + f"/releases/{release['id']}", {"draft": False})
        published = True
    release, present = _release_state(client, notes, blobs)
    require(release is not None and release["draft"] is False and present == set(blobs), "Final release state is unverified")
    recheck()
    return {"result": "PASS", "created_draft": created, "uploaded": uploaded, "published_draft": published,
            "release_id": release["id"]}
