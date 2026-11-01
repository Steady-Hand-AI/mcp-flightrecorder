"""Plan missing registry files and anonymously verify exact published bytes."""
from __future__ import annotations

import hashlib
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from release_artifacts import ReleaseError, VERSION, require, strict_json, verify_bundle

REGISTRIES = {
    "pypi": ("pypi.org", "files.pythonhosted.org"),
    "testpypi": ("test.pypi.org", "test-files.pythonhosted.org"),
}
MAX_RESPONSE_BYTES = 1024 * 1024


def _url(url, hosts):
    require(type(url) is str, "invalid registry URL")
    try:
        parsed = urlsplit(url)
        valid = (parsed.scheme == "https" and parsed.hostname in hosts and parsed.port in (None, 443)
                 and parsed.username is None and parsed.password is None and not parsed.fragment
                 and not parsed.query)
    except ValueError:
        valid = False
    require(valid, "registry URL outside the allowed HTTPS origin")
    return parsed


def anonymous_get(url: str, max_bytes: int) -> tuple[int, bytes]:
    """Anonymous HTTPS only; redirects retain the original approved host."""
    all_hosts = {host for hosts in REGISTRIES.values() for host in hosts}
    parsed = _url(url, all_hosts)

    class SameOriginRedirect(HTTPRedirectHandler):
        def redirect_request(self, request, response, code, message, headers, newurl):
            _url(newurl, {parsed.hostname})
            return super().redirect_request(request, response, code, message, headers, newurl)

    request = Request(url, headers={"User-Agent": "mcp-flightrecorder-release-verifier", "Accept": "*/*"})
    try:
        with build_opener(SameOriginRedirect()).open(request, timeout=30) as response:
            length = response.headers.get("Content-Length")
            if length is not None:
                require(length.isdigit() and int(length) <= max_bytes, "registry response exceeds size bound")
            data = response.read(max_bytes + 1)
            require(len(data) <= max_bytes, "registry response exceeds size bound")
            return response.status, data
    except HTTPError as error:
        # Error response bodies are neither needed nor safe to echo.
        error.close()
        return error.code, b""
    except (URLError, TimeoutError, OSError):
        raise ReleaseError("anonymous registry request failed") from None


def _get(transport, url, bound):
    try:
        status, data = transport(url, bound)
    except ReleaseError:
        raise
    except Exception:
        raise ReleaseError("anonymous registry request failed") from None
    require(type(status) is int and type(data) is bytes and len(data) <= bound,
            "invalid or oversized registry response")
    return status, data


def _state(manifest, registry, transport):
    require(type(registry) is str and registry in REGISTRIES, "unknown registry")
    api_host, file_host = REGISTRIES[registry]
    endpoint = f"https://{api_host}/pypi/mcp-flightrecorder/{VERSION}/json"
    status, body = _get(transport, endpoint, MAX_RESPONSE_BYTES)
    if status == 404:
        return {}
    require(status == 200, "registry version lookup did not succeed")
    value = strict_json(body)
    require(type(value) is dict and type(value.get("info")) is dict
            and type(value["info"].get("name")) is str
            and value["info"]["name"].lower().replace("_", "-") == "mcp-flightrecorder"
            and value["info"].get("version") == VERSION, "incorrect registry project/version")
    rows = value.get("urls")
    require(type(rows) is list and 0 < len(rows) <= 2, "invalid registry file inventory")
    expected = {artifact["filename"]: artifact for artifact in manifest["artifacts"]}
    found = {}
    for row in rows:
        require(type(row) is dict, "invalid registry file record")
        name = row.get("filename")
        require(type(name) is str and name in expected and name not in found, "unexpected or duplicate registry file")
        artifact = expected[name]
        require(type(row.get("size")) is int and row["size"] == artifact["size"]
                and type(row.get("digests")) is dict and row["digests"].get("sha256") == artifact["sha256"]
                and row.get("yanked", False) is False, "conflicting registry file identity")
        url = row.get("url")
        parsed = _url(url, {file_host})
        require(parsed.path.startswith("/packages/") and unquote(parsed.path.rsplit("/", 1)[-1]) == name,
                "incorrect registry download path")
        download_status, data = _get(transport, url, artifact["size"])
        require(download_status == 200, "registry file download did not succeed")
        require(len(data) == artifact["size"] and hashlib.sha256(data).hexdigest() == artifact["sha256"],
                "downloaded registry bytes differ from approved artifact")
        found[name] = data
    return found


def registry_plan(bundle: Path, registry: str, upload_dir: Path, expected_digest: str, *, transport=None) -> list[str]:
    """Prove existing files, then stage only absent approved files for the publisher."""
    require(expected_digest is not None, "expected manifest digest is required")
    manifest = verify_bundle(bundle, expected_digest)
    found = _state(manifest, registry, transport or anonymous_get)
    missing = sorted(artifact["filename"] for artifact in manifest["artifacts"] if artifact["filename"] not in found)
    upload_dir = Path(upload_dir)
    require(not upload_dir.exists(), "upload directory already exists")
    expected = {artifact["filename"]: artifact for artifact in manifest["artifacts"]}
    staged = {}
    for name in missing:
        artifact = expected[name]
        with (Path(bundle) / name).open("rb") as source:
            data = source.read(artifact["size"] + 1)
        require(len(data) == artifact["size"] and hashlib.sha256(data).hexdigest() == artifact["sha256"],
                "staged distribution differs from approved artifact")
        staged[name] = data
    upload_dir.mkdir(parents=True)
    for name, data in staged.items():
        (upload_dir / name).write_bytes(data)
    return missing


def registry_verify(bundle: Path, registry: str, destination: Path, expected_digest: str, *,
                    transport=None, attempts=6, delay=10, sleep=time.sleep) -> Path:
    """Poll only absent/correct partial state; conflicts and access errors fail immediately."""
    require(expected_digest is not None, "expected manifest digest is required")
    require(type(attempts) is int and 1 <= attempts <= 12 and type(delay) in (int, float)
            and 0 <= delay <= 30, "invalid propagation retry bound")
    manifest = verify_bundle(bundle, expected_digest)
    destination = Path(destination)
    require(not destination.exists(), "verified destination already exists")
    expected = {artifact["filename"] for artifact in manifest["artifacts"]}
    for attempt in range(attempts):
        found = _state(manifest, registry, transport or anonymous_get)
        if set(found) == expected:
            destination.mkdir(parents=True)
            for name, data in found.items():
                (destination / name).write_bytes(data)
            return destination
        if attempt + 1 < attempts:
            sleep(delay)
    raise ReleaseError(f"registry propagation incomplete; verified {len(found)} of 2 files; remote state unchanged")
