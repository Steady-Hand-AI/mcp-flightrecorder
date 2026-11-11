"""Validate the immutable four-file release bundle and rebuilt wheel payloads."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import shutil
import zipfile

import check_package

REPOSITORY = "Steady-Hand-AI/mcp-flightrecorder"
VERSION = TAG = "0.5.23"
WORKFLOW_PATH = ".github/workflows/publish.yml"
WHEEL = f"mcp_flightrecorder-{VERSION}-py3-none-any.whl"
SDIST = f"mcp_flightrecorder-{VERSION}.tar.gz"
PAIR = frozenset({WHEEL, SDIST})
BUNDLE_FILES = PAIR | {"manifest.json", "SHA256SUMS"}
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 64 * 1024


class ReleaseError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReleaseError(message)


def strict_json(data: bytes | str):
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(_value):
        raise ReleaseError("non-finite JSON value")

    try:
        return json.loads(data, object_pairs_hook=object_pairs, parse_constant=invalid_constant)
    except (UnicodeError, json.JSONDecodeError):
        raise ReleaseError("invalid JSON") from None


def _keys(value, expected, label):
    require(type(value) is dict and set(value) == set(expected), f"incorrect {label} fields")


def _hex(value, length):
    return type(value) is str and re.fullmatch(f"[0-9a-f]{{{length}}}", value) is not None


def _positive(value):
    return type(value) is int and value > 0


def validate_manifest(manifest: dict) -> dict:
    _keys(manifest, {"schema", "repository", "version", "tag", "commit", "tree", "workflow",
                     "ci", "tag_object", "toolchain", "artifacts"}, "manifest")
    require(type(manifest["schema"]) is int and manifest["schema"] == 1, "incorrect manifest schema")
    require(manifest["repository"] == REPOSITORY and manifest["version"] == VERSION
            and manifest["tag"] == TAG, "incorrect release identity")
    require(_hex(manifest["commit"], 40) and _hex(manifest["tree"], 40), "invalid source identity")
    workflow = manifest["workflow"]
    _keys(workflow, {"path", "sha256", "run_id", "run_attempt"}, "workflow")
    require(workflow["path"] == WORKFLOW_PATH and _hex(workflow["sha256"], 64)
            and _positive(workflow["run_id"]) and _positive(workflow["run_attempt"]), "invalid workflow identity")
    ci = manifest["ci"]
    _keys(ci, {"workflow_id", "run_id", "run_attempt", "head_sha"}, "CI")
    require(type(ci["workflow_id"]) is int and ci["workflow_id"] == 403832745
            and _positive(ci["run_id"]) and _positive(ci["run_attempt"])
            and ci["head_sha"] == manifest["commit"], "invalid CI identity")
    require(manifest["tag_object"] is None or _hex(manifest["tag_object"], 40), "invalid tag object")
    toolchain = manifest["toolchain"]
    require(type(toolchain) is dict and bool(toolchain), "missing toolchain")
    for name, version in toolchain.items():
        require(type(name) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", name)
                and type(version) is str and re.fullmatch(r"[0-9][A-Za-z0-9._+-]{0,95}", version),
                "invalid toolchain value")
    artifacts = manifest["artifacts"]
    require(type(artifacts) is list and len(artifacts) == 2, "expected exactly two distributions")
    names = []
    for artifact in artifacts:
        _keys(artifact, {"filename", "size", "sha256"}, "artifact")
        require(type(artifact["filename"]) is str and artifact["filename"] in PAIR,
                "incorrect distribution filename")
        require(_positive(artifact["size"]) and artifact["size"] <= MAX_ARTIFACT_BYTES
                and _hex(artifact["sha256"], 64), "invalid artifact identity")
        names.append(artifact["filename"])
    require(set(names) == PAIR, "duplicate or missing distribution")
    return manifest


def checksums(manifest: dict) -> bytes:
    return "".join(f"{item['sha256']}  {item['filename']}\n"
                   for item in sorted(manifest["artifacts"], key=lambda item: item["filename"])).encode("ascii")


def manifest_digest(bundle: Path) -> str:
    return hashlib.sha256((Path(bundle) / "manifest.json").read_bytes()).hexdigest()


def _inventory(directory: Path, names) -> None:
    require(directory.is_dir() and not directory.is_symlink(), "missing or linked artifact directory")
    entries = list(directory.iterdir())
    require({entry.name for entry in entries} == set(names), "incorrect artifact directory inventory")
    require(all(entry.is_file() and not entry.is_symlink() for entry in entries),
            "non-regular artifact member")


def verify_bundle(bundle: Path, expected_digest: str | None = None) -> dict:
    bundle = Path(bundle)
    _inventory(bundle, BUNDLE_FILES)
    manifest_path = bundle / "manifest.json"
    require(manifest_path.stat().st_size <= MAX_MANIFEST_BYTES, "manifest exceeds size bound")
    data = manifest_path.read_bytes()
    if expected_digest is not None:
        require(_hex(expected_digest, 64), "invalid expected manifest digest")
        require(hashlib.sha256(data).hexdigest() == expected_digest, "manifest digest mismatch")
    manifest = validate_manifest(strict_json(data))
    require((bundle / "SHA256SUMS").stat().st_size <= 1024, "checksum list exceeds size bound")
    require((bundle / "SHA256SUMS").read_bytes() == checksums(manifest), "checksum list mismatch")
    for artifact in manifest["artifacts"]:
        path = bundle / artifact["filename"]
        require(path.stat().st_size == artifact["size"], "distribution size mismatch")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == artifact["sha256"],
                "distribution digest mismatch")
    return manifest


def write_bundle(dist: Path, bundle: Path, identity: dict, toolchain: dict) -> dict:
    dist, bundle = Path(dist), Path(bundle)
    _inventory(dist, PAIR)
    _keys(identity, {"repository", "version", "tag", "commit", "tree", "workflow", "ci", "tag_object"},
          "release identity")
    artifacts = []
    for name in sorted(PAIR):
        path = dist / name
        require(0 < path.stat().st_size <= MAX_ARTIFACT_BYTES, "distribution exceeds size bound")
        artifacts.append({"filename": name, "size": path.stat().st_size,
                          "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest = validate_manifest({"schema": 1, **identity, "toolchain": toolchain, "artifacts": artifacts})
    require(not bundle.exists(), "bundle destination already exists")
    bundle.mkdir(parents=True)
    for name in sorted(PAIR):
        shutil.copyfile(dist / name, bundle / name)
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (bundle / "SHA256SUMS").write_bytes(checksums(manifest))
    return verify_bundle(bundle)


def check_parity(original: Path, rebuilt: Path) -> None:
    try:
        first = check_package.read_archive(original, wheel=True)
        second = check_package.read_archive(rebuilt, wheel=True)
    except (check_package.Rejected, OSError, ValueError, zipfile.BadZipFile) as error:
        raise ReleaseError("unreadable or unsafe parity wheel") from error
    require(set(first) == set(second), "rebuilt wheel member inventory differs")
    require(all(first[name] == second[name] for name in first), "rebuilt wheel member bytes differ")
