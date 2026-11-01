"""Fail closed on unreviewed source/distribution members; never echo file content."""
from __future__ import annotations

import argparse
import base64
import csv
from email.parser import BytesParser
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import stat
import tarfile
import tomllib
import zipfile


class Rejected(ValueError):
    pass


PRIVATE_NAMES = {
    "agents.md", "claude.md", "skill.md", "plan.md", "plan.html", "task.md",
    "next_task.md", "review.md", "result.md", "current_state.md", "baseline.json",
    "identity.json", "issues.json", ".env", ".ds_store",
}
PRIVATE_DIRS = {
    ".git", ".agents", ".codex", ".claude", "private", "internal", "review",
    "playbook", "skills", "handoff", "evidence", "results", "recordings",
    "archive", "work", ".venv", "venv", "__pycache__", ".pytest_cache",
}
PRIVATE_SUFFIXES = (".sqlite", ".db", ".log", ".jsonl", ".pem", ".key", ".p12",
                    ".pfx", ".zip", ".whl", ".tar", ".gz", ".pyc")
# Specific machine paths and credential formats, not words such as "secret".
CONTENT_PATTERNS = {
    "personal absolute path": re.compile(rb"/(?:Users|Volumes|home)/[A-Za-z0-9_.-]+/"),
    "private key": re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "GitHub token": re.compile(rb"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{40,})\b"),
    "AWS access key": re.compile(rb"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "provider key": re.compile(rb"\bsk-(?:proj-)?[A-Za-z0-9_-]{32,}\b"),
}
URLS = {
    "Repository": "https://github.com/Steady-Hand-AI/mcp-flightrecorder",
    "Issues": "https://github.com/Steady-Hand-AI/mcp-flightrecorder/issues",
    "Documentation": "https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/README.md",
}


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise Rejected(reason)


def safe_name(name: str) -> str:
    require(bool(name) and not name.startswith("/") and "\\" not in name
            and ":" not in name and all(ord(c) >= 32 for c in name), "unsafe member path")
    require(all(p not in {"", ".", ".."} for p in name.split("/")), "unsafe member path")
    return name


def inspect_content(name: str, data: bytes) -> None:
    path = PurePosixPath(name.lower())
    require(path.name not in PRIVATE_NAMES and not path.name.startswith(".env.")
            and not set(path.parts[:-1]) & PRIVATE_DIRS
            and not path.name.endswith(PRIVATE_SUFFIXES)
            and ".sqlite-" not in path.name, f"forbidden member: {name}")
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        raise Rejected(f"non-text content: {name}") from None
    for label, pattern in CONTENT_PATTERNS.items():
        require(not pattern.search(data), f"{label} detected in {name}; value omitted")


def exact(actual: set[str], expected: set[str], label: str) -> None:
    require(actual == expected,
            f"{label} inventory differs: unexpected={sorted(actual - expected)}, "
            f"missing={sorted(expected - actual)}")


def read_source(root: Path) -> dict[str, bytes]:
    members = {}
    for path in sorted(root.rglob("*")):
        name = safe_name(path.relative_to(root).as_posix())
        require(not path.is_symlink(), f"source link: {name}")
        if path.is_dir():
            require(not set(PurePosixPath(name.lower()).parts) & PRIVATE_DIRS,
                    f"forbidden directory: {name}")
            continue
        require(path.is_file(), f"non-regular source: {name}")
        members[name] = path.read_bytes()
    require("tools/public_files.txt" in members, "missing public inventory")
    inventory = members["tools/public_files.txt"].decode("utf-8").splitlines()
    require(len(inventory) == len(set(inventory)), "duplicate public inventory member")
    for name in inventory:
        safe_name(name)
    exact(set(members), set(inventory), "source")
    for name, data in members.items():
        inspect_content(name, data)
    return members


def read_archive(path: Path, *, wheel: bool) -> dict[str, bytes]:
    members: dict[str, bytes] = {}
    seen: set[str] = set()
    directories = set()

    def admit(name: str, directory: bool) -> str:
        name = safe_name(name[:-1] if directory and name.endswith("/") else name)
        require(name not in seen, "duplicate archive member")
        seen.add(name)
        if directory:
            directories.add(name)
        return name

    if wheel:
        with zipfile.ZipFile(path) as archive:
            for entry in archive.infolist():
                name = admit(entry.filename, entry.is_dir())
                mode = stat.S_IFMT(entry.external_attr >> 16)
                require(mode in {0, stat.S_IFREG, stat.S_IFDIR}, "non-regular wheel member")
                if not entry.is_dir():
                    members[name] = archive.read(entry)
    else:
        with tarfile.open(path, "r:gz") as archive:
            for entry in archive:
                name = admit(entry.name, entry.isdir())
                require(entry.isdir() or entry.isfile(), "non-regular sdist member")
                if entry.isfile():
                    stream = archive.extractfile(entry)
                    require(stream is not None, "unreadable sdist member")
                    members[name] = stream.read()
    parents = {str(p) for name in members for p in PurePosixPath(name).parents if str(p) != "."}
    require(directories <= parents, "unexpected empty archive directory")
    for name, data in members.items():
        inspect_content(name, data)
    return members


def check_metadata(data: bytes, project: dict) -> None:
    metadata = BytesParser().parsebytes(data)
    for key, value in {"Name": project["name"], "Version": project["version"],
                       "Requires-Python": ">=3.11", "License-Expression": "Apache-2.0",
                       "Author": "Volodymyr Barylyak", "Maintainer": "Volodymyr Barylyak"}.items():
        require(metadata.get_all(key) == [value], f"incorrect metadata {key}")
    require(set(metadata.get_all("Project-URL", [])) == {f"{k}, {v}" for k, v in URLS.items()},
            "incorrect Project-URL metadata")
    require(set(metadata.get_all("License-File", [])) == {"LICENSE", "NOTICE"},
            "incorrect license metadata")
    extras = project["optional-dependencies"]
    require(set(metadata.get_all("Provides-Extra", [])) == set(extras), "incorrect extras metadata")

    def requirement(value: str) -> str:
        # These declared requirements use simple version lists and extra markers.
        # Build backends may reorder comma-separated version constraints.
        dependency, separator, marker = value.partition(";")
        match = re.fullmatch(r"([A-Za-z0-9_.-]+(?:\[[^\]]+\])?)(.*)", dependency.strip())
        require(match is not None, "malformed dependency metadata")
        name, versions = match.groups()
        return name + ",".join(sorted(versions.split(","))) + separator + marker.strip()

    expected = project["dependencies"] + [
        f'{dependency}; extra == "{extra}"' for extra, dependencies in extras.items() for dependency in dependencies
    ]
    require(sorted(map(requirement, metadata.get_all("Requires-Dist", []))) == sorted(map(requirement, expected)),
            "incorrect dependency metadata")


def check_wheel(members: dict[str, bytes], source: dict[str, bytes], project: dict) -> None:
    prefix = f"mcp_flightrecorder-{project['version']}.dist-info/"
    copied = {name.removeprefix("src/"): data for name, data in source.items()
              if name.startswith("src/mcp_flightrecorder/")}
    copied["mcp_flightrecorder/schema.sql"] = source["sql/schema.sql"]
    for name in ("LICENSE", "NOTICE"):
        copied[prefix + "licenses/" + name] = source[name]
    generated = {prefix + name for name in ("METADATA", "WHEEL", "entry_points.txt", "top_level.txt", "RECORD")}
    exact(set(members), set(copied) | generated, "wheel")
    for name, data in copied.items():
        require(members[name] == data, f"wheel content differs from source: {name}")
    check_metadata(members[prefix + "METADATA"], project)
    require(members[prefix + "entry_points.txt"].decode().strip() ==
            "[console_scripts]\nfr = mcp_flightrecorder.cli:main", "incorrect wheel CLI entrypoint")
    rows = list(csv.reader(io.StringIO(members[prefix + "RECORD"].decode())))
    require(all(len(row) == 3 for row in rows), "malformed wheel RECORD")
    require(len(rows) == len(members), "duplicate or missing wheel RECORD row")
    exact({row[0] for row in rows}, set(members), "wheel RECORD")
    for name, digest, size in rows:
        if name == prefix + "RECORD":
            require(digest == size == "", "invalid RECORD self-entry")
        else:
            expected = base64.urlsafe_b64encode(hashlib.sha256(members[name]).digest()).rstrip(b"=").decode()
            require(digest == "sha256=" + expected and size == str(len(members[name])),
                    f"wheel RECORD mismatch: {name}")


def check_sdist(members: dict[str, bytes], source: dict[str, bytes], project: dict) -> None:
    prefix = f"mcp_flightrecorder-{project['version']}/"
    require(all(name.startswith(prefix) for name in members), "incorrect sdist root")
    members = {name.removeprefix(prefix): data for name, data in members.items()}
    # Repository automation is not needed to rebuild the source distribution.
    copied = {name: data for name, data in source.items()
              if name != ".gitignore" and not name.startswith(".github/")}
    egg = "src/mcp_flightrecorder.egg-info/"
    generated = {"PKG-INFO", "setup.cfg"} | {egg + name for name in (
        "PKG-INFO", "SOURCES.txt", "dependency_links.txt", "entry_points.txt", "requires.txt", "top_level.txt")}
    exact(set(members), set(copied) | generated, "sdist")
    for name, data in copied.items():
        require(members[name] == data, f"sdist content differs from source: {name}")
    check_metadata(members["PKG-INFO"], project)
    check_metadata(members[egg + "PKG-INFO"], project)
    exact(set(members[egg + "SOURCES.txt"].decode().splitlines()),
          set(members) - {"PKG-INFO", "setup.cfg"}, "sdist build manifest")


def inventory(members: dict[str, bytes]) -> list[dict]:
    return [{"path": name, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            for name, data in sorted(members.items())]


def check(source: Path, wheel: Path | None = None, sdist: Path | None = None) -> dict:
    members = read_source(source)
    project = tomllib.loads(members["pyproject.toml"].decode())["project"]
    require(project["name"] == "mcp-flightrecorder" and project["version"] == "0.5.16"
            and project["requires-python"] == ">=3.11" and project["license"] == "Apache-2.0"
            and project["urls"] == URLS, "incorrect project identity")
    report = {"status": "PASS", "source": inventory(members)}
    for kind, path, validator in (("wheel", wheel, check_wheel), ("sdist", sdist, check_sdist)):
        if path is not None:
            packed = read_archive(path, wheel=kind == "wheel")
            validator(packed, members, project)
            report[kind] = {"path": str(path.resolve()), "size": path.stat().st_size,
                            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                            "members": inventory(packed)}
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="pristine tracked-source export")
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--sdist", type=Path)
    parser.add_argument("--report", type=Path, help="write inventory JSON outside the source")
    args = parser.parse_args()
    try:
        report = check(args.source, args.wheel, args.sdist)
    except Rejected as error:
        print(f"Package guard FAIL: {error}")
        return 1
    except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as error:
        print(f"Package guard FAIL: unreadable input ({type(error).__name__}); details omitted")
        return 1
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Package guard PASS: " + ", ".join(report.keys() - {"status"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
