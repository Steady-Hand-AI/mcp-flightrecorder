"""Build one reviewed pair and coordinate its explicitly authorized promotion."""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile

# Only reviewed sibling tooling is added; runtime imports remain isolated.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from release_artifacts import (ReleaseError, SDIST, WHEEL, check_parity, manifest_digest,
                               require, verify_bundle, write_bundle)
from release_github import dispatch_identity, finalize_release, recheck_identity, verify_promotion_artifact
from release_registry import registry_plan, registry_verify


def child_environment(extra=None):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("GITHUB_", "ACTIONS_", "GH_", "PYTHON", "PYTEST"))
           and not any(word in key.upper() for word in ("TOKEN", "SECRET", "PASSWORD"))}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.update(extra or {})
    return env


def run(label, argv, work, *, cwd=None, extra_env=None):
    """Keep raw command output and paths runner-local, including failed runs."""
    work = Path(work)
    logs = work / "logs"
    logs.mkdir(exist_ok=True)
    argv = list(map(str, argv))
    with (logs / (label + ".log")).open("wb") as output:
        result = subprocess.run(argv, cwd=cwd or work, env=child_environment(extra_env),
                                stdout=output, stderr=subprocess.STDOUT, timeout=600)
    record = {"step": label, "argv": argv, "exit": result.returncode}
    with (work / "commands.jsonl").open("a") as output:
        output.write(json.dumps(record) + "\n")
    require(result.returncode == 0, f"release check failed: {label}; raw output remains runner-local")


def new_work(path, source):
    path, source = Path(path).resolve(), Path(source).resolve()
    require(not path.is_relative_to(source), "release work must be outside source")
    require(not path.exists(), "release work destination already exists")
    path.mkdir(parents=True)
    return path


def install_smoke(wheel, source, work):
    """Install precisely the supplied wheel, including registry-downloaded bytes."""
    wheel, source = Path(wheel).resolve(), Path(source).resolve()
    work = new_work(work, source)
    run("venv", [sys.executable, "-I", "-B", "-m", "venv", work / "env"], work)
    python = work / "env/bin/python"
    run("install", [python, "-I", "-B", "-m", "pip", "--isolated", "install", "--no-index", "--no-deps", wheel], work)
    run("pip-check", [python, "-I", "-B", "-m", "pip", "check"], work)
    run("installed-smoke", [python, "-I", "-B", source / "tools/installed_smoke.py",
                            "--source", source, "--output", work / "smoke", "--profile", "core"], work)


def build_candidate(repo, work, identity):
    """Purely local build seam; the CLI supplies authoritative dispatch identity."""
    repo = Path(repo).resolve()
    work = new_work(work, repo)
    source = work / "source"
    source.mkdir()
    run("export", ["git", "-C", repo, "archive", "--format=tar", "-o", work / "source.tar", identity["commit"]], work)
    with tarfile.open(work / "source.tar") as archive:
        archive.extractall(source, filter="data")
    python = sys.executable
    guard = source / "tools/check_package.py"
    run("source-guard", [python, "-I", "-B", guard, "--source", source], work)
    shutil.copytree(source, work / "build-source")
    dist = work / "dist"
    run("build", [python, "-I", "-B", "-m", "build", "--no-isolation", "--wheel", "--sdist",
                  "--outdir", dist, work / "build-source"], work)
    run("package-guard", [python, "-I", "-B", guard, "--source", source, "--wheel", dist / WHEEL,
                          "--sdist", dist / SDIST, "--report", work / "package-report.json"], work)
    run("twine", [python, "-I", "-B", "-m", "twine", "check", "--strict", dist / WHEEL, dist / SDIST], work)
    fixtures = {"MFR_GUARD_SOURCE": str(source), "MFR_GUARD_WHEEL": str(dist / WHEEL),
                "MFR_GUARD_SDIST": str(dist / SDIST), "TMPDIR": str(work)}
    run("guard-tests", [python, "-I", "-B", source / "tools/test_package_guard.py"], work, extra_env=fixtures)
    run("release-tests", [python, "-I", "-B", "-m", "unittest", "discover", "-s", source / "tools",
                          "-p", "test_release*.py", "-v"], work, extra_env=fixtures)
    extracted = work / "extracted"
    extracted.mkdir()
    with tarfile.open(dist / SDIST) as archive:
        archive.extractall(extracted, filter="data")
    sdist_source = extracted / "mcp_flightrecorder-0.5.18"
    rebuilt = work / "rebuilt"
    run("sdist-build", [python, "-I", "-B", "-m", "build", "--no-isolation", "--wheel", "--outdir", rebuilt, sdist_source], work)
    run("rebuilt-guard", [python, "-I", "-B", guard, "--source", source, "--wheel", rebuilt / WHEEL,
                          "--sdist", dist / SDIST, "--report", work / "rebuilt-report.json"], work)
    check_parity(dist / WHEEL, rebuilt / WHEEL)
    install_smoke(dist / WHEEL, source, work / "installed")
    install_smoke(rebuilt / WHEEL, sdist_source, work / "rebuilt-installed")
    toolchain = {name: importlib.metadata.version(name) for name in ("build", "setuptools", "packaging", "twine", "PyYAML")}
    toolchain["python"] = platform.python_version()
    manifest = write_bundle(dist, work / "bundle", identity, toolchain)
    return manifest


def stage_identity(repo, bundle, expected_digest, env):
    require(env.get("RELEASE_MODE") == "staged-release", "publication stage requires explicit staged-release")
    manifest = verify_bundle(bundle, expected_digest)
    require(manifest["tag_object"] is not None, "publication requires the approved annotated tag")
    recheck_identity(repo, manifest, env)
    verify_promotion_artifact(manifest, env)
    return manifest


def plan_registry_stage(repo, bundle, expected_digest, registry, work, env):
    stage_identity(repo, bundle, expected_digest, env)
    work = new_work(work, repo)
    missing = registry_plan(bundle, registry, work / "upload", expected_digest)
    # This is the final step before the pinned publisher action receives files.
    stage_identity(repo, bundle, expected_digest, env)
    return len(missing)


def verify_registry_stage(repo, bundle, expected_digest, registry, work, env):
    stage_identity(repo, bundle, expected_digest, env)
    work = new_work(work, repo)
    downloaded = registry_verify(bundle, registry, work / "downloaded", expected_digest)
    install_smoke(downloaded / WHEEL, repo, work / "installed")
    stage_identity(repo, bundle, expected_digest, env)


def finalize_github_stage(repo, bundle, expected_digest, env):
    manifest = stage_identity(repo, bundle, expected_digest, env)
    notes = (Path(repo) / "docs/release-notes.md").read_text(encoding="utf-8")
    return finalize_release(bundle, manifest, notes,
                            lambda: stage_identity(repo, bundle, expected_digest, env))


def set_output(key, value):
    output = os.environ.get("GITHUB_OUTPUT")
    require(output is not None, "GitHub output channel is unavailable")
    require("\n" not in str(value) and "\r" not in str(value), "invalid output value")
    with open(output, "a", encoding="utf-8") as stream:
        stream.write(f"{key}={value}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--work", required=True, type=Path)
    for name in ("registry-plan", "registry-verify", "github-release"):
        command = commands.add_parser(name)
        command.add_argument("--bundle", required=True, type=Path)
        command.add_argument("--manifest-sha256", required=True)
        if name != "github-release":
            command.add_argument("--registry", required=True, choices=("testpypi", "pypi"))
            command.add_argument("--work", required=True, type=Path)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    try:
        require(sys.flags.isolated and not sys.flags.optimize, "use isolated Python with assertions enabled")
        if args.command == "build":
            identity = dispatch_identity(repo, os.environ)
            manifest = build_candidate(repo, args.work, identity)
            recheck_identity(repo, manifest, os.environ)
            set_output("manifest_sha256", manifest_digest(args.work / "bundle"))
        elif args.command == "registry-plan":
            count = plan_registry_stage(repo, args.bundle, args.manifest_sha256, args.registry, args.work, os.environ)
            set_output("missing_count", count)
        elif args.command == "registry-verify":
            verify_registry_stage(repo, args.bundle, args.manifest_sha256, args.registry, args.work, os.environ)
        else:
            finalize_github_stage(repo, args.bundle, args.manifest_sha256, os.environ)
    except ReleaseError as error:
        print(f"Release refused: {error}", file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        print("Release failed: input, service or local command unavailable; no downstream stage is authorized", file=sys.stderr)
        return 1
    print(json.dumps({"stage": args.command, "status": "PASS"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
