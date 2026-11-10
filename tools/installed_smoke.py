"""Check a normally installed wheel from outside its source directory."""

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import site
import sqlite3
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=("core", "integration"), required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    output = args.output.resolve()
    assert not Path.cwd().resolve().is_relative_to(source), "Run outside the source directory"
    assert not output.is_relative_to(source), "Smoke output must be outside source"
    assert sys.prefix != sys.base_prefix, "Use an isolated virtual environment"
    assert not sys.flags.optimize, "Assertions must be enabled"
    assert sys.flags.isolated, "Use python -I"
    import mcp_flightrecorder

    installed = Path(mcp_flightrecorder.__file__).resolve()
    assert any(installed.is_relative_to(Path(p).resolve()) for p in site.getsitepackages())
    assert installed.is_relative_to(Path(sys.prefix).resolve())
    assert not installed.is_relative_to(source)
    assert mcp_flightrecorder.__version__ == "0.5.19"
    assert (installed.parent / "schema.sql").read_bytes() == (source / "sql/schema.sql").read_bytes()
    sdks = ("openai", "mcp", "mcp-types", "httpx2")
    if args.profile == "core":
        for name in ("openai", "mcp", "mcp_types", "httpx2"):
            assert importlib.util.find_spec(name) is None, f"SDK leaked into core environment: {name}"
        sdk_versions = {}
    else:
        sdk_versions = {name: importlib.metadata.version(name) for name in sdks}

    output.mkdir()
    cli = str(Path(sys.executable).parent / "fr")
    commands = []

    def run(argv, *, json_output=True):
        result = subprocess.run(argv, cwd=output, text=True, capture_output=True, timeout=60)
        if result.returncode:
            sys.stdout.write(result.stdout)
            sys.stderr.write(result.stderr)
            result.check_returncode()
        commands.append({"argv": argv, "exit": result.returncode})
        return json.loads(result.stdout) if json_output else result.stdout.strip()

    def inspect(*arguments):
        result = run([cli, *arguments])
        assert result["ok"] is True, result
        assert result["error"] is None, result
        return result["data"]

    assert run([cli, "--version"], json_output=False) == "mcp-flightrecorder 0.5.19"
    schema = inspect("schema")
    assert schema["database_schema_version"] == 6
    assert schema["package_version"] == "0.5.19"
    recording = output / "run.sqlite"
    copy = output / "copy.sqlite"
    created = run([sys.executable, "-I", "-B", str(source / "examples/quickstart.py"), str(recording)], json_output=False)
    assert Path(created).resolve() == recording
    original_hash = hashlib.sha256(recording.read_bytes()).hexdigest()
    turns = inspect("turns", str(recording))
    assert len(turns["items"]) == 1
    assert turns["items"][0]["counts"]["events"] == 1
    assert inspect("sql", str(recording), "SELECT count(*) AS n FROM events")["rows"] == [[1]]
    snapshot = inspect("snapshot", str(recording), str(copy))
    assert snapshot["verified"] is True
    assert snapshot["journal_mode"] == "delete"
    assert snapshot["output_path"] == str(copy)
    assert len(inspect("runs", str(copy))["items"]) == 1
    connection = sqlite3.connect(copy.as_uri() + "?mode=ro", uri=True)
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)
        assert connection.execute("SELECT count(*) FROM events").fetchone() == (1,)
    finally:
        connection.close()
    assert hashlib.sha256(recording.read_bytes()).hexdigest() == original_hash
    assert all(not Path(str(copy) + suffix).exists() for suffix in ("-wal", "-shm", "-journal"))
    facade_recording = output / "agent.sqlite"
    facade_result = run([sys.executable, "-I", "-B", str(source / "examples/agent_recorder.py"),
                         "--output", str(facade_recording)])
    assert facade_result == {"answer": "alpha", "recording": str(facade_recording)}
    disabled_path = output / "disabled.sqlite"
    disabled_result = run([sys.executable, "-I", "-B", str(source / "examples/agent_recorder.py"),
                           "--output", str(disabled_path), "--disabled"])
    assert disabled_result == {"answer": "alpha", "recording": None}
    assert not disabled_path.exists()
    diagnosis = run([cli, "doctor", str(facade_recording), "--json"])
    assert diagnosis["ok"] and diagnosis["format"] == "mfr.diagnose/1"
    assert diagnosis["data"]["whole_run_verdict"] == "not_evaluated"
    assert run([cli, "doctor", str(facade_recording)], json_output=False)
    assert run([cli, "turns", str(facade_recording), "--last", "--text"], json_output=False)
    assert run([cli, "explain", str(facade_recording), "--last", "--text"], json_output=False)
    assert len(inspect("turns", str(facade_recording))["items"]) == 2
    report = {
        "result": "PASS", "python": sys.version, "executable": sys.executable,
        "sqlite": sqlite3.sqlite_version, "installed_module": str(installed),
        "sdk_versions": sdk_versions, "commands": commands,
        "recording_sha256": original_hash,
    }
    (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
