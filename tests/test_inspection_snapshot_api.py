"""The public snapshot entry shares the installed CLI's backup and refusal rules."""
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from mcp_flightrecorder import inspect

from .test_inspection_snapshot import closed_recording, live_recording, _source_facts


def test_public_snapshot_default_and_installed_cli_share_live_source(live_recording, tmp_path):
    source, recorder = live_recording
    before = _source_facts(source)
    python_output, cli_output = tmp_path / "python.sqlite", tmp_path / "cli.sqlite"
    actual = inspect.snapshot(source, python_output)
    command = subprocess.run(
        [str(Path(sys.executable).with_name("fr")), "snapshot", str(source), str(cli_output)],
        cwd=tmp_path, capture_output=True, text=True, timeout=15,
    )
    assert command.returncode == 0 and not command.stderr
    cli = json.loads(command.stdout)
    assert actual["ok"] and actual["operation"] == "snapshot"
    assert actual == {**cli, "data": {**cli["data"], "output_path": str(python_output)}}
    assert actual["data"]["source_run_states"][0]["close_state"] == "open"
    assert _source_facts(python_output) == _source_facts(cli_output) == {**before, "journal_mode": "delete"}
    assert _source_facts(source) == before
    for output in (python_output, cli_output):
        assert not any(Path(str(output) + suffix).exists() for suffix in ("-wal", "-shm", "-journal"))
    assert recorder.health().ordinary.used_slots == 0


@pytest.mark.parametrize("case,reason", [("self", "self_copy"), ("existing", "destination_exists")])
def test_public_snapshot_refuses_owned_destinations(closed_recording, tmp_path, case, reason):
    output = closed_recording if case == "self" else tmp_path / "owned.sqlite"
    if case == "existing":
        output.write_bytes(b"owned destination")
    before = output.read_bytes()
    result = inspect.snapshot(closed_recording, output)
    assert not result["ok"] and result["error"]["reason"] == reason
    assert output.read_bytes() == before
    assert not list(tmp_path.glob(".mfr-snapshot-*"))


@pytest.mark.parametrize("timeout", [0, 31, True])
def test_public_snapshot_validates_deadline_before_destination(closed_recording, tmp_path, timeout):
    output = tmp_path / "invalid.sqlite"
    result = inspect.snapshot(closed_recording, output, timeout=timeout)
    assert not result["ok"] and result["error"]["code"] == "invalid_request"
    assert not output.exists() and not list(tmp_path.glob(".mfr-snapshot-*"))


def test_public_snapshot_backup_failure_removes_private_stage(closed_recording, tmp_path, monkeypatch):
    import mcp_flightrecorder._snapshot as implementation

    output = tmp_path / "failed.sqlite"
    before = closed_recording.read_bytes()

    def fail(ctx, destination):
        assert destination.parent.is_dir()
        raise sqlite3.OperationalError("controlled backup failure")

    monkeypatch.setattr(implementation, "_backup", fail)
    result = inspect.snapshot(closed_recording, output)
    assert not result["ok"] and result["error"]["code"] == "recording_unreadable"
    assert not output.exists() and not list(tmp_path.glob(".mfr-snapshot-*"))
    assert closed_recording.read_bytes() == before
