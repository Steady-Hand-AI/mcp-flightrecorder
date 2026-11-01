"""Installed snapshot command: real WAL backup, refusal and owned-file cleanup."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder, inspect
from mcp_flightrecorder.cli import main

from .recordings import core_recording


COVERAGE = {"producers": {"model": "snapshot.model", "host": "snapshot.host", "mcp": "snapshot.mcp"},
            "model_complete": True}
SUFFIXES = ("", "-wal", "-shm", "-journal")


def _paths(path):
    return [Path(str(path) + suffix) for suffix in SUFFIXES]


def _drained(recorder):
    # Public health describes charged pending work; wait for actual committed release within an outer watchdog.
    deadline, pause = time.monotonic() + 10, threading.Event()
    while recorder.health().ordinary.used_slots and time.monotonic() < deadline:
        pause.wait(0.002)
    assert recorder.health().ordinary.used_slots == 0, "The public producer must commit all accepted rows before their snapshot oracle is sampled."


@pytest.fixture
def live_recording(tmp_path):
    source = tmp_path / "live source ? #.sqlite"
    recorder = Recorder(source, capture="content", coverage=COVERAGE)
    try:
        with recorder.turn("snapshot-content-sentinel", conditions={"case": "installed_snapshot"}) as turn:
            assert turn.receipt.status == "accepted", "The snapshot source must be produced through a successful public turn admission."
        _drained(recorder)
        yield source, recorder
    finally:
        closed = recorder.close()
        assert closed.writer_stopped, "The fixture must join its real public producer even when a snapshot assertion fails."


@pytest.fixture
def closed_recording(tmp_path):
    source = tmp_path / "closed-source.sqlite"
    recording, _ = core_recording()
    shutil.copyfile(recording, source)
    return source


def _source_facts(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        states = [dict(zip(("run_id", "close_state"), row)) for row in connection.execute(
            "SELECT run_id,close_state FROM runs ORDER BY started_at,run_id")]
        counts = {table: connection.execute("SELECT count(*) FROM " + table).fetchone()[0]
                  for table in ("runs", "turns", "events", "operations", "links", "artifacts", "findings")}
        bodies = connection.execute("SELECT artifact_id,body_text FROM artifacts ORDER BY artifact_id").fetchall()
    return {"journal_mode": mode, "states": states, "counts": counts,
            "content_sha256": hashlib.sha256(json.dumps(bodies, ensure_ascii=False).encode()).hexdigest()}


def _command(entry, source, output, cwd, *options):
    prefix = [str(Path(sys.executable).with_name("fr"))] if entry == "fr" else [sys.executable, "-I", "-B", "-m", "mcp_flightrecorder"]
    process = subprocess.run(prefix + ["snapshot", str(source), str(output), *options], cwd=cwd,
                             capture_output=True, text=True, timeout=15)
    assert process.stdout, "The actual installed snapshot command must emit a result even for an expected refusal."
    result = json.loads(process.stdout)
    check.equal(process.stdout, json.dumps(result, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n",
                msg="Snapshot emits one compact JSON envelope and exactly one newline.")
    check.equal(process.stderr, "", msg="Ordinary snapshot success/failure must not leak tracebacks or raw storage diagnostics.")
    check.equal(result["operation"], "snapshot", msg="Local copy reports the assigned snapshot operation name.")
    check.equal(result["format"], "mfr.inspect/1", msg="Snapshot shares the fixed inspection envelope version.")
    return process, result


def _refused(process, result, code, exit_code, reason=None):
    assert isinstance(result.get("error"), dict), "A negative snapshot control must expose the intended structured error."
    check.equal(process.returncode, exit_code, msg="The copy refusal/failure must use its specifically assigned expected exit.")
    check.is_false(result["ok"], msg="An unsuccessful copy must never claim verified completion.")
    check.is_none(result["data"], msg="An unsuccessful copy cannot return partial successful snapshot metadata.")
    check.equal(result["error"]["code"], code, msg="Copy argument, storage, WAL and deadline failures remain distinct.")
    if reason:
        check.equal(result["error"]["reason"], reason, msg="The controlled failure reaches its named implementation branch.")
    check.less_equal(len(process.stdout[:-1].encode()), 1024, msg="Snapshot failure envelopes obey the small independent error cap.")


@pytest.mark.parametrize("entry", ["fr", "module"])
def test_installed_snapshot_preserves_live_wal_and_verifies_lone_file(live_recording, tmp_path, entry):
    source, recorder = live_recording
    before = _source_facts(source)
    assert before["journal_mode"] == "wal", "This success control must exercise an accessible real live-WAL source."
    assert before["states"][0]["close_state"] == "open", "A still-accepting public producer must expose its observed open run state."
    wal = Path(str(source) + "-wal")
    shm = Path(str(source) + "-shm")
    assert wal.is_file() and shm.is_file(), "The live producer must have its required accessible WAL fileset."
    source_bytes, wal_bytes = source.read_bytes(), wal.read_bytes()
    cwd = tmp_path / "empty-command-directory"
    cwd.mkdir()
    output = tmp_path / "portable output ? #.sqlite"
    process, result = _command(entry, source, output, cwd)
    assert result["ok"], "The installed command must complete the live SQLite backup before destination verification."
    check.equal(process.returncode, 0, msg="Accessible WAL is a supported successful source, not evidence of a read error.")
    check.equal(result["data"], {"output_path": str(output), "journal_mode": "delete", "verified": True,
                                "source_run_states": before["states"]},
                msg="Snapshot reports the intact destination identity and the copied open run state.")
    checked = inspect.check(output, before["states"][0]["run_id"])
    assert checked["ok"], "The copied open recording must be inspectable before its capture-health meaning is assessed."
    health_items = [item for item in checked["data"]["items"] if item["id"] == "capture_health"]
    assert len(health_items) == 1, "A completed integrity read must expose its one required capture-health item."
    check.equal(health_items[0]["status"], "unknown",
                msg="Making an open recording portable cannot manufacture completed capture health.")
    check.equal(source.read_bytes(), source_bytes, msg="The utility never modifies the live source main-file bytes.")
    check.equal(wal.read_bytes(), wal_bytes, msg="The utility never checkpoints, truncates or rewrites the live source WAL.")
    check.equal(_source_facts(source), before, msg="Source journal, row counts, content hashes and closure state stay unchanged.")
    check.is_true(wal.exists() and shm.exists(), msg="Snapshot must not manually remove source sidecars.")
    for sidecar in _paths(output)[1:]:
        check.is_false(sidecar.exists(), msg="The new portable DELETE output has no required SQLite sidecar.")
    lone = tmp_path / "lone-main-file"
    lone.mkdir()
    copied = lone / "recording.sqlite"
    shutil.copyfile(output, copied)
    probe = '''from contextlib import closing
import hashlib,json,sqlite3,sys
from pathlib import Path
p=Path(sys.argv[1])
with closing(sqlite3.connect(p.as_uri()+"?mode=ro",uri=True)) as c:
 states=[dict(zip(("run_id","close_state"),r)) for r in c.execute("SELECT run_id,close_state FROM runs ORDER BY started_at,run_id")]
 counts={t:c.execute("SELECT count(*) FROM "+t).fetchone()[0] for t in ("runs","turns","events","operations","links","artifacts","findings")}
 bodies=c.execute("SELECT artifact_id,body_text FROM artifacts ORDER BY artifact_id").fetchall()
 print(json.dumps({"journal_mode":c.execute("PRAGMA journal_mode").fetchone()[0],"states":states,"counts":counts,"content_sha256":hashlib.sha256(json.dumps(bodies,ensure_ascii=False).encode()).hexdigest()}))
'''
    fresh = subprocess.run([sys.executable, "-I", "-B", "-c", probe, str(copied)], cwd=lone,
                           capture_output=True, text=True, timeout=10)
    assert fresh.returncode == 0, "A new isolated process must read a copied destination main file without sibling resources."
    observed = json.loads(fresh.stdout)
    check.equal(observed, {**before, "journal_mode": "delete"}, msg="Independent lone-file verification preserves every copied row and content hash.")
    with recorder.turn("source remains usable") as later:
        check.equal(later.receipt.status, "accepted", msg="Snapshot cannot stop or take ownership of the still-live recorder.")
    _drained(recorder)
    check.equal(_source_facts(source)["counts"]["turns"], 2, msg="The source can commit additional work after the backup.")
    check.equal(_source_facts(output)["counts"]["turns"], 1, msg="The destination remains the earlier coherent snapshot after source changes.")


@pytest.mark.parametrize("suffix", ("-wal", "-shm", "-journal"),
                         ids=("inside-link-wal", "inside-link-shm", "inside-link-journal"))
def test_snapshot_sidecar_arrives_inside_link_refuses_verified_success(
        closed_recording, tmp_path, monkeypatch, capsys, suffix):
    """A sidecar arriving inside link() invalidates the public success claim."""
    import mcp_flightrecorder._snapshot as snapshot_module

    output = tmp_path / "late-sidecar.sqlite"
    competitor = Path(str(output) + suffix)
    marker = b"INDEPENDENT_LINK_BOUNDARY_OWNER:" + suffix.encode("ascii")
    source_bytes = closed_recording.read_bytes()
    assert inspect.runs(closed_recording)["ok"], "The source must be inspectable before the interleaving."
    real_link = os.link
    calls = []

    def competing_link(staged, destination, *args, **kwargs):
        assert Path(destination) == output, "Only the intended public output may be published."
        assert Path(staged).name == "copy.sqlite", "The native link must publish the verified staged main."
        assert not competitor.exists(), "The pre-link availability scan must have observed no sidecar."
        with competitor.open("xb") as stream:
            stream.write(marker)
        calls.append((Path(staged), Path(destination)))
        return real_link(staged, destination, *args, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(snapshot_module.os, "link", competing_link)
        patched.setattr(sys, "argv", ["fr", "snapshot", str(closed_recording), str(output)])
        code = main()
    captured = capsys.readouterr()
    assert len(calls) == 1, "The host must make exactly one real, exclusively publishing link call."
    assert captured.err == "", "Expected error handling must not emit a traceback or raw paths."
    result = json.loads(captured.out)
    assert captured.out == json.dumps(result, ensure_ascii=False, allow_nan=False,
                                      separators=(",", ":")) + "\n", "CLI emits exactly one compact result."
    assert code == 3, "A visible post-link suffix must be an inspection/storage error."
    assert result["operation"] == "snapshot"
    assert result["ok"] is False and result["data"] is None
    assert result["error"]["code"] == "recording_unreadable"
    assert result["error"]["reason"] == "destination_finalize"
    assert result["error"]["message"] == (
        "A destination SQLite sidecar appeared during publication; "
        "the public main may already exist. Inspect the destination before using it.")
    assert competitor.read_bytes() == marker, "Snapshot must never modify independent sidecar bytes."
    assert output.is_file(), "After a successful native link the public main must not be unlinked."
    assert closed_recording.read_bytes() == source_bytes, "Snapshot must not change the source bytes."
    assert inspect.runs(closed_recording)["ok"], "Source readability must survive publication failure."
    assert not list(tmp_path.glob(".mfr-snapshot-*")), "Private staging must be removed on failure."


@pytest.mark.parametrize("suffix", SUFFIXES, ids=["existing-main", "existing-wal", "existing-shm", "existing-journal"])
def test_snapshot_collision_preserves_each_preexisting_destination(closed_recording, tmp_path, suffix):
    output = tmp_path / "collision.sqlite"
    existing = Path(str(output) + suffix)
    existing.write_bytes(b"preexisting-owner-sentinel")
    source_before = closed_recording.read_bytes()
    process, result = _command("fr", closed_recording, output, tmp_path)
    _refused(process, result, "invalid_request", 1, "destination_exists")
    check.equal(existing.read_bytes(), b"preexisting-owner-sentinel", msg="A collision must preserve the exact preexisting main/sidecar bytes.")
    check.equal(closed_recording.read_bytes(), source_before, msg="Destination refusal cannot alter the source recording.")
    for candidate in _paths(output):
        if candidate != existing:
            check.is_false(candidate.exists(), msg="A refused preexisting destination cannot create or remove other output siblings.")


def test_snapshot_self_copy_is_refused_before_any_source_change(closed_recording, tmp_path):
    before = closed_recording.read_bytes()
    process, result = _command("module", closed_recording, closed_recording, tmp_path)
    _refused(process, result, "invalid_request", 1, "self_copy")
    check.equal(closed_recording.read_bytes(), before, msg="Self-copy refusal preserves the exact source file.")


def test_snapshot_dangling_destination_symlink_is_an_existing_owner(closed_recording, tmp_path):
    output, target = tmp_path / "owned-symlink.sqlite", tmp_path / "uncreated-target.sqlite"
    output.symlink_to(target.name)
    before = closed_recording.read_bytes()
    process, result = _command("fr", closed_recording, output, tmp_path)
    _refused(process, result, "invalid_request", 1, "destination_exists")
    check.is_true(output.is_symlink(), msg="A dangling destination link is preexisting owner evidence and must remain intact.")
    check.equal(output.readlink(), Path(target.name), msg="Refusal cannot redirect the preexisting symbolic link.")
    check.is_false(target.exists(), msg="Snapshot must not follow a dangling final-component link to create its target.")
    check.equal(closed_recording.read_bytes(), before, msg="A dangling destination refusal leaves source bytes unchanged.")


def test_snapshot_parent_directory_symlink_allows_fresh_destination(closed_recording, tmp_path):
    directory = tmp_path / "actual-output-directory"
    directory.mkdir()
    alias = tmp_path / "output-directory-alias"
    alias.symlink_to(directory, target_is_directory=True)
    output = alias / "fresh.sqlite"
    process, result = _command("module", closed_recording, output, tmp_path)
    assert result["ok"], "A legitimate existing parent-directory alias must allow a genuinely fresh destination."
    check.equal(process.returncode, 0, msg="A symlinked parent is not itself an existing destination collision.")
    check.equal(result["data"]["output_path"], str(output.resolve()), msg="The successful output retains its full resolved file identity.")
    check.is_true(alias.is_symlink(), msg="Snapshot preserves the independently owned directory alias.")
    check.equal(_source_facts(output)["counts"], _source_facts(closed_recording)["counts"],
                msg="The fresh destination reached through a parent alias contains the real copied recording.")


def test_snapshot_default_output_budget_omits_only_trailing_intact_states(closed_recording, tmp_path):
    # Extra runs are a labelled reader-input variant in a private copy, not a fabricated public-producer fixture.
    with closing(sqlite3.connect(closed_recording)) as writer:
        writer.executemany("INSERT INTO runs(run_id,started_at,environment_json,policy_json,coverage_json) VALUES(?,?,?,?,?)",
                           [("state-" + str(index).zfill(5) + "x" * 80, "9999", "{}", "{}", "{}") for index in range(1200)])
        writer.commit()
    expected = _source_facts(closed_recording)["states"]
    output = tmp_path / "many-states.sqlite"
    process, result = _command("fr", closed_recording, output, tmp_path)
    assert result["ok"], "An oversized optional run-state listing must not invalidate the successfully verified copied database."
    states = result["data"]["source_run_states"]
    check.equal(process.returncode, 0, msg="Trimming optional snapshot metadata preserves verified-copy success.")
    check.less_equal(len(process.stdout[:-1].encode("utf-8")), 65_536, msg="Snapshot uses the shared default compact envelope budget.")
    check.less(len(states), len(expected), msg="This concrete large-run control must actually exercise trailing-state omission.")
    check.equal(states, expected[:len(states)], msg="The optional listing preserves deterministic order and every emitted identity intact.")
    check.is_in("byte_limit", [item["code"] for item in result["cutoffs"]], msg="Omitted run states carry an explicit byte-limit cutoff.")
    check.equal(result["data"]["output_path"], str(output), msg="The output file identity is never shortened to fit optional metadata.")
    check.equal(_source_facts(output)["states"], expected, msg="Output trimming never removes actual run rows from the completed SQLite snapshot.")
    check.equal(set(result["data"]), {"output_path", "journal_mode", "verified", "source_run_states"},
                msg="The truncated snapshot response does not fabricate a total or continuation field.")


@pytest.mark.parametrize("kind", ["invalid", "missing", "directory"], ids=["readable-invalid-source", "missing-source", "unopenable-directory"])
def test_snapshot_generic_source_failures_do_not_invent_wal_status(tmp_path, kind):
    source = tmp_path / "bad-source.sqlite"
    if kind == "invalid":
        source.write_bytes(b"readable bytes that are not a SQLite recording")
    elif kind == "directory":
        source.mkdir()
    output = tmp_path / "new-output.sqlite"
    process, result = _command("fr", source, output, tmp_path)
    _refused(process, result, "recording_unreadable", 3)
    for candidate in _paths(output):
        check.is_false(candidate.exists(), msg="Source-open/read failure must not reserve a new destination or sidecar.")


@pytest.mark.skipif(os.name != "posix", reason="POSIX unprivileged lone-WAL snapshot probe")
def test_unprivileged_lone_wal_refusal_and_recovery(closed_recording):
    # A directly created temporary directory has traversable ancestors for the
    # ordinary-user subprocess; pytest's own temporary parents may be private.
    with tempfile.TemporaryDirectory(prefix="flightrec-snapshot-media-") as temporary:
        directory = Path(temporary)
        directory.chmod(0o755)
        stopped = directory / "stopped.sqlite"
        shutil.copyfile(closed_recording, stopped)
        with closing(sqlite3.connect(stopped)) as writer:
            assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal", \
                "The permission control must deliberately retain real WAL mode."
            writer.execute("UPDATE runs SET close_state='open'")
            writer.commit()
            before = _source_facts(stopped)
            rows_before = {table: writer.execute("SELECT * FROM " + table + " ORDER BY rowid").fetchall()
                           for table in before["counts"]}
        assert sorted(path.name for path in directory.iterdir()) == [stopped.name], \
            "All committed rows must be in the stopped main file, without required sidecars."
        source_bytes = stopped.read_bytes()
        readonly = directory / "readonly"
        readonly.mkdir()
        source = readonly / "lone.sqlite"
        shutil.copyfile(stopped, source)
        source.chmod(0o444)
        readonly.chmod(0o555)
        writable = directory / "writable"
        writable.mkdir()
        writable.chmod(0o777)
        output = writable / "snapshot.sqlite"
        fr = str(Path(sys.executable).with_name("fr"))
        probe = '''import json,os,sys
if os.geteuid()==0:
    try:
        os.setgroups([]); os.setgid(65534); os.setuid(65534)
    except PermissionError:
        print(json.dumps({'drop_unavailable':True})); raise SystemExit(0)
assert os.geteuid()!=0, 'The snapshot permission probe must run as an ordinary user.'
import mcp_flightrecorder.cli
os.execv(sys.argv[1],sys.argv[1:])
'''
        try:
            process = subprocess.run([sys.executable, "-I", "-B", "-c", probe, fr,
                                      "snapshot", str(source), str(output)],
                                     cwd=directory, capture_output=True, text=True, timeout=15)
            assert process.stdout, f"The installed command must be readable and executable by the ordinary user: {process.stderr}"
            result = json.loads(process.stdout)
            if result.get("drop_unavailable"):
                pytest.skip("Unprivileged snapshot requires an ordinary user or allowed UID drop")
            assert isinstance(result.get("error"), dict), "Unwritable lone-WAL media must return a structured snapshot refusal."
            assert result["error"]["code"] in {"recording_unreadable", "recording_open_wal"}, \
                "The refusal must describe the real unreadable WAL source, whether or not mode detection succeeds first."
            _refused(process, result, result["error"]["code"], 3)
            check.equal(process.stderr, "", msg="Expected media refusal must not expose a traceback or raw storage diagnostic.")
            check.equal(source.read_bytes(), source_bytes, msg="The refused snapshot must preserve the stopped source bytes.")
            check.equal(sorted(path.name for path in readonly.iterdir()), [source.name],
                        msg="Unwritable original media must remain a lone main file.")
            for candidate in _paths(output):
                check.is_false(candidate.exists(), msg="Unreadable source refusal must leave no output main file or sidecar.")

            # Only a stopped source can be recovered by copying its lone main
            # file to media on which SQLite can create its required sidecars.
            staged = writable / "staged.sqlite"
            shutil.copyfile(stopped, staged)
            staged.chmod(0o444)
            retry = subprocess.run([sys.executable, "-I", "-B", "-c", probe, fr,
                                    "snapshot", str(staged), str(output)],
                                   cwd=directory, capture_output=True, text=True, timeout=15)
            assert retry.returncode == 0, f"The same ordinary user must recover the stopped source on writable media: {retry.stderr}"
            recovered = json.loads(retry.stdout)
            assert recovered["ok"], "Writable-media retry must create a verified portable snapshot."
            check.equal(retry.stderr, "", msg="Successful recovery has no separate error output.")
            check.equal(recovered["data"], {"output_path": str(output.resolve()), "journal_mode": "delete",
                                           "verified": True, "source_run_states": before["states"]},
                        msg="Recovery preserves observed run state while reporting a verified DELETE-mode destination.")
            check.equal(_source_facts(output), {**before, "journal_mode": "delete"},
                        msg="Every retained table population, artifact body and run state must survive ordinary-user recovery.")
            with closing(sqlite3.connect(output.as_uri() + "?mode=ro", uri=True)) as reader:
                check.equal({table: reader.execute("SELECT * FROM " + table + " ORDER BY rowid").fetchall()
                             for table in rows_before}, rows_before,
                            msg="The portable snapshot must preserve every recorded row, not merely table totals.")
            for sidecar in _paths(output)[1:]:
                check.is_false(sidecar.exists(), msg="The recovered portable output must not require any SQLite sidecar.")
            check.equal(staged.read_bytes(), source_bytes, msg="Successful recovery must not rewrite the writable-media source.")
            check.equal(source.read_bytes(), source_bytes, msg="Recovery must leave the original unwritable-media source unchanged.")
        finally:
            readonly.chmod(0o755)


@pytest.mark.skipif(os.name != "posix", reason="POSIX source symlink-loop probe")
def test_source_symlink_loop_is_invalid_path(tmp_path):
    source = tmp_path / "source-loop.sqlite"
    source.symlink_to(source.name)
    output = tmp_path / "uncreated.sqlite"
    process, result = _command("fr", source, output, tmp_path)
    _refused(process, result, "invalid_request", 1, "invalid_path")
    for candidate in _paths(output):
        check.is_false(candidate.exists(), msg="A source symlink loop must be rejected before any destination or sidecar is created.")


@pytest.mark.skipif(os.name != "posix", reason="POSIX two-link symlink-loop probe")
def test_inspection_two_link_symlink_loop_is_invalid_path(tmp_path):
    source = tmp_path / "cycle-a.sqlite"
    other = tmp_path / "cycle-b.sqlite"
    source.symlink_to(other.name)
    other.symlink_to(source.name)
    result = inspect.runs(source)
    assert result["ok"] is False and result["data"] is None, \
        "The public Python reader must reject a cyclic source before storage access."
    assert result["error"]["code"] == "invalid_request"
    assert result["error"]["reason"] == "invalid_path"

    fr = str(Path(sys.executable).with_name("fr"))
    process = subprocess.run([fr, "runs", str(source)], cwd=tmp_path,
                             capture_output=True, text=True, timeout=15)
    assert process.returncode == 1, "The installed CLI must classify the same invalid path."
    assert process.stderr == "", "A normal invalid-path envelope cannot leak a traceback."
    envelope = json.loads(process.stdout)
    assert envelope["ok"] is False and envelope["data"] is None
    assert envelope["error"]["code"] == "invalid_request"
    assert envelope["error"]["reason"] == "invalid_path"
    assert os.readlink(source) == other.name and os.readlink(other) == source.name
    assert sorted(path.name for path in tmp_path.iterdir()) == sorted([source.name, other.name]), \
        "Neither reader may create SQLite files or sidecars for a cyclic path."


@pytest.mark.skipif(os.name != "posix", reason="POSIX destination symlink-loop probe")
def test_snapshot_destination_symlink_loop_is_invalid_path(closed_recording, tmp_path):
    output = tmp_path / "output-loop.sqlite"
    output.symlink_to(output.name)
    before_source = closed_recording.read_bytes()
    before_target = os.readlink(output)
    process, result = _command("fr", closed_recording, output, tmp_path)
    _refused(process, result, "invalid_request", 1, "invalid_path")
    assert output.is_symlink() and os.readlink(output) == before_target, \
        "Validation cannot replace or remove an independently owned symlink."
    assert closed_recording.read_bytes() == before_source
    assert not any(os.path.lexists(str(output) + suffix) for suffix in SUFFIXES[1:]), \
        "An invalid destination must not produce a SQLite sidecar."
    assert not list(tmp_path.glob(".mfr-snapshot-*")), \
        "Invalid destination validation must occur before snapshot staging."


@pytest.mark.parametrize("timeout", ["0", "-1", "nan", "inf", "30.1"],
                         ids=["zero", "negative", "nan", "infinity", "over-thirty"])
def test_snapshot_timeout_arguments_validate_before_storage(tmp_path, timeout):
    output = tmp_path / "uncreated.sqlite"
    process, result = _command("fr", tmp_path / "missing.sqlite", output, tmp_path, "--timeout", timeout)
    _refused(process, result, "invalid_request", 1, "invalid_timeout")
    check.is_false(output.exists(), msg="Invalid timeout configuration cannot create a snapshot destination.")


def test_snapshot_states_come_from_verified_destination_not_changed_source(closed_recording, tmp_path, monkeypatch, capsys):
    output = tmp_path / "state-snapshot.sqlite"
    real_connect = sqlite3.connect
    with closing(real_connect(closed_recording)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("UPDATE runs SET close_state='open'")
        writer.commit()
        original_states = _source_facts(closed_recording)["states"]
        changed = []

        class AfterBackupConnection(sqlite3.Connection):
            def backup(self, target, **options):
                super().backup(target, **options)
                writer.execute("UPDATE runs SET close_state='incomplete'")
                writer.commit()
                changed.append(True)

        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", lambda *args, **kwargs:
                          real_connect(*args, factory=AfterBackupConnection, **kwargs))
            patch.setattr(sys, "argv", ["fr", "snapshot", str(closed_recording), str(output)])
            code = main()
        result = json.loads(capsys.readouterr().out)
        assert result["ok"], "The controlled source change must not prevent a real completed SQLite backup."
        check.equal(code, 0, msg="A source update after copy is compatible with successful snapshot verification.")
        check.equal(changed, [True], msg="The independent writer must actually change source state after native backup completes.")
        check.equal(_source_facts(closed_recording)["states"][0]["close_state"], "incomplete", msg="The live source now carries a different committed state.")
        check.equal(result["data"]["source_run_states"], original_states, msg="Reported states must come from the verified destination snapshot.")
        check.equal(_source_facts(output)["states"], original_states, msg="Independent destination reads confirm the report's copied open state.")


def test_snapshot_native_busy_backup_deadline_cleans_owned_output_and_retries(closed_recording, tmp_path):
    output = tmp_path / "bounded-busy.sqlite"
    unrelated = tmp_path / "unrelated.sqlite-wal"
    unrelated.write_bytes(b"preserve unrelated owner")
    before = closed_recording.read_bytes()
    # The control inserts a real destination lock at the native backup boundary; the actual backup/progress loop enforces time.
    probe = '''from contextlib import closing
import sqlite3,sys
from mcp_flightrecorder.cli import main
source,destination=sys.argv[1:3]
real=sqlite3.connect
class LockedBackup(sqlite3.Connection):
 def backup(self,target,**options):
  # The native target is isolated under a private staging directory.
  staged=target.execute("PRAGMA database_list").fetchone()[2]
  with closing(real(staged,timeout=0.1)) as blocker:
   blocker.execute("BEGIN EXCLUSIVE")
   blocker.execute("CREATE TABLE owned_partial(value)")
   try:
    return super().backup(target,**options)
   finally:
    blocker.rollback()
def connect(*args,**kwargs):
 return real(*args,factory=LockedBackup,**kwargs)
sqlite3.connect=connect
sys.argv=["fr","snapshot",source,destination,"--timeout","0.05"]
raise SystemExit(main())
'''
    process = subprocess.run([sys.executable, "-I", "-B", "-c", probe, str(closed_recording), str(output)],
                             cwd=tmp_path, capture_output=True, text=True, timeout=15)
    assert process.stdout, "The real busy-backup loop must terminate under the subprocess watchdog and emit its timeout."
    result = json.loads(process.stdout)
    _refused(process, result, "query_timeout", 1, "deadline_expired")
    for candidate in _paths(output):
        check.is_false(candidate.exists(), msg="Deadline failure closes resources then removes only this attempt's owned partial output files.")
    check.equal(unrelated.read_bytes(), b"preserve unrelated owner", msg="Backup cleanup never touches a different destination's sidecars.")
    check.equal(closed_recording.read_bytes(), before, msg="A timed-out busy backup leaves source bytes unchanged.")
    retry, copied = _command("fr", closed_recording, output, tmp_path)
    assert copied["ok"], "The cleaned output identity must be reusable by an ordinary installed snapshot command."
    check.equal(retry.returncode, 0, msg="Retry succeeds after the independently held destination lock is released.")


@pytest.mark.parametrize("failure", ["backup-storage", "verification-storage", "keyboard", "system-exit", "cleanup-storage"])
def test_snapshot_failure_closes_owners_before_removing_only_owned_files(closed_recording, tmp_path, monkeypatch, capsys, failure):
    output = tmp_path / "attempt.sqlite"
    unrelated = tmp_path / "unrelated.sqlite-journal"
    unrelated.write_bytes(b"preexisting independent bytes")
    source_before = closed_recording.read_bytes()
    import mcp_flightrecorder._snapshot as snapshot_module
    real_connect = sqlite3.connect
    real_cleanup = snapshot_module.tempfile.TemporaryDirectory.cleanup
    owners, cleanup_observations, orphan_stages = [], [], []

    class FaultConnection(sqlite3.Connection):
        def close(self):
            self.closed = True
            return super().close()

        def execute(self, sql, parameters=()):
            if (failure == "verification-storage" and self.destination and self.readonly
                    and sql.startswith("SELECT run_id,close_state")):
                raise sqlite3.OperationalError("controlled fresh destination read failure")
            return super().execute(sql, parameters)

        def backup(self, target, **options):
            if failure == "verification-storage":
                return super().backup(target, **options)
            target.execute("CREATE TABLE owned_partial(value)")
            target.commit()
            # Only the private SQLite staging directory belongs to this
            # attempt. Public destination suffixes never acquire ownership.
            staged = Path(target.execute("PRAGMA database_list").fetchone()[2])
            for suffix in ("-wal", "-shm", "-journal"):
                Path(str(staged) + suffix).write_bytes(b"attempt-owned private partial sidecar")
            if failure == "keyboard":
                raise KeyboardInterrupt()
            if failure == "system-exit":
                raise SystemExit(17)
            raise sqlite3.OperationalError("controlled backup storage failure")

    def connect(database, **options):
        connection = real_connect(database, factory=FaultConnection, **options)
        connection.closed = False
        connection.destination = ".mfr-snapshot-" in str(database)
        connection.readonly = "mode=ro" in str(database)
        owners.append(connection)
        return connection

    def cleanup(directory):
        cleanup_observations.append(all(owner.closed for owner in owners))
        if failure == "cleanup-storage":
            orphan_stages.append(Path(directory.name))
            raise PermissionError("controlled inability to remove private staged output")
        return real_cleanup(directory)

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", connect)
        patch.setattr(snapshot_module.tempfile.TemporaryDirectory, "cleanup", cleanup)
        patch.setattr(sys, "argv", ["fr", "snapshot", str(closed_recording), str(output)])
        if failure == "system-exit":
            # SystemExit remains caller control; its finalizer must still close/remove owned resources.
            with pytest.raises(SystemExit) as raised:
                main()
            check.equal(raised.value.code, 17, msg="Snapshot cannot replace the caller's explicit process exit.")
            code = 17
        else:
            code = main()
    captured = capsys.readouterr()
    assert owners, "Each fault must reach actual SQLite source/destination ownership before cleanup is assessed."
    check.is_true(all(owner.closed for owner in owners), msg="Every actual source and destination connection closes on all controlled exit paths.")
    check.is_true(bool(cleanup_observations) and all(cleanup_observations),
                  msg="Private stage cleanup starts only after every SQLite connection has closed.")
    if failure in {"keyboard", "system-exit"}:
        check.equal(code, 130 if failure == "keyboard" else 17, msg="CLI interruption preserves the assigned cancellation/process-exit behavior.")
        check.equal(captured.out, "", msg="Host interruption emits no success JSON or fabricated completed result.")
    else:
        result = json.loads(captured.out)
        check.equal(code, 3, msg="Controlled storage and cleanup failures use the storage failure exit.")
        check.equal(result["error"]["code"], "recording_unreadable", msg="Failure of copy/verification/cleanup is a storage error.")
        check.equal(result["error"]["reason"], "output_cleanup_failed" if failure == "cleanup-storage" else "storage_failure",
                    msg="An inability to remove owned partial output is explicitly reported instead of concealed.")
    for candidate in _paths(output):
        check.is_false(candidate.exists(),
                       msg="Failed copy never creates or removes any public destination pathname.")
    stages = list(tmp_path.glob(".mfr-snapshot-*"))
    check.is_true(len(stages) <= (1 if failure == "cleanup-storage" else 0),
                  msg="Only a simulated private-stage cleanup failure may leave an owned stage behind.")
    check.equal(len(orphan_stages), 1 if failure == "cleanup-storage" else 0,
                msg="The private-stage cleanup failure hook must actually execute once.")
    for directory in orphan_stages:
        if directory.exists():
            shutil.rmtree(directory)
    check.equal(unrelated.read_bytes(), b"preexisting independent bytes", msg="No failure path removes an unrelated preexisting sidecar.")
    check.equal(closed_recording.read_bytes(), source_before, msg="Source evidence survives backup failure, verification failure and interruption unchanged.")
    check.is_true(inspect.runs(closed_recording)["ok"], msg="Every failed or interrupted snapshot leaves its source usable for another reader.")


@pytest.mark.parametrize("mode,established,expected", [("wal", True, "recording_open_wal"),
                                                        ("wal", False, "recording_unreadable"),
                                                        ("delete", True, "recording_unreadable")],
                         ids=["established-related-wal-failure", "wal-not-established", "delete-source-related-error"])
def test_snapshot_wal_error_requires_observed_mode_and_related_failure(closed_recording, tmp_path, monkeypatch, capsys,
                                                                      mode, established, expected):
    output = tmp_path / "wal-failure-output.sqlite"
    real_connect = sqlite3.connect
    with closing(real_connect(closed_recording)) as holder:
        holder.execute("PRAGMA journal_mode=" + mode)
        holder.commit()

        class WalFault(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                trigger = "SELECT name FROM sqlite_master LIMIT 1" if established else "PRAGMA journal_mode"
                if sql == trigger:
                    error = sqlite3.OperationalError("controlled inaccessible WAL condition")
                    error.sqlite_errorname = "SQLITE_CANTOPEN_DIRTYWAL"
                    error.sqlite_errorcode = 1294
                    raise error
                return super().execute(sql, parameters)

        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", lambda *args, **kwargs: real_connect(*args, factory=WalFault, **kwargs))
            patch.setattr(sys, "argv", ["fr", "snapshot", str(closed_recording), str(output)])
            code = main()
        result = json.loads(capsys.readouterr().out)
        check.equal(code, 3, msg="Observed WAL-specific failure and generic storage failure both use the pinned storage exit.")
        check.equal(result["error"]["code"], expected, msg="Only established WAL mode plus a related read failure warrants recording_open_wal.")
        check.is_false(output.exists(), msg="A source WAL availability failure cannot create a purported snapshot.")


def test_snapshot_exclusive_publication_refuses_racing_owner(closed_recording, tmp_path, monkeypatch, capsys):
    """An independent O_EXCL competitor wins immediately before the hard link."""
    import mcp_flightrecorder._snapshot as snapshot_module
    output = tmp_path / "race.sqlite"
    real_link = os.link
    reached = []
    source_bytes = closed_recording.read_bytes()

    def racing_link(staged, destination, *args, **kwargs):
        assert Path(destination) == output
        assert not output.exists(), "a backup in private staging has no public main placeholder"
        with output.open("xb") as competitor:
            competitor.write(b"independent racing owner")
        reached.append(True)
        return real_link(staged, destination, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(snapshot_module.os, "link", racing_link)
        patch.setattr(sys, "argv", ["fr", "snapshot", str(closed_recording), str(output)])
        code = main()
    result = json.loads(capsys.readouterr().out)
    check.equal(reached, [True], msg="The rival must acquire the main name at the final publication boundary.")
    check.equal(code, 1, msg="A publication race uses the normal exclusive-destination refusal.")
    check.equal(result["error"]["reason"], "destination_exists")
    check.equal(output.read_bytes(), b"independent racing owner",
                msg="The rejected link never overwrites or later removes the competitor's bytes.")
    check.equal(closed_recording.read_bytes(), source_bytes)
    check.is_false(any(tmp_path.glob(".mfr-snapshot-*")))


@pytest.mark.parametrize("failure", ["storage", "keyboard", "system-exit"])
def test_failed_private_backup_never_removes_competing_main(
        closed_recording, tmp_path, monkeypatch, failure):
    """No public placeholder exists at failure or interrupt cleanup boundaries."""
    import mcp_flightrecorder._snapshot as snapshot_module

    output = tmp_path / "foreign-main.sqlite"
    source_before = closed_recording.read_bytes()
    foreign = ("COMPETITOR-MAIN-" + failure).encode()
    entered, written = threading.Event(), threading.Event()
    errors = []

    def competitor():
        try:
            assert entered.wait(3)
            with output.open("xb") as writer:
                writer.write(foreign)
        except BaseException as error:
            errors.append(error)
        finally:
            written.set()

    def fail_private_backup(ctx, staged):
        assert not os.path.lexists(output), "the output name is never reserved during backup"
        assert staged.parent != output.parent
        entered.set()
        assert written.wait(3)
        assert not errors, errors
        if failure == "keyboard":
            raise KeyboardInterrupt()
        if failure == "system-exit":
            raise SystemExit(17)
        raise sqlite3.OperationalError("controlled backup failure")

    monkeypatch.setattr(snapshot_module, "_backup", fail_private_backup)
    actor = threading.Thread(target=competitor, daemon=True)
    actor.start()
    try:
        if failure != "storage":
            with pytest.raises(KeyboardInterrupt if failure == "keyboard" else SystemExit) as raised:
                snapshot_module.snapshot(closed_recording, output, timeout=3)
            if failure == "system-exit":
                assert raised.value.code == 17
        else:
            result = snapshot_module.snapshot(closed_recording, output, timeout=3)
            assert not result["ok"]
            assert (result["error"]["code"], result["error"]["reason"]) == (
                "recording_unreadable", "storage_failure")
    finally:
        actor.join(timeout=3)
    assert not actor.is_alive() and not errors
    assert output.read_bytes() == foreign, "failure may not unlink a competitor's public main"
    assert closed_recording.read_bytes() == source_before
    assert inspect.runs(closed_recording)["ok"]
    assert not list(tmp_path.glob(".mfr-snapshot-*"))


def test_ambiguous_link_result_keeps_verified_published_file(
        closed_recording, tmp_path, monkeypatch):
    """A late OS publication error cannot license cleanup of a public name."""
    import mcp_flightrecorder._snapshot as snapshot_module

    output = tmp_path / "published-but-uncertain.sqlite"
    real_link = os.link
    source_before = closed_recording.read_bytes()
    called = []

    def publish_then_interrupt(staged, destination):
        assert Path(destination) == output
        real_link(staged, destination)
        called.append(True)
        raise InterruptedError("controlled uncertain native link result")

    monkeypatch.setattr(snapshot_module.os, "link", publish_then_interrupt)
    result = snapshot_module.snapshot(closed_recording, output, timeout=3)
    assert called == [True]
    assert not result["ok"]
    assert (result["error"]["code"], result["error"]["reason"]) == (
        "recording_unreadable", "storage_failure")
    assert "could not be confirmed" in result["error"]["message"]
    assert inspect.runs(output)["ok"], "the linked copy cannot be removed after an uncertain result"
    assert _source_facts(output) == _source_facts(closed_recording)
    assert closed_recording.read_bytes() == source_before
    assert not list(tmp_path.glob(".mfr-snapshot-*"))


def test_post_publication_replacement_survives_private_cleanup_failure(
        closed_recording, tmp_path, monkeypatch):
    """A competing replacement after link must never become a cleanup target."""
    import mcp_flightrecorder._snapshot as snapshot_module

    output = tmp_path / "late-replaced-main.sqlite"
    moved = tmp_path / "original-published-inode.sqlite"
    foreign_bytes = b"FOREIGN-OWNER-AFTER-PUBLICATION"
    original_source = closed_recording.read_bytes()
    cleanup_called = []
    stage_paths = []

    def replace_and_fail_private_cleanup(directory):
        cleanup_called.append(True)
        stage_paths.append(Path(directory.name))
        assert output.is_file(), "the stage finalizer must run after publication in this scenario"
        output.rename(moved)
        with output.open("xb") as rival:
            rival.write(foreign_bytes)
        raise PermissionError("controlled private cleanup failure")

    monkeypatch.setattr(snapshot_module.tempfile.TemporaryDirectory,
                        "cleanup", replace_and_fail_private_cleanup)
    result = snapshot_module.snapshot(closed_recording, output, timeout=3)
    assert not result["ok"]
    assert (result["error"]["code"], result["error"]["reason"]) == (
        "recording_unreadable", "output_cleanup_failed")
    assert cleanup_called == [True]
    assert output.read_bytes() == foreign_bytes, "late foreign replacement must never be unlinked"
    assert inspect.runs(moved)["ok"], "the original published main inode remains a valid copy"
    assert closed_recording.read_bytes() == original_source
    for path in stage_paths:
        if path.exists():
            shutil.rmtree(path)


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
@pytest.mark.parametrize("failure", ["storage", "keyboard", "system-exit"])
def test_after_reservation_foreign_sidecar_survives_failure_and_interrupt(
        closed_recording, tmp_path, monkeypatch, suffix, failure):
    """A competing writer, not SQLite, creates a suffix after exclusive reserve."""
    import threading
    import mcp_flightrecorder._snapshot as snapshot_module

    output = tmp_path / "after-reservation.sqlite"
    foreign = Path(str(output) + suffix)
    marker = ("INDEPENDENT-OWNER:" + suffix + ":" + failure).encode()
    source_bytes = closed_recording.read_bytes()
    entered, published = threading.Event(), threading.Event()
    worker_errors = []

    def rival():
        try:
            assert entered.wait(3), "backup hook must execute after main reservation"
            with foreign.open("xb") as stream:
                stream.write(marker)
            published.set()
        except BaseException as error:
            worker_errors.append(error)
            published.set()

    def fail_backup(ctx, staged):
        assert not os.path.lexists(output), "backup must finish before the public main name is created"
        assert staged.parent != output.parent, "SQLite backup belongs in a private directory"
        entered.set()
        assert published.wait(3), "the independent writer must finish before cleanup"
        assert not worker_errors, worker_errors
        if failure == "keyboard":
            raise KeyboardInterrupt()
        if failure == "system-exit":
            raise SystemExit(17)
        raise sqlite3.OperationalError("controlled native backup failure")

    monkeypatch.setattr(snapshot_module, "_backup", fail_backup)
    actor = threading.Thread(target=rival, daemon=True)
    actor.start()
    try:
        if failure in {"keyboard", "system-exit"}:
            expected = KeyboardInterrupt if failure == "keyboard" else SystemExit
            with pytest.raises(expected) as caught:
                snapshot_module.snapshot(closed_recording, output, timeout=3)
            if failure == "system-exit":
                assert caught.value.code == 17, "cleanup must not mask the caller's exit"
        else:
            result = snapshot_module.snapshot(closed_recording, output, timeout=3)
            assert result["ok"] is False
            assert result["error"]["code"] == "recording_unreadable"
            assert result["error"]["reason"] == "storage_failure"
    finally:
        actor.join(timeout=3)
    assert not actor.is_alive() and not worker_errors
    assert foreign.read_bytes() == marker, "an unowned public suffix must never be deleted"
    assert not output.exists(), "failure never publishes a public main file"
    assert closed_recording.read_bytes() == source_bytes, "the source main is immutable"
    assert inspect.runs(closed_recording)["ok"], "the source remains readable"
    assert not list(tmp_path.glob(".mfr-snapshot-*")), "private staging must be cleaned"


def test_failed_backup_cleans_real_sqlite_sidecars_only_inside_owned_stage(
        closed_recording, tmp_path, monkeypatch):
    """The attempt can safely remove SQLite-created sidecars in its private dir."""
    import mcp_flightrecorder._snapshot as snapshot_module

    output = tmp_path / "private-stage.sqlite"
    source_bytes = closed_recording.read_bytes()
    created = []

    def fail_backup(ctx, staged):
        with closing(sqlite3.connect(staged)) as target:
            assert target.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
            target.execute("CREATE TABLE attempt_owned(value)")
            target.execute("INSERT INTO attempt_owned VALUES(1)")
            target.commit()
            assert Path(str(staged) + "-wal").exists(), "real SQLite must create its own WAL"
            assert Path(str(staged) + "-shm").exists(), "real SQLite must create its own SHM"
            created.extend([str(staged) + "-wal", str(staged) + "-shm"])
            raise sqlite3.OperationalError("staged native copy stopped")

    monkeypatch.setattr(snapshot_module, "_backup", fail_backup)
    result = snapshot_module.snapshot(closed_recording, output, timeout=3)
    assert result["error"]["code"] == "recording_unreadable"
    assert len(created) == 2 and all(not Path(path).exists() for path in created)
    assert not output.exists() and all(not item.exists() for item in _paths(output))
    assert not list(tmp_path.glob(".mfr-snapshot-*"))
    assert closed_recording.read_bytes() == source_bytes
    assert inspect.runs(closed_recording)["ok"]


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_after_reservation_foreign_sidecar_forces_safe_publish_refusal(
        closed_recording, tmp_path, monkeypatch, suffix):
    """A successful private backup cannot publish beside another owner's suffix."""
    import mcp_flightrecorder._snapshot as snapshot_module

    output = tmp_path / "publish-race.sqlite"
    sidecar = Path(str(output) + suffix)
    marker = ("FOREIGN-PUBLISH-SIDECAR:" + suffix).encode()
    original_backup = snapshot_module._backup
    source_before = closed_recording.read_bytes()

    def backup_then_compete(ctx, staged):
        original_backup(ctx, staged)
        assert not os.path.lexists(output), "a completed private backup still has no public main"
        with sidecar.open("xb") as writer:
            writer.write(marker)

    monkeypatch.setattr(snapshot_module, "_backup", backup_then_compete)
    result = snapshot_module.snapshot(closed_recording, output, timeout=3)
    assert not result["ok"]
    assert (result["error"]["code"], result["error"]["reason"]) == (
        "recording_unreadable", "destination_finalize")
    assert sidecar.read_bytes() == marker
    assert not output.exists(), "late sidecar collision is detected before any public publication"
    assert closed_recording.read_bytes() == source_before
    assert not list(tmp_path.glob(".mfr-snapshot-*"))


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_preexisting_dangling_destination_sidecar_symlink_is_never_claimed(
        closed_recording, tmp_path, suffix):
    output = tmp_path / "sidecar-link.sqlite"
    sidecar = Path(str(output) + suffix)
    sidecar.symlink_to("nonexistent-independent-target")
    source_before = closed_recording.read_bytes()
    from mcp_flightrecorder._snapshot import snapshot
    result = snapshot(closed_recording, output, timeout=3)
    assert not result["ok"]
    assert (result["error"]["code"], result["error"]["reason"]) == (
        "invalid_request", "destination_exists")
    assert sidecar.is_symlink() and sidecar.readlink() == Path("nonexistent-independent-target")
    assert not output.exists() and closed_recording.read_bytes() == source_before
