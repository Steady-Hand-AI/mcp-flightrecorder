"""Installed stdio bridge byte, correlation, failure and lifecycle controls."""
from contextlib import closing, contextmanager
import json
import os
from pathlib import Path
import signal
import select
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

from mcp_flightrecorder import inspect


PEER = Path(__file__).resolve().parent / "producers/proxy_peer.py"
FR = str(Path(sys.executable).with_name("fr"))
pytestmark = pytest.mark.skipif(os.name != "posix", reason="The proxy qualifies POSIX pipe/process ownership.")


def wire(value):
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def request(identity, name="answer", arguments=None, method="tools/call"):
    return {"jsonrpc": "2.0", "id": identity, "method": method,
            "params": {"name": name, "arguments": arguments or {}}}


def answer(identity, text="42", *, error=False):
    return {"jsonrpc": "2.0", "id": identity,
            "result": {"content": [{"type": "text", "text": text}], "isError": error}}


def fixture(tmp_path, response, *options):
    tmp_path.mkdir(exist_ok=True)
    reply = tmp_path / "native response.bin"
    observed = tmp_path / "observed.bin"
    pid = tmp_path / "child.pid"
    reply.write_bytes(response)
    argv = [sys.executable, "-I", "-B", str(PEER), "peer", "--response", str(reply),
            "--observed", str(observed), "--pid", str(pid), *map(str, options)]
    return argv, observed, pid


def proxy(argv, recording=None, *, capture="content", fault=None, evidence=None, release=None,
          require_recording=False):
    args = ["proxy"]
    if recording is not None:
        args += ["--record", str(recording), "--capture", capture]
    if require_recording:
        args += ["--require-recording"]
    args += ["--", *argv]
    if fault:
        prefix = [sys.executable, "-I", "-B", str(PEER), "bootstrap", "--fault", fault,
                  "--evidence", str(evidence)]
        if release is not None:
            prefix += ["--release", str(release)]
        return [*prefix, "--", *args]
    return [FR, *args]


def run(argv, data, cwd, *, env=None):
    with launched(argv, cwd, env=env) as process:
        stdout, stderr = process.communicate(data, timeout=12)
    return subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)


@contextmanager
def launched(argv, cwd, *, env=None, stderr=subprocess.PIPE):
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=stderr)
    failed = True
    try:
        yield process
        failed = False
    finally:
        forced = process.poll() is None
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        # A failing lifecycle assertion must not leave its own fixture child.
        if "--pid" in argv:
            pidfile = Path(argv[argv.index("--pid") + 1])
            if pidfile.exists():
                try:
                    child_pid = int(pidfile.read_text())
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    pass
                else:
                    try:
                        os.kill(child_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    assert failed or forced, "The completed proxy left its owned fixture child alive."
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass


def read_bytes(stream, count=None, timeout=8):
    deadline = time.monotonic() + timeout
    chunks, size = [], 0
    while count is None or size < count:
        remaining = deadline - time.monotonic()
        assert remaining > 0, "Native output did not arrive within the fixture deadline."
        ready, _, _ = select.select([stream], [], [], remaining)
        assert ready, "Native output did not arrive within the fixture deadline."
        block = os.read(stream.fileno(), min(1024, count - size) if count is not None else 1024)
        if not block:
            break
        chunks.append(block)
        size += len(block)
    return b"".join(chunks)


def rows(path, sql):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(sql)]


def operations(path):
    return rows(path, """SELECT o.*, a.body_text AS result_body FROM operations o
        LEFT JOIN events e ON e.event_id=o.end_event_id
        LEFT JOIN artifacts a ON a.artifact_id=e.payload_ref ORDER BY o.started_at,o.operation_id""")


def has_operation(path):
    try:
        return path.is_file() and bool(operations(path))
    except sqlite3.Error:
        return False


def wait_for(predicate, message, timeout=5):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, message
        time.sleep(0.01)


def gone(pidfile):
    assert pidfile.exists(), "The actual child must have started."
    pid = int(pidfile.read_text())
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def assert_portable(path):
    assert rows(path, "PRAGMA journal_mode") == [{"journal_mode": "delete"}]
    assert not any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-shm", "-journal"))


def test_installed_forwarding_preserves_opaque_bytes_argv_cwd_and_environment(tmp_path):
    data = b'  {"jsonrpc":"2.0", "id":"1", "method":"opaque", "x":"caf\xc3\xa9"}\n\n' + b'not JSON\n'
    response = b'\t{"jsonrpc":"2.0","id":"1", "error":{"code":-3,"message":"opaque"},"x": [1, 2]} \n'
    identity = tmp_path / "identity.json"
    label = "literal ; dollar $ and spaces 🌱"
    child, observed, pid = fixture(tmp_path, response, "--identity", identity, "--label", label)
    environment = {**os.environ, "PROXY_PEER_LABEL": "inherited fixture value"}
    result = run(proxy(child), data, tmp_path, env=environment)
    assert (result.returncode, result.stdout, result.stderr) == (0, response, b"")
    assert observed.read_bytes() == data and gone(pid)
    assert json.loads(identity.read_text()) == {"cwd": str(tmp_path.resolve()), "label": label,
                                                "environment": "inherited fixture value"}
    assert not list(tmp_path.glob("*.sqlite*")), "Forwarding-only mode owns no recording."


def test_pipeline_numeric_string_ids_and_out_of_order_results_are_distinct(tmp_path):
    calls = [request(1, "numeric", {"value": 1}), request("1", "string", {"value": 2}),
             request(2, "failure"), {"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}}]
    replies = [answer("1", "string result", error=True), {"jsonrpc": "2.0", "id": 3, "result": {"tools": []}},
               {"jsonrpc": "2.0", "id": 2, "error": {"code": -32602, "message": "native error", "data": {"opaque": 4}}},
               answer(1, "numeric result")]
    data, response = b"".join(map(wire, calls)), b"".join(map(wire, replies))
    child, observed, pid = fixture(tmp_path, response)
    path = tmp_path / "capture.sqlite"
    result = run(proxy(child, path), data, tmp_path)
    assert result.returncode == 0 and result.stdout == response and observed.read_bytes() == data
    assert gone(pid)
    stored = {row["name"]: row for row in operations(path)}
    assert len(stored) == 4
    assert stored["numeric"]["status"] == "ok"
    assert json.loads(stored["numeric"]["result_body"]) == replies[-1]["result"]
    assert stored["string"]["status"] == "tool_error"
    assert json.loads(stored["string"]["result_body"]) == replies[0]["result"]
    assert stored["failure"]["status"] == "protocol_error" and stored["failure"]["error_code"] == -32602
    assert json.loads(stored["failure"]["result_body"]) == {"error": replies[2]["error"]}
    assert stored[None]["method"] == "tools/list" and stored[None]["status"] == "ok"
    assert all(row[key] is None for row in stored.values()
               for key in ("turn_id", "model_call_id", "decision_id", "dispatch_event_id"))
    assert all(row["link_basis"] == "none" and row["resolution"] == "unknown" for row in stored.values())
    assert rows(path, "SELECT count(*) AS n FROM turns") == [{"n": 0}]
    coverage = json.loads(rows(path, "SELECT coverage_json FROM runs")[0]["coverage_json"])
    assert coverage == {"producers": {"model": None, "host": None, "mcp": "mfr.proxy"}, "model_complete": False}
    assert inspect.check(path, stored["numeric"]["run_id"], expect=["mcp"])["data"]["status"] == "pass"
    assert_portable(path)


@pytest.mark.parametrize("capture", ["metadata", "content"])
def test_retention_default_and_explicit_content_do_not_change_wire(tmp_path, capture):
    marker = "body-only 🌱 marker"
    data, response = wire(request(1, arguments={"text": marker})), wire(answer(1, marker))
    child, observed, _ = fixture(tmp_path, response)
    path = tmp_path / "capture.sqlite"
    argv = ([FR, "proxy", "--record", str(path), "--", *child] if capture == "metadata"
            else proxy(child, path, capture=capture))
    result = run(argv, data, tmp_path)
    assert result.returncode == 0 and result.stdout == response and observed.read_bytes() == data
    assert len(operations(path)) == 1 and operations(path)[0]["status"] == "ok"
    bodies = rows(path, "SELECT body_text,omitted_reason FROM artifacts")
    if capture == "metadata":
        assert bodies and all(row["body_text"] is None for row in bodies)
        assert all(row["omitted_reason"] for row in bodies)
    else:
        assert any(marker in (row["body_text"] or "") for row in bodies)
    assert_portable(path)


@pytest.mark.parametrize("fault", ["start-rejected", "capacity", "writer-failed", "init-failed"])
@pytest.mark.parametrize("exit_code", [0, 7])
def test_real_installed_capture_failure_preserves_native_result_count_and_exit(tmp_path, fault, exit_code):
    data = wire(request(1)) + wire(request(2, "error"))
    response = wire(answer(2, "native failure", error=True)) + wire(answer(1))
    direct, observed_direct, _ = fixture(tmp_path / "direct", response, "--exit-code", exit_code)
    expected = run(direct, data, tmp_path)
    child, observed, pid = fixture(tmp_path / "proxied", response, "--exit-code", exit_code)
    evidence = tmp_path / "fault.jsonl"
    result = run(proxy(child, tmp_path / "capture.sqlite", fault=fault, evidence=evidence), data, tmp_path)
    assert (result.returncode, result.stdout) == (expected.returncode, expected.stdout) == (exit_code, response)
    assert observed.read_bytes() == observed_direct.read_bytes() == data and gone(pid)
    proof = [json.loads(line) for line in evidence.read_text().splitlines()]
    assert "site-packages" in proof[0]["installed_origin"]
    assert b"mfr.proxy:" in result.stderr and len(result.stderr) < 8192
    if fault in {"start-rejected", "capacity"}:
        starts = [row for row in proof if row["kind"] == "start"]
        assert len(starts) == 2 and all(row["status"] == "rejected" and not row["ticket"] for row in starts)
        assert all(row["reason"] == "invalid_input" for row in starts) if fault == "start-rejected" else all(row["reason"] for row in starts)
    elif fault == "writer-failed":
        assert any(row["kind"] == "injected-storage-error" for row in proof)
        assert any(row["kind"] == "close" and row["health"]["writer_errors"] > 0 for row in proof)
    else:
        assert any(row["kind"] == "injected-initialization-error" for row in proof)


def test_existing_destination_is_preserved_while_native_child_runs(tmp_path):
    path = tmp_path / "existing.sqlite"
    path.write_bytes(b"preserved existing file")
    data, response = wire(request(1)), wire(answer(1))
    child, observed, _ = fixture(tmp_path, response)
    result = run(proxy(child, path), data, tmp_path)
    assert result.returncode == 0 and result.stdout == response and observed.read_bytes() == data
    assert path.read_bytes() == b"preserved existing file"
    assert b"mfr.proxy:" in result.stderr


@pytest.mark.parametrize('existing', [False, True])
def test_directory_recordings_are_fresh_and_announced_after_initialization(tmp_path, existing):
    directory = tmp_path / 'recordings with\ncontrols'
    if existing:
        directory.mkdir(mode=0o750)
    destination = directory if existing else str(directory) + '/'
    data, response = wire(request(1)), wire(answer(1))
    paths = []
    for index in range(2):
        child, observed, pid = fixture(tmp_path / str(index), response)
        result = run(proxy(child, destination), data, tmp_path)
        assert result.returncode == 0 and result.stdout == response and observed.read_bytes() == data
        assert gone(pid)
        lines = result.stderr.decode('ascii').splitlines()
        announced = [line.removeprefix('mfr.proxy: recording_path: ') for line in lines
                     if line.startswith('mfr.proxy: recording_path: ')]
        assert len(announced) == 1
        path = Path(json.loads(announced[0]))
        assert path.is_absolute() and path.parent == directory and path.suffix == '.sqlite'
        assert len(operations(path)) == 1 and operations(path)[0]['status'] == 'ok'
        assert_portable(path)
        paths.append(path)
    assert paths[0] != paths[1] and len(list(directory.iterdir())) == 2
    assert directory.stat().st_mode & 0o777 == (0o750 if existing else 0o700)


@pytest.mark.parametrize('strict', [False, True])
def test_initialization_failure_is_real_and_strict_mode_launches_no_child(tmp_path, strict):
    child, observed, pid = fixture(tmp_path, wire(answer(1)))
    proof = tmp_path / 'initialization.jsonl'
    result = run(proxy(child, tmp_path / 'failed.sqlite', fault='init-failed',
                       evidence=proof, require_recording=strict), wire(request(1)), tmp_path)
    facts = [json.loads(line) for line in proof.read_text().splitlines()]
    assert any(item['kind'] == 'injected-initialization-error' for item in facts)
    assert b'recording_unavailable' in result.stderr and b'recording_path:' not in result.stderr
    if strict:
        assert result.returncode == 1 and result.stdout == b''
        assert not pid.exists() and not observed.exists()
    else:
        assert result.returncode == 0 and result.stdout == wire(answer(1))
        assert observed.read_bytes() == wire(request(1)) and gone(pid)


@pytest.mark.parametrize('suffix', ['', '-wal', '-shm', '-journal'])
@pytest.mark.parametrize('strict', [False, True])
def test_file_and_sidecar_conflicts_are_actionable_and_preserved(tmp_path, suffix, strict):
    path = tmp_path / 'conflict.sqlite'
    conflict = Path(str(path) + suffix)
    conflict.write_bytes(b'preserve this exact file')
    child, observed, pid = fixture(tmp_path, wire(answer(1)))
    result = run(proxy(child, path, require_recording=strict), wire(request(1)), tmp_path)
    assert conflict.read_bytes() == b'preserve this exact file'
    assert b'file or SQLite sidecar' in result.stderr and b'fresh file or a directory' in result.stderr
    assert b'recording_path:' not in result.stderr
    if strict:
        assert result.returncode == 1 and not pid.exists() and not observed.exists()
    else:
        assert result.returncode == 0 and result.stdout == wire(answer(1)) and gone(pid)


def test_require_recording_without_destination_is_usage_error(tmp_path):
    child, observed, pid = fixture(tmp_path, b'never launched')
    result = run(proxy(child, require_recording=True), b'', tmp_path)
    assert result.returncode == 2 and result.stdout == b'' and not pid.exists()
    assert b'--require-recording requires --record' in result.stderr


def test_strict_startup_does_not_promise_later_capture_acceptance(tmp_path):
    child, observed, pid = fixture(tmp_path, wire(answer(1)))
    proof = tmp_path / 'rejected.jsonl'
    result = run(proxy(child, tmp_path / 'capture.sqlite', fault='start-rejected',
                       evidence=proof, require_recording=True), wire(request(1)), tmp_path)
    assert result.returncode == 0 and result.stdout == wire(answer(1)) and gone(pid)
    assert observed.read_bytes() == wire(request(1))
    facts = [json.loads(line) for line in proof.read_text().splitlines()]
    assert any(item['kind'] == 'start' and item['status'] == 'rejected' for item in facts)
    assert b'recording_path:' in result.stderr and b'start_rejected' in result.stderr


@pytest.mark.parametrize('strict,sent,expected', [(False, None, 0), (True, None, 0),
                                                (True, signal.SIGINT, 130),
                                                (True, signal.SIGTERM, 143)])
def test_startup_readiness_precedes_child_only_when_required(tmp_path, strict, sent, expected):
    child, observed, pid = fixture(tmp_path, wire(answer(1)))
    held, release, ready, launched_at = [tmp_path / name for name in
                                        ('held', 'release', 'initialized', 'launch.json')]
    bootstrap = tmp_path / 'startup.py'
    bootstrap.write_text('''import json, pathlib, subprocess, sys, time
from mcp_flightrecorder import Recorder
held, release, ready, launched_at = map(pathlib.Path, sys.argv[1:5])
initialize, popen = Recorder.__init__, subprocess.Popen
def init(self, *args, **kwargs):
    held.write_text('waiting')
    deadline = time.monotonic() + 10
    while not release.exists():
        if time.monotonic() > deadline:
            raise TimeoutError('initialization fixture deadline')
        time.sleep(0.01)
    initialize(self, *args, **kwargs)
    ready.write_text('actual initialization completed')
def launch(*args, **kwargs):
    launched_at.write_text(json.dumps({'initialized': ready.exists()}))
    return popen(*args, **kwargs)
Recorder.__init__, subprocess.Popen = init, launch
sys.argv = ['fr', *sys.argv[5:]]
from mcp_flightrecorder.cli import main
raise SystemExit(main())
''')
    command = [sys.executable, '-I', '-B', str(bootstrap), *map(str, (held, release, ready, launched_at)),
               *proxy(child, tmp_path / 'capture.sqlite', require_recording=strict)[1:]]
    with launched(command, tmp_path) as process:
        wait_for(held.exists, 'Actual recorder initialization did not begin.')
        if strict:
            assert not launched_at.exists() and not pid.exists()
        else:
            wait_for(pid.exists, 'Default forwarding waited on capture initialization.')
        if sent is not None:
            process.send_signal(sent)
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == expected and stdout == b''
            assert not launched_at.exists() and not pid.exists() and not ready.exists()
            return
        release.write_text('continue')
        stdout, stderr = process.communicate(wire(request(1)), timeout=12)
    assert process.returncode == 0 and stdout == wire(answer(1)) and gone(pid)
    assert json.loads(launched_at.read_text()) == {'initialized': strict}
    assert ready.exists() and observed.read_bytes() == wire(request(1))
    assert b'recording_path:' in stderr


@pytest.mark.parametrize('strict', [False, True])
def test_relative_destination_reports_absolute_path_without_recursive_creation(tmp_path, strict):
    data, response = wire(request(1)), wire(answer(1))
    child, observed, pid = fixture(tmp_path / 'success', response)
    result = run(proxy(child, 'new recordings/', require_recording=strict), data, tmp_path)
    assert result.returncode == 0 and result.stdout == response and gone(pid)
    assert b'relative_recording_path' in result.stderr
    announced = next(line.split(': ', 2)[2] for line in result.stderr.decode('ascii').splitlines()
                     if line.startswith('mfr.proxy: recording_path: '))
    path = Path(json.loads(announced))
    assert path.is_absolute() and path.parent == tmp_path / 'new recordings'
    assert len(operations(path)) == 1
    child, observed, pid = fixture(tmp_path / 'failure', response)
    result = run(proxy(child, 'missing/leaf/', require_recording=strict), data, tmp_path)
    assert not (tmp_path / 'missing').exists() and b'recording_unavailable' in result.stderr
    assert result.returncode == (1 if strict else 0)
    if strict:
        assert not pid.exists() and not observed.exists()
    else:
        assert result.stdout == response and observed.read_bytes() == data and gone(pid)


def test_rejected_start_response_cannot_attach_to_another_ticket(tmp_path):
    data = wire(request(1, "rejected")) + wire(request(2, "accepted"))
    response = wire(answer(1, "wrong result")) + wire(answer(2, "right result"))
    child, observed, _ = fixture(tmp_path, response)
    path = tmp_path / "capture.sqlite"
    result = run(proxy(child, path, fault="first-rejected", evidence=tmp_path / "fault.jsonl"), data, tmp_path)
    assert result.returncode == 0 and result.stdout == response and observed.read_bytes() == data
    stored = operations(path)
    assert len(stored) == 1 and stored[0]["name"] == "accepted" and stored[0]["status"] == "ok"
    assert json.loads(stored[0]["result_body"]) == answer(2, "right result")["result"]


@pytest.mark.parametrize("opaque_first", [False, True])
def test_duplicate_live_id_stays_poisoned_after_responses_without_harming_other_ids(tmp_path, opaque_first):
    first = request(1, "first", method="opaque/method" if opaque_first else "tools/call")
    initial = wire(first) + wire(request(1, "duplicate")) + wire(request(2, "healthy"))
    replies = wire(answer(1, "ambiguous A")) + wire(answer(1, "ambiguous B")) + wire(answer(2, "healthy"))
    tail = wire(request(1, "still ambiguous")) + wire(request(3, "later healthy"))
    tail_replies = wire(answer(1, "ambiguous C")) + wire(answer(3, "later healthy"))
    tail_file = tmp_path / "tail.bin"
    tail_file.write_bytes(tail_replies)
    child, observed, _ = fixture(tmp_path, replies, "--input-bytes", len(initial),
                                 "--tail-input-bytes", len(tail), "--tail-response", tail_file)
    path = tmp_path / "capture.sqlite"
    with launched(proxy(child, path), tmp_path) as process:
        process.stdin.write(initial); process.stdin.flush()
        assert read_bytes(process.stdout, len(replies)) == replies
        stdout, stderr = process.communicate(tail, timeout=10)
    assert process.returncode == 0 and stdout == tail_replies and observed.read_bytes() == initial + tail
    assert b"duplicate_live_id" in stderr
    stored = {row["name"]: row for row in operations(path)}
    assert set(stored) == ({"healthy", "later healthy"} if opaque_first else {"first", "healthy", "later healthy"})
    assert stored["healthy"]["status"] == stored["later healthy"]["status"] == "ok"
    if not opaque_first:
        assert stored["first"]["status"] == "incomplete" and stored["first"]["end_event_id"] is None


@pytest.mark.parametrize("bad_id", [True, None, 1.5, [1], {"id": 1}, 2 ** 80, "x" * 257])
def test_unobservable_request_id_disables_correlation_but_preserves_later_bytes(tmp_path, bad_id):
    data = wire(request(1, "earlier")) + wire(request(bad_id)) + wire(request(2, "later"))
    response = wire(answer(1)) + wire(answer(bad_id)) + wire(answer(2))
    child, observed, _ = fixture(tmp_path, response)
    path = tmp_path / "capture.sqlite"
    result = run(proxy(child, path), data, tmp_path)
    assert result.returncode == 0 and result.stdout == response and observed.read_bytes() == data
    assert b"correlation_disabled" in result.stderr and len(result.stderr) < 8192
    stored = operations(path)
    assert len(stored) == 1 and stored[0]["name"] == "earlier" and stored[0]["status"] == "incomplete"


@pytest.mark.parametrize("invalid", [b'{bad json}\n', b'{"jsonrpc":"2.0","id":1,"id":2}\n',
    wire(request(3, arguments={"deep": [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[0]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]})),
    wire(request(3, arguments={"nodes": [0] * 4097})), wire(request(3, arguments={"k" * 257: 1}))])
def test_observer_parser_and_projection_limits_never_become_transport_limits(tmp_path, invalid):
    data = wire(request(1, "earlier")) + invalid + wire(request(2, "later"))
    response = wire(answer(1)) + b'opaque reply bytes\n' + wire(answer(2))
    child, observed, _ = fixture(tmp_path, response)
    path = tmp_path / "capture.sqlite"
    result = run(proxy(child, path), data, tmp_path)
    assert result.returncode == 0 and result.stdout == response and observed.read_bytes() == data
    assert b"correlation_disabled" in result.stderr
    stored = operations(path)
    assert len(stored) == 1 and stored[0]["status"] == "incomplete"


@pytest.mark.parametrize("direction", ["client", "server"])
def test_oversized_frame_and_later_frame_forward_completely_with_bounded_loss(tmp_path, direction):
    large = "🌱" * 50000
    data = wire(request(1, arguments={"large": large if direction == "client" else "small"})) + wire(request(2))
    response = wire(answer(1, large if direction == "server" else "small")) + wire(answer(2))
    warmup, ready = wire(request(0, "frame-ready")), wire(answer(0, "ready"))
    tail = tmp_path / "large-response.bin"
    tail.write_bytes(response)
    child, observed, pid = fixture(tmp_path, ready, "--input-bytes", len(warmup),
                                  "--tail-input-bytes", len(data), "--tail-response", tail)
    path = tmp_path / "capture.sqlite"
    with launched(proxy(child, path), tmp_path) as process:
        process.stdin.write(warmup); process.stdin.flush()
        assert read_bytes(process.stdout, len(ready)) == ready
        # Establish observer readiness through a real completed operation before
        # the large-frame phase. Normal 16 KiB fixture chunks isolate the frame
        # bound; separate cases exercise tiny fragments and observer queue loss.
        wait_for(lambda: has_operation(path) and any(row["name"] == "frame-ready" and row["status"] == "ok"
                 for row in operations(path)), "warmup operation committed before the oversized frame")
        stdout, stderr = process.communicate(data, timeout=12)
    assert process.returncode == 0 and stdout == response and observed.read_bytes() == warmup + data and gone(pid)
    assert b"frame_limit" in stderr and b"correlation_disabled" in stderr
    assert len(stderr) < 8192 and large[:100].encode() not in stderr
    all_stored = operations(path)
    assert len([row for row in all_stored if row["name"] == "frame-ready" and row["status"] == "ok"]) == 1
    stored = [row for row in all_stored if row["name"] != "frame-ready"]
    assert len(stored) == (0 if direction == "client" else 2)
    assert all(row["status"] == "incomplete" for row in stored)


def test_more_than_map_bound_is_visible_and_cannot_complete_unknown_calls(tmp_path):
    data = b"".join(wire(request(i, str(i))) for i in range(140))
    response = b"".join(wire(answer(i, str(i))) for i in reversed(range(140)))
    child, observed, _ = fixture(tmp_path, response)
    path = tmp_path / "capture.sqlite"
    result = run(proxy(child, path), data, tmp_path)
    assert result.returncode == 0 and result.stdout == response and observed.read_bytes() == data
    assert b"in_flight_limit" in result.stderr and b"correlation_disabled" in result.stderr
    stored = operations(path)
    assert 0 < len(stored) <= 128 and all(row["status"] == "incomplete" for row in stored)


def test_queue_pressure_does_not_block_native_delivery_or_invent_terminals(tmp_path):
    data = wire(request(1)) + (wire({"jsonrpc": "2.0", "method": "opaque/notice", "params": {"text": "x" * 3000}}) * 400)
    response = wire(answer(1))
    ready, release = tmp_path / "ready", tmp_path / "release"
    child, observed, _ = fixture(tmp_path, response, "--input-bytes", len(data), "--ready", ready,
                                 "--tail-input-bytes", 1)
    path, proof = tmp_path / "capture.sqlite", tmp_path / "fault.jsonl"
    with launched(proxy(child, path, fault="held-start", evidence=proof, release=release), tmp_path) as process:
        process.stdin.write(wire(request(1))); process.stdin.flush()
        wait_for(lambda: proof.exists() and '"start-held"' in proof.read_text(), "observer admission handshake")
        failures = []
        def write_rest():
            try:
                process.stdin.write(data[len(wire(request(1))):]); process.stdin.flush()
            except BaseException as error:
                failures.append(error)
        writer = threading.Thread(target=write_rest, daemon=True); writer.start()
        wait_for(ready.exists, "native peer received all bytes despite held capture")
        writer.join(timeout=1)
        assert not writer.is_alive() and not failures
        assert read_bytes(process.stdout, len(response)) == response
        release.write_text("release")
        stdout, stderr = process.communicate(b"\n", timeout=10)
    assert process.returncode == 0 and stdout == b"" and observed.read_bytes() == data + b"\n"
    assert b"correlation_disabled" in stderr and len(stderr) < 8192
    stored = operations(path)
    assert len(stored) == 1 and stored[0]["status"] == "incomplete"


def test_fragmented_unicode_and_newline_are_not_lost(tmp_path):
    data, response = wire(request(1, arguments={"text": "🌱 café"})), wire(answer(1, "🌱 café"))
    child, observed, _ = fixture(tmp_path, response, "--input-bytes", len(data), "--read-size", 1, "--write-size", 1)
    path = tmp_path / "capture.sqlite"
    split = data.index("🌱".encode()) + 1
    with launched(proxy(child, path), tmp_path) as process:
        process.stdin.write(data[:split]); process.stdin.flush()
        wait_for(lambda: observed.exists() and observed.stat().st_size == split, "peer saw partial UTF-8 bytes")
        stdout, stderr = process.communicate(data[split:], timeout=10)
    assert process.returncode == 0 and stdout == response and observed.read_bytes() == data
    assert b"correlation_disabled" not in stderr and operations(path)[0]["status"] == "ok"


def test_pending_request_at_eof_remains_incomplete(tmp_path):
    child, observed, pid = fixture(tmp_path, b"")
    path = tmp_path / "capture.sqlite"
    data = wire(request(1)) + wire({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 1}})
    result = run(proxy(child, path), data, tmp_path)
    assert result.returncode == 0 and result.stdout == b"" and observed.read_bytes() == data and gone(pid)
    stored = operations(path)
    assert len(stored) == 1 and stored[0]["status"] == "incomplete" and stored[0]["end_event_id"] is None
    assert_portable(path)


@pytest.mark.parametrize("mode", ["nonzero", "signal", "early"])
def test_child_exit_and_produced_output_are_preserved(tmp_path, mode):
    options = {"nonzero": ("--exit-code", 7), "signal": ("--signal-exit", signal.SIGTERM),
               "early": ("--exit-before-input", "--exit-code", 9)}[mode]
    data, response = b"native input\n", b"already produced output\n"
    child, _, pid = fixture(tmp_path, response, *options)
    result = run(proxy(child), data, tmp_path)
    assert (result.returncode, result.stdout) == ({"nonzero": 7, "signal": 143, "early": 9}[mode], response)
    assert gone(pid)


def test_large_native_stderr_never_enters_protocol_stdout(tmp_path):
    data, response = wire(request(1)), wire(answer(1))
    child, observed, pid = fixture(tmp_path, response, "--stderr-bytes", 300000)
    result = run(proxy(child, tmp_path / "capture.sqlite"), data, tmp_path)
    assert result.returncode == 0 and result.stdout == response and observed.read_bytes() == data and gone(pid)
    assert len(result.stderr) == 300000 and result.stderr.startswith(b"NATIVE-STDERR\n")


@pytest.mark.parametrize("sent,expected", [(signal.SIGINT, 130), (signal.SIGTERM, 143)])
def test_interruption_reaps_owned_child_and_leaves_pending_capture_incomplete(tmp_path, sent, expected):
    ready = tmp_path / "ready"
    data = wire(request(1))
    child, observed, pid = fixture(tmp_path, b"", "--input-bytes", len(data), "--ready", ready,
                                  "--hang", "--ignore-term")
    path = tmp_path / "capture.sqlite"
    with launched(proxy(child, path), tmp_path) as process:
        process.stdin.write(data); process.stdin.flush()
        wait_for(ready.exists, "child received the pending invocation")
        wait_for(lambda: has_operation(path), "pending invocation committed before interruption")
        start = time.monotonic(); process.send_signal(sent)
        stdout, stderr = process.communicate(timeout=8)
    assert process.returncode == expected and stdout == b"" and time.monotonic() - start < 8
    assert observed.read_bytes() == data and gone(pid)
    stored = operations(path)
    assert len(stored) == 1 and stored[0]["status"] == "incomplete" and stored[0]["end_event_id"] is None


def test_child_stdout_eof_with_live_process_is_bounded_and_reaped(tmp_path):
    child, _, pid = fixture(tmp_path, b"", "--input-bytes", 0, "--close-output", "--hang")
    started = time.monotonic()
    result = run(proxy(child), b"", tmp_path)
    assert result.returncode == 143 and result.stdout == b"" and time.monotonic() - started < 8 and gone(pid)


def test_broken_client_output_does_not_leak_child_or_deadlock(tmp_path):
    child, _, pid = fixture(tmp_path, b"x" * 2_000_000, "--input-bytes", 0)
    with launched(proxy(child), tmp_path) as process:
        process.stdout.close(); process.stdout = None
        process.communicate(timeout=8)
    assert process.returncode != 0 and gone(pid)


def test_slow_client_reader_and_bidirectional_backpressure_preserve_complete_bytes(tmp_path):
    data, response = b"client bytes " * 200000, b"server bytes " * 200000
    child, observed, pid = fixture(tmp_path, response, "--output-before-input", "--read-size", 127)
    with launched(proxy(child), tmp_path) as process:
        failure = []
        def write():
            try:
                process.stdin.write(data); process.stdin.close()
            except BaseException as error:
                failure.append(error)
        writer = threading.Thread(target=write, daemon=True); writer.start()
        first = read_bytes(process.stdout, 1)
        assert first == response[:1] and writer.is_alive(), "Input remains backpressured until the output is consumed."
        output = first + read_bytes(process.stdout)
        writer.join(timeout=5)
        assert not writer.is_alive() and not failure
        process.wait(timeout=8)
    assert process.returncode == 0 and output == response and observed.read_bytes() == data and gone(pid)


@pytest.mark.parametrize('strict', [False, True])
def test_blocked_diagnostic_stderr_cannot_hold_native_exit_or_child_reaping(tmp_path, strict):
    data, response = wire(request(1)), wire(answer(1))
    child, observed, pid = fixture(tmp_path, response)
    path = tmp_path / "existing.sqlite"
    if not strict:
        path.write_bytes(b"existing destination must survive")
    read_fd, write_fd = os.pipe()
    try:
        # Fill the inherited pipe before launch, without requiring the child to
        # write or letting communicate() silently drain the diagnostic stream.
        os.set_blocking(write_fd, False)
        filled = 0
        while True:
            try:
                filled += os.write(write_fd, b"x")
            except BlockingIOError:
                break
        assert filled > 0
        os.set_blocking(write_fd, True)
        with launched(proxy(child, path, require_recording=strict), tmp_path, stderr=write_fd) as process:
            process.stdin.write(data); process.stdin.close()
            assert read_bytes(process.stdout, len(response)) == response
            # Existing-path capture initialization emits a diagnostic while the
            # full pipe remains unread, including throughout interpreter exit.
            process.wait(timeout=8)
            assert process.returncode == 0
        assert observed.read_bytes() == data and gone(pid)
        if strict:
            assert len(operations(path)) == 1 and operations(path)[0]['status'] == 'ok'
            assert_portable(path)
        else:
            assert path.read_bytes() == b"existing destination must survive"
    finally:
        os.close(write_fd)
        os.close(read_fd)


@pytest.mark.parametrize("marker", [None, 0, "false", [], {}, False, True, "absent"])
def test_nonboolean_error_marker_is_partial_without_changing_native_result(tmp_path, marker):
    reply = answer(1)
    if marker == "absent":
        del reply["result"]["isError"]
    else:
        reply["result"]["isError"] = marker
    data, response = wire(request(1)), wire(reply)
    child, observed, pid = fixture(tmp_path, response)
    path = tmp_path / "capture.sqlite"
    result = run(proxy(child, path), data, tmp_path)
    assert result.returncode == 0 and result.stdout == response
    assert observed.read_bytes() == data and gone(pid)
    stored = operations(path)
    assert len(stored) == 1 and json.loads(stored[0]["result_body"]) == reply["result"]
    assert stored[0]["status"] == ("tool_error" if marker is True else "ok")
    terminal = rows(path, "SELECT details_json FROM events WHERE kind='mcp.response'")
    assert len(terminal) == 1
    complete = type(marker) is bool or marker == "absent"
    assert json.loads(terminal[0]["details_json"])["complete"] is complete


def test_input_required_discriminator_stays_partial_with_unknown_content_extension(tmp_path):
    reply = {"jsonrpc": "2.0", "id": 1, "result": {
        "resultType": "input_required", "requestState": {"opaque": ["continuation", 42]},
        "content": [{"type": "text", "text": "extension is not a completed tool result"}],
    }}
    data, response = wire(request(1)), wire(reply)
    child, observed, pid = fixture(tmp_path, response)
    path = tmp_path / "capture.sqlite"
    result = run(proxy(child, path), data, tmp_path)
    assert result.returncode == 0 and result.stdout == response
    assert observed.read_bytes() == data and gone(pid)
    stored = operations(path)
    assert len(stored) == 1 and stored[0]["status"] == "ok"
    assert json.loads(stored[0]["result_body"]) == reply["result"]
    terminal = rows(path, "SELECT details_json FROM events WHERE kind='mcp.response'")
    assert len(terminal) == 1 and json.loads(terminal[0]["details_json"])["complete"] is False


@pytest.mark.parametrize("method", ["tools/list", "tools/call"])
@pytest.mark.parametrize("capture", ["metadata", "content"])
@pytest.mark.parametrize("discriminator,present,complete", [
    pytest.param("peer_deferred", True, False, id="foreign"),
    pytest.param(None, True, False, id="null"),
    pytest.param(False, True, False, id="boolean"),
    pytest.param(7, True, False, id="integer"),
    pytest.param([], True, False, id="array"),
    pytest.param({}, True, False, id="object"),
    pytest.param("complete", True, True, id="complete"),
    pytest.param(None, False, True, id="legacy-absent"),
    pytest.param("input_required", True, False, id="input-required"),
])
def test_result_discriminator_controls_projection_without_changing_wire_or_next_rpc(
        tmp_path, method, capture, discriminator, present, complete):
    first = (request(11, "matrix-first") if method == "tools/call" else
             {"jsonrpc": "2.0", "id": 11, "method": method, "params": {}})
    # Both known field names deliberately overlap an opaque extension. They
    # cannot turn a foreign/malformed discriminator into a core final result.
    body = {"content": [{"type": "text", "text": "opaque overlapping text"}],
            "tools": [], "status": "working", "requestState": {"handle": "native-only"}}
    if present:
        body["resultType"] = discriminator
    first_data = wire(first)
    first_response = wire({"jsonrpc": "2.0", "id": 11, "result": body})
    next_data = wire(request("later", "matrix-later", {"text": "next"}))
    next_response = wire(answer("later", "ordinary next result"))
    tail = tmp_path / "next-response.bin"
    tail.write_bytes(next_response)
    options = ("--input-bytes", len(first_data), "--tail-input-bytes", len(next_data),
               "--tail-response", tail)
    direct, direct_input, direct_pid = fixture(tmp_path / "direct", first_response, *options)
    native = run(direct, first_data + next_data, tmp_path)
    child, observed, pid = fixture(tmp_path / "proxy", first_response, *options)
    path = tmp_path / "capture.sqlite"
    argv = ([FR, "proxy", "--record", str(path), "--", *child] if capture == "metadata"
            else proxy(child, path))
    with launched(argv, tmp_path) as process:
        process.stdin.write(first_data); process.stdin.flush()
        assert read_bytes(process.stdout, len(first_response)) == first_response
        stdout, stderr = process.communicate(next_data, timeout=12)
    assert process.returncode == native.returncode == 0 and gone(pid) and gone(direct_pid)
    assert first_response + stdout == native.stdout == first_response + next_response
    assert observed.read_bytes() == direct_input.read_bytes() == first_data + next_data
    assert len(stderr) < 8192

    stored = operations(path)
    assert len(stored) == 2 and all(row["status"] == "ok" for row in stored)
    first_operation = next(row for row in stored if row["name"] != "matrix-later")
    next_operation = next(row for row in stored if row["name"] == "matrix-later")
    assert first_operation["method"] == method and next_operation["method"] == "tools/call"
    terminals = {row["event_id"]: row for row in rows(path, """SELECT e.event_id,e.details_json,
        a.body_text,a.stored_bytes,a.omitted_reason FROM events e
        JOIN artifacts a ON a.artifact_id=e.payload_ref WHERE e.kind='mcp.response'""")}
    assert len(terminals) == 2
    first_terminal = terminals[first_operation["end_event_id"]]
    next_terminal = terminals[next_operation["end_event_id"]]
    if capture == "metadata":
        assert all(row["body_text"] is None and row["stored_bytes"] == 0 and row["omitted_reason"]
                   for row in terminals.values())
    else:
        assert json.loads(first_terminal["body_text"]) == body
        assert json.loads(next_terminal["body_text"]) == answer("later", "ordinary next result")["result"]
        assert all(row["stored_bytes"] > 0 and row["omitted_reason"] is None for row in terminals.values())
    later_details = json.loads(next_terminal["details_json"])
    assert later_details["complete"] is True
    assert len(later_details["result_text_digest"]["value"]) == 64
    details = json.loads(first_terminal["details_json"])
    digest = details["result_text_digest"]
    if complete and method == "tools/call":
        assert len(digest["value"]) == 64 and digest["basis"] == "observed_original"
    else:
        assert digest["value"] is None and digest["reason"] == "text_paths_unavailable"
    assert details["complete"] is complete
    assert_portable(path)
