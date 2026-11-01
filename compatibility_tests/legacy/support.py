"""Independent persisted facts and owned SDK1 session transport for this lane."""
from contextlib import asynccontextmanager, closing
from functools import wraps
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from mcp_flightrecorder import inspect


ROOT = Path(__file__).resolve().parents[2]
COVERAGE = {"producers": {"model": None, "host": "integration.session-owner",
                           "mcp": "mcp_session_legacy/1.23.1"},
            "model_complete": False}


def load_example(name):
    spec = importlib.util.spec_from_file_location("legacy_example_" + name, ROOT / "examples" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def rows(path, statement):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(statement)]


def events(path):
    values = rows(path, """SELECT e.*, a.body_text FROM events e
        LEFT JOIN artifacts a ON a.artifact_id=e.payload_ref ORDER BY e.sequence""")
    for value in values:
        value["details"] = json.loads(value["details_json"])
        value["body"] = None if value["body_text"] is None else json.loads(value["body_text"])
    return values


def finish(recorder, path):
    result = recorder.close(timeout=5)
    assert result.writer_stopped and result.portable, (
        "Persisted comparisons require a joined writer and portable owned recording."
    )
    run_id = rows(path, "SELECT run_id FROM runs")[0]["run_id"]
    facts = {"events": events(path), "operations": rows(path, "SELECT * FROM operations ORDER BY rowid"),
             "inspection": inspect.check(path, run_id, expect=["mcp"]),
             "foreign_keys": rows(path, "PRAGMA foreign_key_check"),
             "integrity": rows(path, "PRAGMA integrity_check")}
    return facts


def wire_log(directory):
    path = directory / "server.jsonl"
    return [] if not path.exists() else [json.loads(line) for line in path.read_text().splitlines()]


@asynccontextmanager
async def native_session(directory, *, paged=False, malformed=False):
    directory.mkdir(parents=True, exist_ok=True)
    args = ["-I", "-B", str(Path(__file__).with_name("session_peer.py")),
            str(directory / "server.jsonl")]
    if paged:
        args.append("--paged")
    if malformed:
        args.append("--malformed")
    params = StdioServerParameters(command=sys.executable, args=args)
    with (directory / "server.stderr").open("w") as errors:
        async with stdio_client(params, errlog=errors) as streams:
            async with ClientSession(*streams) as native:
                await native.initialize()
                yield native


def spy_method(monkeypatch, session, name):
    """A test-owned native-call witness; it changes no result or forwarding."""
    original = getattr(session, name)
    calls, outcomes = [], []

    @wraps(original)
    def spy(*args, **kwargs):
        calls.append((args, kwargs))

        async def execute():
            try:
                result = await original(*args, **kwargs)
            except BaseException as error:
                outcomes.append(error)
                raise
            outcomes.append(result)
            return result
        return execute()

    monkeypatch.setattr(session, name, spy)
    return calls, outcomes
