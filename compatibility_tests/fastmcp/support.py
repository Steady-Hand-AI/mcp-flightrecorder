"""Owned native transports and independent ledgers for the maintained FastMCP example."""
from contextlib import asynccontextmanager, closing
from dataclasses import asdict, is_dataclass
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys

from fastmcp import Client
from fastmcp.client.transports import StdioTransport
from mcp.client.caching import CacheConfig, InMemoryResponseCacheStore
from mcp_flightrecorder import Recorder, inspect
from mcp_flightrecorder.integrations.mcp_session import OperationEvidence


COVERAGE = {"producers": {"model": None, "host": "integration.fastmcp-session",
                            "mcp": "mcp-session-native-2.3.0"}, "model_complete": False}


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def project(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True, exclude_unset=True)
    if is_dataclass(value):
        return {key: project(part) for key, part in asdict(value).items()}
    if isinstance(value, dict):
        return {key: project(part) for key, part in value.items()}
    if isinstance(value, (list, tuple)):
        return [project(part) for part in value]
    return value


def rows(path, statement):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute(statement)]


def events(path):
    result = rows(path, "SELECT e.*,a.body_text FROM events e LEFT JOIN artifacts a "
                        "ON a.artifact_id=e.payload_ref ORDER BY e.sequence")
    for row in result:
        row["body"] = None if row["body_text"] is None else json.loads(row["body_text"])
        row["details"] = json.loads(row["details_json"])
    return result


def ledger(path):
    return [] if not path.exists() else [json.loads(line) for line in path.read_text().splitlines()]


def finish(recorder, path):
    closed = recorder.close(timeout=5)
    assert closed.writer_stopped and closed.portable, "Each owned recorder must join and finalize before independent inspection."
    run_id = rows(path, "SELECT run_id FROM runs")[0]["run_id"]
    facts = {"events": events(path), "operations": rows(path, "SELECT * FROM operations ORDER BY rowid"),
             "check": inspect.check(path, run_id), "run_id": run_id,
             "integrity": rows(path, "PRAGMA integrity_check"),
             "foreign_keys": rows(path, "PRAGMA foreign_key_check"),
             "journal_mode": rows(path, "PRAGMA journal_mode"),
             "links": rows(path, "SELECT * FROM links")}
    return facts


def witness_native(session, method, native_log):
    native = getattr(session, method)

    async def witness(*args, **kwargs):
        entry = {"method": method, "args": project(args), "kwargs": project(kwargs)}
        native_log.append(entry)
        try:
            result = await native(*args, **kwargs)
        except BaseException as error:
            entry["error"] = {"type": type(error).__name__, "message": str(error)}
            raise
        entry["result"] = project(result)
        entry["result_type"] = type(result).__module__ + "." + type(result).__name__
        entry["object_id"] = id(result)
        return result

    setattr(session, method, witness)


def attach_observer(recorder, session, receipts, native_log):
    """Run the maintained example, then add only test-owned receipt/outcome witnesses."""
    spec = importlib.util.spec_from_file_location(
        "mfr_fastmcp_example", Path(__file__).resolve().parents[2] / "examples/fastmcp_session.py")
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    result = example.attach_session(recorder, session, client_id="fastmcp.session",
                                    server_key="owned-stdio")
    assert result is None, "The attachment recipe changes the owned active session and returns no replacement client."

    def witness_attachment(method):
        observed_method = getattr(session, method)

        async def observed(*args, **kwargs):
            evidence = OperationEvidence()
            receipts.append(evidence)
            result = await observed_method(*args, evidence=evidence, **kwargs)
            candidates = [entry for entry in native_log if entry["method"] == method]
            candidates[-1]["observer_same_object"] = id(result) == candidates[-1]["object_id"]
            return result

        setattr(session, method, observed)

    witness_attachment("list_tools")
    witness_attachment("call_tool")

class CacheStoreLedger(InMemoryResponseCacheStore):
    """Observe the documented public store seam without changing cache policy."""
    def __init__(self):
        super().__init__()
        self.ledger = []

    async def get(self, key):
        entry = await super().get(key)
        self.ledger.append({"action": "get", "method": key.method, "hit": entry is not None,
                            "value": None if entry is None else project(entry.value),
                            "expires_at": None if entry is None else entry.expires_at})
        return entry

    async def set(self, key, entry):
        self.ledger.append({"action": "set", "method": key.method, "value": project(entry.value)})
        return await super().set(key, entry)


@asynccontextmanager
async def native_client(case, recorder=None, *, cache=False, mode="auto"):
    case.mkdir(parents=True, exist_ok=False)
    transport = StdioTransport(command=sys.executable,
        args=["-I", "-B", str(Path(__file__).with_name("server.py")), str(case / "server.jsonl")],
        keep_alive=False, log_file=case / "server.stderr")
    native_log, receipts = [], []
    cache_store = CacheStoreLedger() if cache else None
    cache_config = (CacheConfig(store=cache_store, partition="reviewer", target_id="owned-stdio")
                    if cache else False)
    try:
        async with Client(transport, cache=cache_config, mode=mode, timeout=15) as client:
            witness_native(client.session, "list_tools", native_log)
            witness_native(client.session, "call_tool", native_log)
            if recorder is not None:
                attach_observer(recorder, client.session, receipts, native_log)
            yield client, native_log, receipts
    finally:
        # Attachment tests independently read the observed cache-store calls.
        write_json(case / "cache-store.json", [] if cache_store is None else cache_store.ledger)


def make_recorder(case, capture="content"):
    path = case / "recording.sqlite"
    return Recorder(path, capture=capture, coverage=COVERAGE), path
