"""Isolated genuine MCP1 peers retain handshake and bidirectional ID ownership."""
from contextlib import closing
from datetime import timedelta
import json
import os
from pathlib import Path
import sqlite3
import sys

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
import pytest
from pytest_check import check

import mcp_flightrecorder


PEER = Path(__file__).resolve().parents[2] / "tests/producers/proxy_sdk_peer.py"


def ledger(directory):
    return [json.loads(line) for line in (directory / "peer.jsonl").read_text().splitlines()]


def rows(path, query):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(query)]


async def exercise(directory, mode):
    directory.mkdir()
    child = [sys.executable, "-I", "-B", str(PEER), "--legacy",
             "--ledger", str(directory / "peer.jsonl"), "--pid", str(directory / "child.pid")]
    if mode == "direct":
        command, args = child[0], child[1:]
    else:
        command, args = str(Path(sys.executable).with_name("fr")), ["proxy"]
        if mode != "forward":
            args += ["--record", str(directory / "recording.sqlite"), "--capture", mode]
        args += ["--", *child]
    assert "site-packages" in mcp_flightrecorder.__file__, "Compatibility acceptance requires an installed candidate."
    (directory / "launch.json").write_text(json.dumps({"command": command, "args": args,
        "cwd": str(directory), "installed_origin": mcp_flightrecorder.__file__}, indent=2) + "\n")
    params = StdioServerParameters(command=command, args=args, cwd=directory)
    outcomes = []
    with (directory / "stderr.log").open("w") as errors:
        async with stdio_client(params, errlog=errors) as streams:
            async with ClientSession(*streams, read_timeout_seconds=timedelta(seconds=10)) as native:
                outcomes.append(await native.initialize())
                outcomes.append(await native.list_tools())
                for name in ("echo", "echo", "refuse"):
                    outcomes.append(await native.call_tool(name, {"text": "same input café 🌱"}))
    with pytest.raises(ProcessLookupError):
        os.kill(int((directory / "child.pid").read_text()), 0)
    result = [value.model_dump(mode="json", by_alias=True, exclude_unset=True) for value in outcomes]
    (directory / "outcomes.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return result


@pytest.mark.parametrize("mode", ["content", "metadata", "forward"])
def test_legacy_native_handshake_and_reverse_request_id_collision(tmp_path, run_sdk_async, mode):
    async def scenario():
        direct = await exercise(tmp_path / "direct", "direct")
        proxied = await exercise(tmp_path / "proxy", mode)
        check.equal(proxied, direct, msg="The genuine SDK1 client receives identical native handshake, discovery and tool outcomes.")
    run_sdk_async(scenario(), timeout=30)
    observed = ledger(tmp_path / "proxy")
    check.equal(observed, ledger(tmp_path / "direct"), msg="Independent native peer messages, IDs, arguments and invocation counts remain unchanged.")
    upstream = [r["message"] for r in observed if r["kind"] == "client-to-server"]
    downstream = [r["message"] for r in observed if r["kind"] == "server-to-client"]
    check.equal([r.get("method") for r in upstream if "method" in r],
                ["initialize", "notifications/initialized", "tools/list"] + ["tools/call"] * 3)
    check.equal([r["id"] for r in downstream if r.get("method") == "ping"], [0, 1])
    check.equal([r["id"] for r in upstream if "result" in r], [0, 1],
                msg="Both genuine reverse requests receive native client responses despite same-valued client request IDs.")
    positions = {(r["kind"], r["message"].get("method"), r["message"].get("id")): i
                 for i, r in enumerate(observed) if "message" in r}
    check.less(positions[("client-to-server", "tools/list", 1)], positions[("server-to-client", "ping", 1)])
    check.less(positions[("server-to-client", "ping", 1)], positions[("server-to-client", None, 1)],
               msg="The colliding reverse ID is live before the original client request completes.")
    check.equal(len([r for r in observed if r["kind"] == "invocation"]), 3)
    db = tmp_path / "proxy/recording.sqlite"
    if mode == "forward":
        check.is_false(db.exists())
        return
    operations = rows(db, "SELECT * FROM operations ORDER BY rowid")
    check.equal([o["method"] for o in operations], ["tools/list"] + ["tools/call"] * 3,
                msg="Handshake, reverse ping requests and their responses must not become tool operations.")
    check.equal([o["status"] for o in operations], ["ok", "ok", "ok", "tool_error"])
    check.is_true(all(o["turn_id"] is None and o["model_call_id"] is None and o["decision_id"] is None
                      and o["dispatch_event_id"] is None and o["link_basis"] == "none" for o in operations))
    check.equal(rows(db, "SELECT count(*) AS n FROM turns"), [{"n": 0}])
    check.equal(rows(db, "SELECT count(*) AS n FROM events WHERE boundary IN ('model','host')"), [{"n": 0}])
    if mode == "metadata":
        check.equal(rows(db, "SELECT count(*) AS n FROM artifacts WHERE body_text IS NOT NULL"), [{"n": 0}])
    else:
        check.equal(rows(db, "SELECT count(*) AS n FROM events WHERE payload_ref IS NULL"), [{"n": 0}])
